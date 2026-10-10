# order-analytics

[![CI](https://github.com/Jhonatas-de-Freitas/order-analytics/actions/workflows/ci.yml/badge.svg)](https://github.com/Jhonatas-de-Freitas/order-analytics/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.13-blue)

## O que é

Um serviço em Python que ouve os eventos de uma loja no Kafka e transforma em números: quantos pedidos, quanto faturou, o que foi cancelado e por quê, quais produtos mais saem.

Faz parte de um sistema de cinco projetos. Os eventos vêm dos serviços Java do [order-platform](https://github.com/Jhonatas-de-Freitas/order-platform); o painel [order-dashboard](https://github.com/Jhonatas-de-Freitas/order-dashboard) mostra estes números passando pelo [api-gateway](https://github.com/Jhonatas-de-Freitas/api-gateway). Mas ele roda sozinho: tem um comando que publica eventos de exemplo.

Usei FastAPI, SQLAlchemy 2 (assíncrono) com Alembic, PostgreSQL e `aiokafka`.

## Como funciona

```mermaid
flowchart LR
    O[orders.events] --> C
    I[inventory.events] --> C
    P[payments.events] --> C
    C[consumidor<br/>grupo order-analytics] -->|não é um evento válido| R[(rejected_events)]
    C -->|eventId ainda não visto| E[(events)]
    E -->|todos os eventos do pedido| J[projeção]
    J --> M[(orders e order_items)]
    M --> A[API /analytics]
    E -.->|rebuild| J
```

Para cada mensagem:

1. Valido o envelope. Se não for JSON ou faltar campo, a mensagem vai para `rejected_events` com o motivo e o consumo segue.
2. Gravo o evento em `events`. O `eventId` é a chave: se já existe, nada muda.
3. Leio todos os eventos daquele pedido e recalculo a linha dele em `orders` (e os itens em `order_items`).
4. Só depois do commit no banco eu confirmo o offset no Kafka.

Os passos 2 e 3 vão na mesma transação. A rejeição do passo 1 tem a sua.

A API lê `orders` e `order_items`. De `events` ela só busca a data do último evento (o `lastEventAt` do resumo). A tabela `events` é a fonte: o resto pode ser apagado e refeito.

De onde vem cada campo do pedido:

| Campo | Vem de |
|---|---|
| status | `CONFIRMED` ou `CANCELLED` pelo primeiro `OrderConfirmed`/`OrderCancelled` (pela data do evento). Sem nenhum dos dois: `STOCK_RESERVED` se existe `StockReserved`, senão `PENDING` |
| cliente | `OrderCreated`; se ele ainda não chegou, `OrderConfirmed` ou `OrderCancelled` |
| total, nomes e preços | `OrderConfirmed`; se ele ainda não chegou, `StockReserved` |
| criado em | `OrderCreated` |
| terminou em | data do evento que finalizou o pedido |

Os outros tipos (`StockRejected`, `StockReleased`, `PaymentApproved`, `PaymentRefused` e qualquer tipo que eu não conheça) ficam guardados em `events`, mas não mudam o pedido.

## Como rodar

Precisa só de Docker.

```bash
docker compose up -d --build --wait
docker compose exec order-analytics python -m order_analytics.seed
curl http://localhost:8090/analytics/summary
```

O `seed` publica 16 pedidos de exemplo, espalhados em cinco dias (de 05 a 09/10/2026). A resposta é sempre esta:

```json
{"totalOrders":16,"confirmed":9,"cancelled":5,"inProgress":2,"confirmedRevenue":7871.9,
 "approvalRate":0.6429,"averageSecondsToConfirm":6.0,"lastEventAt":"2026-10-09T09:31:00Z"}
```

A documentação da API, com exemplos, fica em http://localhost:8090/docs. Para desligar e apagar os dados: `docker compose down -v`.

| Variável | Padrão | Para quê |
|---|---|---|
| `DATABASE_URL` | `postgresql://postgres:postgres@localhost:5432/analytics_db` | banco |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:29092` | Kafka |
| `KAFKA_ENABLED` | `true` | `false` sobe só a API, sem consumir |
| `PORT` | `8090` | porta HTTP |
| `LOG_LEVEL` | `INFO` | nível do log (uma linha JSON por registro) |

## Como testar os cenários

**Os números do exemplo.** O script abaixo confere cada endpoint contra o valor esperado (usa `curl` e `jq`):

```bash
./scripts/smoke-test.sh
```

**Os endpoints, um a um.**

```bash
curl http://localhost:8090/analytics/orders-by-status
curl "http://localhost:8090/analytics/revenue?from=2026-10-05&to=2026-10-09&granularity=day"
curl "http://localhost:8090/analytics/revenue?from=2026-10-07&to=2026-10-07&granularity=hour"
curl http://localhost:8090/analytics/cancellations
curl "http://localhost:8090/analytics/top-products?limit=3"
curl http://localhost:8090/analytics/reports/orders.csv
curl http://localhost:8090/health
```

Parâmetro inválido devolve `422`:

```bash
curl -i "http://localhost:8090/analytics/revenue?from=2026-10-09&to=2026-10-05"
```

**Evento repetido.** Rode o seed de novo. São os mesmos eventos, com os mesmos `eventId`, e o resumo não muda. No log aparece `duplicate event ignored`.

```bash
docker compose exec order-analytics python -m order_analytics.seed
docker compose logs order-analytics | grep -c "duplicate event ignored"
```

**Mensagem inválida.** O seed manda uma de propósito (um JSON sem os campos do envelope). Ela fica guardada com o motivo:

```bash
docker compose exec postgres psql -U postgres -d analytics_db \
  -c "select kafka_topic, kafka_offset, reason from rejected_events"
```

**Evento fora de ordem.** O seed já publica tudo embaralhado. Para ver com um pedido só, publique a confirmação antes da criação:

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic orders.events < examples/order-confirmed.json
curl http://localhost:8090/analytics/summary
```

O pedido já conta como confirmado (`confirmed` vai a 10, `confirmedRevenue` a 8221.8), mas ainda não entra no tempo médio, porque não sei quando ele foi criado. Agora a criação, atrasada:

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic orders.events < examples/order-created.json
curl http://localhost:8090/analytics/summary
```

`averageSecondsToConfirm` passa de 6.0 para 5.9. Este cenário acrescenta um pedido, então o `smoke-test.sh` deixa de bater depois dele.

**Reconstrução.** Apaga `orders` e `order_items` e refaz a partir de `events`. Os números voltam iguais.

```bash
docker compose exec order-analytics python -m order_analytics.rebuild
```

**Encerramento.** `docker compose stop order-analytics` manda SIGTERM. O consumidor termina a mensagem que está na mão, confirma os offsets do que gravou e sai do grupo; o log termina com `consumer stopped` e `Application shutdown complete`.

**Testes automatizados.** Uso [uv](https://docs.astral.sh/uv/):

```bash
uv sync
uv run ruff check . && uv run ruff format --check . && uv run mypy
uv run pytest
```

Os testes de integração sobem PostgreSQL e Kafka com Testcontainers. Sem Docker eles são pulados (no CI, não). Para usar um PostgreSQL que já existe: `TEST_DATABASE_URL=postgresql+asyncpg://usuario:senha@host:5432/banco`.

O CI repete tudo isso: `quality`, `test` e um job `e2e` que sobe o compose, roda o seed e o smoke test, publica o seed de novo, reconstrói, para com SIGTERM, sobe outra vez e faz o cenário do evento fora de ordem.

## Decisões que tomei e por quê

- **Recalcular o pedido inteiro a cada evento.** Em vez de aplicar o evento novo em cima da linha atual, leio todos os eventos do pedido e passo por uma função pura (`project`, em `projection.py`). O resultado depende de quais eventos existem, não da ordem em que chegaram. Um pedido tem meia dúzia de eventos, então reler custa pouco. E a reconstrução usa a mesma função, por isso dá o mesmo resultado.
- **Quem decide o estado final é o order-service.** Só `OrderConfirmed` e `OrderCancelled` finalizam o pedido aqui. Um `PaymentApproved` sozinho não confirma nada. Se os dois finais aparecerem para o mesmo pedido, vale o que aconteceu primeiro (`occurredAt`), porque lá o estado final não muda mais.
- **Evento e modelo de leitura na mesma transação, offset depois.** Se o processo cair no meio, o Kafka entrega de novo e o `eventId` repetido é ignorado. O pior caso é ler duas vezes, nunca perder.
- **Trava por pedido.** Os eventos de um pedido vêm de três tópicos, então duas instâncias podem tratar o mesmo pedido ao mesmo tempo. Um `pg_advisory_xact_lock` pelo id do pedido faz a segunda esperar e enxergar o que a primeira gravou. Sem ele, o teste de eventos simultâneos falha.
- **Rejeitar só o que é problema do dado.** JSON quebrado, envelope incompleto, id com mais de 200 caracteres (não cabe no índice), payload errado de um tipo conhecido e texto que o PostgreSQL recusa (um `\u0000`, por exemplo) vão para `rejected_events`. Erro de infraestrutura (banco fora) não: o consumidor volta o offset e tenta de novo, senão perderia o evento.
- **Reconstrução com o serviço no ar.** O comando trava a escrita em `orders` até terminar. O consumidor espera e depois grava o estado completo do pedido; quem está lendo a API continua vendo os dados antigos até o commit.
- **Dinheiro em `NUMERIC` e `Decimal`.** As somas são feitas no banco, sem ponto flutuante. Só viram `float` na saída, porque o contrato pede número JSON.
- **O consumidor cria os tópicos se eles não existirem** (3 partições, como no contrato). Assim não importa se ele sobe antes ou depois de quem publica. Se o broker recusar a criação, ele não começa a ler: registra o motivo e tenta de novo.
- **Tudo em UTC.** O faturamento entra no dia e na hora em que o pedido foi confirmado. Dia ou hora sem pedido confirmado não aparece na resposta.
- **Sem login aqui.** Quem protege `/api/analytics/**` (papel `admin`) é o gateway.

## O que eu faria a seguir

- Limite de tentativas e uma fila de erros para a mensagem que falha por outro motivo que não o dado.
- Métricas do próprio serviço em formato Prometheus: atraso do consumidor, eventos por tipo, rejeitados.
- Reconstrução em lotes, com várias transações, para quando `events` for grande. Hoje é uma transação só.
- Preencher com zero os dias sem venda e aceitar fuso horário no faturamento.
- Filtro por período e por status no CSV.
- Particionar `events` por mês.
