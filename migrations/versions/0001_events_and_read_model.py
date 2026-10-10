"""events and read model"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0001"
down_revision = None


def upgrade() -> None:
    op.create_table(
        "events",
        sa.Column("event_id", sa.Text, primary_key=True),
        sa.Column("type", sa.Text, nullable=False),
        sa.Column("order_id", sa.Text, nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", JSONB, nullable=False),
        sa.Column("kafka_topic", sa.Text, nullable=False),
        sa.Column("kafka_partition", sa.Integer, nullable=False),
        sa.Column("kafka_offset", sa.BigInteger, nullable=False),
        sa.Column(
            "received_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
    )
    op.create_index("ix_events_order_id", "events", ["order_id"])
    op.create_index("ix_events_occurred_at", "events", ["occurred_at"])

    op.create_table(
        "rejected_events",
        sa.Column("id", sa.BigInteger, sa.Identity(), primary_key=True),
        sa.Column("kafka_topic", sa.Text, nullable=False),
        sa.Column("kafka_partition", sa.Integer, nullable=False),
        sa.Column("kafka_offset", sa.BigInteger, nullable=False),
        sa.Column("message_key", sa.Text),
        sa.Column("message_value", sa.Text),
        sa.Column("reason", sa.Text, nullable=False),
        sa.Column(
            "received_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("kafka_topic", "kafka_partition", "kafka_offset"),
    )

    op.create_table(
        "orders",
        sa.Column("order_id", sa.Text, primary_key=True),
        sa.Column("status", sa.Text, nullable=False),
        sa.Column("customer_id", sa.Text),
        sa.Column("customer_name", sa.Text),
        sa.Column("customer_email", sa.Text),
        sa.Column("total_amount", sa.Numeric(12, 2)),
        sa.Column("cancellation_reason", sa.Text),
        sa.Column("cancellation_detail", sa.Text),
        sa.Column("created_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_orders_finished_at", "orders", ["finished_at"])

    op.create_table(
        "order_items",
        sa.Column(
            "order_id",
            sa.Text,
            sa.ForeignKey("orders.order_id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("position", sa.Integer, primary_key=True),
        sa.Column("product_id", sa.Text, nullable=False),
        sa.Column("product_name", sa.Text),
        sa.Column("quantity", sa.Integer, nullable=False),
        sa.Column("unit_price", sa.Numeric(12, 2)),
    )


def downgrade() -> None:
    op.drop_table("order_items")
    op.drop_table("orders")
    op.drop_table("rejected_events")
    op.drop_table("events")
