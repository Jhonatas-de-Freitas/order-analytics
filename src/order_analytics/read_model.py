from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from order_analytics.events import PAYLOADS
from order_analytics.projection import project
from order_analytics.tables import Event, Order, OrderItem


async def refresh_order(session: AsyncSession, order_id: str) -> None:
    """Rewrite the row of one order from all of its stored events.

    Recomputing from the full set, instead of applying only the new event on top of
    the current row, is what makes the result independent of arrival order. An order
    has a handful of events, so reading them again each time costs very little.
    """
    events = await session.scalars(
        select(Event).where(Event.order_id == order_id, Event.type.in_(PAYLOADS))
    )
    state = project(events.all())
    if state is None:
        return

    columns = {
        "status": state.status,
        "customer_id": state.customer_id,
        "customer_name": state.customer_name,
        "customer_email": state.customer_email,
        "total_amount": state.total_amount,
        "cancellation_reason": state.cancellation_reason,
        "cancellation_detail": state.cancellation_detail,
        "created_at": state.created_at,
        "finished_at": state.finished_at,
    }
    await session.execute(
        insert(Order)
        .values(order_id=order_id, **columns)
        .on_conflict_do_update(index_elements=[Order.order_id], set_=columns)
    )
    await session.execute(delete(OrderItem).where(OrderItem.order_id == order_id))
    if state.items:
        await session.execute(
            insert(OrderItem),
            [
                {
                    "order_id": order_id,
                    "position": position,
                    "product_id": item.product_id,
                    "product_name": item.product_name,
                    "quantity": item.quantity,
                    "unit_price": item.unit_price,
                }
                for position, item in enumerate(state.items)
            ],
        )
