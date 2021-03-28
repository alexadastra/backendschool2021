from sqlalchemy import and_, func, select, distinct
from sqlalchemy.orm import join

from store.db.schema import orders_table, delivery_hours_table, orders_delivery_hours_table


COURIERS_ORDERS_SEQUENCES_QUERY = select([
    func.count(distinct(orders_table.c.assignment_time))
]
).select_from(
    orders_table
).group_by(
   orders_table.c.assignment_time
)

COURIERS_ORDERS_REGIONS_QUERY = select([
    orders_table.c.region,
    func.avg(orders_table.c.completion_time - orders_table.c.assignment_time).label('average_timedelta')
]).select_from(
    orders_table
) .group_by(
    orders_table.c.region,
)
