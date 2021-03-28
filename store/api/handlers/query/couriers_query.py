from sqlalchemy import and_, func, select
from sqlalchemy.orm import join

from store.db.schema import couriers_table, working_hours_table, couriers_working_hours_table, regions_table, couriers_regions_table


COURIERS_QUERY = select(
    [
        couriers_table.c.courier_id,
        couriers_table.c.courier_type,
        # В результате LEFT JOIN у жителей не имеющих родственников список
        # relatives будет иметь значение [None]. Чтобы удалить это значение
        # из списка используется функция array_remove.
        func.array_remove(
            func.array_agg(regions_table.c.region_id),
            None
        ).label('regions'),
        func.array_remove(
            func.array_agg(working_hours_table.c.time_start),
            None
        ).label('time_start'),
        func.array_remove(
            func.array_agg(working_hours_table.c.time_finish),
            None
        ).label('time_finish')
    ]
).select_from(
    couriers_table.outerjoin(
        couriers_regions_table.join(
            regions_table, couriers_regions_table.c.region_id == regions_table.c.region_id
        ), couriers_table.c.courier_id == couriers_regions_table.c.courier_id
    ).outerjoin(
        couriers_working_hours_table.join(
            working_hours_table, couriers_working_hours_table.c.working_hours_id == working_hours_table.c.working_hours_id
        ), couriers_table.c.courier_id == couriers_working_hours_table.c.courier_id
    )
).group_by(
    couriers_table.c.courier_id
)
