from store.api.handlers.base import BaseView

from http import HTTPStatus

from aiohttp.web_exceptions import HTTPNotFound
from aiohttp.web_response import Response
from aiohttp_apispec import docs, request_schema, response_schema
from sqlalchemy import and_, or_

from store.api.schema import CourierUpdateRequestSchema, CourierItemSchema, CourierGetResponseSchema
from store.db.schema import couriers_table, couriers_regions_table, couriers_working_hours_table, working_hours_table, \
    regions_table, orders_table

from ..query import COURIERS_QUERY, AvailableOrdersDefiner, COURIERS_ORDERS_SEQUENCES_QUERY, COURIERS_ORDERS_REGIONS_QUERY
from ...domain import TimeIntervalsConverter, CourierConfigurator


class CouriersView(BaseView):
    URL_PATH = r'/couriers/{courier_id:\d+}'

    @property
    def courier_id(self):
        return int(self.request.match_info.get('courier_id'))

    @staticmethod
    async def acquire_lock(conn, courier_id):
        await conn.execute('SELECT pg_advisory_xact_lock($1)', courier_id)

    @staticmethod
    async def get_courier(conn, courier_id):
        query = COURIERS_QUERY.where(and_(
            couriers_table.c.courier_id == courier_id,
        ))
        courier = await conn.fetchrow(query)
        if courier is None:
            raise HTTPNotFound()
        return {
            'courier_id': courier['courier_id'],
            'courier_type': courier['courier_type'],
            'regions': list(dict.fromkeys(courier['regions'])),
            'working_hours': TimeIntervalsConverter.int_to_string_array(time_start_intervals=courier['time_start'],
                                                                        time_finish_intervals=courier['time_finish'])
        }

    @staticmethod
    async def get_orders(conn, courier_id):
        query = orders_table.select().where(
            and_(orders_table.c.courier_id == courier_id, orders_table.c.completion_time == None))
        return await conn.fetch(query)

    @staticmethod
    async def add_regions(conn, courier_id, region_ids):
        if not region_ids:
            return

        query = regions_table.select()
        regions = await conn.fetch(query)
        regions = {i['region_id'] for i in regions}
        regions = region_ids - regions
        values = [{'region_id': i} for i in regions]
        query = regions_table.insert().values(values)
        await conn.execute(query)

        values = [{'courier_id': courier_id, 'region_id': region_id} for region_id in region_ids]
        query = couriers_regions_table.insert().values(values)
        await conn.execute(query)

    @staticmethod
    async def remove_regions(conn, courier_id, region_ids):
        if not region_ids:
            return

        conditions = []
        for region_id in region_ids:
            conditions.append(
                and_(couriers_regions_table.c.region_id == region_id,
                     couriers_regions_table.c.courier_id == courier_id
                     ),
            )
        query = couriers_regions_table.delete().where(or_(*conditions))
        await conn.execute(query)

    @staticmethod
    async def add_working_hours(conn, courier_id, working_hours):
        if not working_hours:
            return
        time_start, time_finish = TimeIntervalsConverter.string_to_int_array(working_hours)
        values = [{'time_start': time_start[i], 'time_finish': time_finish[i]} for i in range(len(time_start))]
        query = working_hours_table.insert().values(values).returning(working_hours_table.c.working_hours_id)
        first_id = await conn.fetchval(query)

        values = [{'courier_id': courier_id, 'working_hours_id': first_id + i} for i in range(len(time_start))]
        query = couriers_working_hours_table.insert().values(values)
        await conn.execute(query)

    @staticmethod
    async def remove_working_hours(conn, courier_id, working_hours_ids):
        if not working_hours_ids:
            return
        conditions = []
        for working_hours_id in working_hours_ids:
            conditions.extend(
                and_(couriers_working_hours_table.c.working_hours_id == working_hours_id,
                     couriers_working_hours_table.c.courier_id == courier_id),
            )
        query = couriers_working_hours_table.delete().where(or_(*conditions))
        await conn.execute(query)

        conditions = []
        for working_hours_id in working_hours_ids:
            conditions.extend(and_(working_hours_table.c.working_hours_id == working_hours_id,
                                   working_hours_table.c.working_hours_id == working_hours_id))
        query = working_hours_table.delete().where(or_(*conditions))
        await conn.execute(query)

    @classmethod
    async def update_courier_type(cls, conn, courier_id, data):
        values = {'courier_type': data['courier_type']}
        if values:
            query = couriers_table.update().values(values).where(
                couriers_table.c.courier_id == courier_id
            )
            await conn.execute(query)

    @classmethod
    async def remove_orders(cls, conn, order_ids):
        values = {'courier_id': None, 'assignment_time': None}
        conditions = or_(*list([orders_table.c.order_id == order_id for order_id in order_ids]))
        query = orders_table.update().values(values).where(conditions)
        await conn.execute(query)

    @classmethod
    async def get_courier_orders_done_sequense_count(cls, conn, courier_id):
        query = COURIERS_ORDERS_SEQUENCES_QUERY.where(
            and_(orders_table.c.courier_id == courier_id)
        )
        total_sequences = await conn.fetchval(query)

        query = COURIERS_ORDERS_SEQUENCES_QUERY.where(
            and_(orders_table.c.completion_time == None, orders_table.c.courier_id == courier_id)
        )
        sequences_undone = await conn.fetchval(query)

        return total_sequences if sequences_undone is None else total_sequences - sequences_undone

    @classmethod
    async def get_courier_t(cls, conn, courier_id):
        query = COURIERS_ORDERS_REGIONS_QUERY.where(
            and_(orders_table.c.completion_time != None, orders_table.c.courier_id == courier_id)
        )
        regions_average_delivery_timedelta = await conn.fetch(query)
        if regions_average_delivery_timedelta == []:
            return None
        else:
            lst = [row['average_timedelta'].total_seconds() for row in regions_average_delivery_timedelta]
            if len(lst) == 1:
                return lst[0]
            return min(*lst)

    @docs(summary='Обновить указанного жителя в определенной выгрузке')
    @request_schema(CourierUpdateRequestSchema())
    @response_schema(CourierItemSchema(), code=HTTPStatus.OK.value)
    async def patch(self):
        # Транзакция требуется чтобы в случае ошибки (или отключения клиента,
        # не дождавшегося ответа) откатить частично добавленные изменения, а
        # также для получения транзакционной advisory-блокировки.
        async with self.pg.transaction() as conn:

            # Блокировка позволит избежать состояние гонки между конкурентными запросами
            await self.acquire_lock(conn, self.courier_id)
            # Получаем информацию о жителе
            courier = await self.get_courier(conn, self.courier_id)
            couriers_orders = await self.get_orders(conn, self.courier_id)
            # Обновляем таблицу couriers
            if 'courier_type' in self.request['data']:
                await self.update_courier_type(conn, self.courier_id, self.request['data'])

            if 'regions' in self.request['data']:
                cur_regions = set(courier['regions'])
                new_regions = set(self.request['data']['regions'])

                await self.remove_regions(
                    conn, self.courier_id,
                    cur_regions - new_regions
                )

                await self.add_regions(
                    conn, self.courier_id,
                    new_regions - cur_regions
                )

            if 'working_hours' in self.request['data']:
                query = couriers_working_hours_table \
                    .select() \
                    .where(couriers_working_hours_table.c.courier_id == self.courier_id)
                cur_hours_ids = [i['working_hours_id'] for i in await conn.fetch(query)]
                new_hours = set(self.request['data']['working_hours'])
                cur_hours_ids = set(cur_hours_ids)
                await self.remove_working_hours(conn, self.courier_id, cur_hours_ids)
                await self.add_working_hours(conn, self.courier_id, new_hours)

            query = COURIERS_QUERY.where(and_(
                couriers_table.c.courier_id == self.courier_id,
            ))
            courier = await conn.fetchrow(query)

            if len(couriers_orders) != 0:
                orders_to_assign_ids = await AvailableOrdersDefiner().get_orders(conn, {
                    'courier_id': courier['courier_id'],
                    'courier_type': courier['courier_type'],
                    'regions': list(dict.fromkeys(courier['regions'])),
                    'working_hours': [
                        {'time_start': courier['time_start'][i], 'time_finish': courier['time_finish'][i]}
                        for i in range(len(courier['time_start']))]
                }, courier['courier_id'])
                orders_to_decline_ids = set([i['order_id'] for i in couriers_orders]) - set(orders_to_assign_ids)
                await self.remove_orders(conn, orders_to_decline_ids)

        return Response(body={
            'courier_id': courier['courier_id'],
            'courier_type': courier['courier_type'],
            'regions': list(dict.fromkeys(courier['regions'])),
            'working_hours': TimeIntervalsConverter.int_to_string_array(time_start_intervals=courier['time_start'],
                                                                        time_finish_intervals=courier['time_finish'])
        })

    @docs(summary='Get courier information')
    # @request_schema()
    @response_schema(CourierGetResponseSchema(), code=HTTPStatus.OK.value)
    async def get(self):
        # Транзакция требуется чтобы в случае ошибки (или отключения клиента,
        # не дождавшегося ответа) откатить частично добавленные изменения.
        async with self.pg.transaction() as conn:
            courier = await self.get_courier(conn, self.courier_id)

            courier_t = await self.get_courier_t(conn, self.courier_id)
            sequences_count = await self.get_courier_orders_done_sequense_count(conn, self.courier_id)

            if courier_t:
                rating = await CourierConfigurator.calculate_rating(courier_t)
                courier["rating"] = rating
            if sequences_count:
                earnings = await CourierConfigurator.calculate_earnings(sequences_count, courier["courier_type"])
                courier["earnings"] = earnings
            else:
                courier["earnings"] = 0

            return Response(body=courier)
