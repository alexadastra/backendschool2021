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
        """
        looks up for courier in table by id. If not found returns 404
        :param conn: sql connection
        :param courier_id: int id
        :return: courier entity for output
        """
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
        """
        selects incomplete orders assigned to courier with id
        :param conn: sql connection
        :param courier_id: int id
        :return: list of order records
        """
        query = orders_table.select().where(
            and_(orders_table.c.courier_id == courier_id, orders_table.c.completion_time == None))
        return await conn.fetch(query)

    @staticmethod
    async def add_regions(conn, courier_id, region_ids):
        """
        add regions to table if they're not there, add relation between courier and regions
        :param conn: sql connection
        :param courier_id: int id
        :param region_ids: {int ids}
        """
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
        """
        removes each relation between courier and regions
        :param conn: sql connection
        :param courier_id: int id
        :param region_ids: {int ids}
        """
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
        """
        adds working hours and relations with courier
        :param conn: sql connection
        :param courier_id: int id
        :param working_hours: ['hh:mm-hh:mm']
        """
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
        """
        removes working hours and relations with courier
        :param conn: sql connection
        :param courier_id: int id
        :param working_hours: ['hh:mm-hh:mm']
        """
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
        """
        changes couriers type
        :param conn: sql connection
        :param courier_id: int id
        :param data: courier_type
        :return:
        """
        values = {'courier_type': data['courier_type']}
        if values:
            query = couriers_table.update().values(values).where(
                couriers_table.c.courier_id == courier_id
            )
            await conn.execute(query)

    @classmethod
    async def remove_orders(cls, conn, order_ids):
        """
        removes orders that courier can't deliver (mark as not having courier)
        :param conn: sql connection
        :param order_ids: [int ids]
        """
        values = {'courier_id': None, 'assignment_time': None, 'deliver_start_time': None}
        conditions = or_(*list([orders_table.c.order_id == order_id for order_id in order_ids]))
        query = orders_table.update().values(values).where(conditions)
        await conn.execute(query)

    @classmethod
    async def get_courier_orders_done_sequense_count(cls, conn, courier_id):
        """
        counts done order sequences
        :param conn: sql connection
        :param courier_id: int
        :return: int
        """
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
        """
        Counts minimum of average felivery time
        :param conn:
        :param courier_id:
        :return:
        """
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


            await self.acquire_lock(conn, self.courier_id)

            courier = await self.get_courier(conn, self.courier_id)
            couriers_orders = await self.get_orders(conn, self.courier_id)

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

        async with self.pg.transaction() as conn:

            await self.acquire_lock(conn, self.courier_id)

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
