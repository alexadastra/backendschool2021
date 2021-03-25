from store.api.handlers.base import BaseView

from http import HTTPStatus

from aiohttp.web_exceptions import HTTPNotFound
from aiohttp.web_response import Response
from aiohttp_apispec import docs, request_schema, response_schema
from asyncpg import ForeignKeyViolationError
from marshmallow import ValidationError
from sqlalchemy import and_, or_

from store.api.schema import CourierUpdateRequest, CourierItemSchema
from store.db.schema import couriers_table, couriers_regions_table, couriers_working_hours_table, working_hours_table, \
    regions_table

from ..query import COURIERS_QUERY
from ...domain import TimeIntervalsConverter


class CourierView(BaseView):
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
            'type': courier['type'],
            'regions': list(dict.fromkeys(courier['regions'])),
            'working_hours': TimeIntervalsConverter.int_to_string_array(time_start_intervals=courier['time_start'],
                                                                        time_finish_intervals=courier['time_finish'])
        }

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
            conditions.extend(
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
    async def update_courier(cls, conn, courier_id, data):
        values = {k: v for k, v in data.items() if k not in ['regions', 'working_hours']}
        if values:
            query = couriers_table.update().values(values).where(
                couriers_table.c.courier_id == courier_id
            )
            await conn.execute(query)

    @docs(summary='Обновить указанного жителя в определенной выгрузке')
    @request_schema(CourierUpdateRequest())
    @response_schema(CourierItemSchema(), code=HTTPStatus.OK.value)
    async def patch(self):
        # Транзакция требуется чтобы в случае ошибки (или отключения клиента,
        # не дождавшегося ответа) откатить частично добавленные изменения, а
        # также для получения транзакционной advisory-блокировки.
        async with self.pg.transaction() as conn:

            # Блокировка позволит избежать состояние гонки между конкурентными
            # запросами на изменение родственников.
            await self.acquire_lock(conn, self.courier_id)
            # Получаем информацию о жителе
            courier = await self.get_courier(conn, self.courier_id)
            # Обновляем таблицу couriers
            if 'type' in self.request['data']:
                await self.update_courier(conn, self.courier_id, self.request['data'])

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

            courier = await self.get_courier(conn, self.courier_id)
        return Response(body=courier)
