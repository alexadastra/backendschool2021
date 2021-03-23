from store.api.handlers.base import BaseView

from http import HTTPStatus

from aiohttp.web_exceptions import HTTPNotFound
from aiohttp.web_response import Response
from aiohttp_apispec import docs, request_schema, response_schema
from asyncpg import ForeignKeyViolationError
from marshmallow import ValidationError
from sqlalchemy import and_, or_

from store.api.schema import CourierUpdateRequest, CourierItemSchema
from store.db.schema import couriers_table, couriers_regions_table, couriers_working_hours_table, working_hours_table

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
        print(courier['time_start'])
        return {
            'courier_id': courier['courier_id'],
            'type': courier['type'],
            'regions': list(dict.fromkeys(courier['regions'])),
            'working_hours': TimeIntervalsConverter.int_to_string_array(time_start_intervals=courier['time_start'],
                                                                        time_finish_intervals=courier['time_finish'])
        }
    """
    @staticmethod
    async def add_relatives(conn, import_id, citizen_id, relative_ids):
        if not relative_ids:
            return

        values = []
        base = {'import_id': import_id}
        for relative_id in relative_ids:
            values.append({**base, 'citizen_id': citizen_id,
                           'relative_id': relative_id})

            # Обратная связь не нужна, если житель сам себе родственник
            if citizen_id != relative_id:
                values.append({**base, 'citizen_id': relative_id,
                               'relative_id': citizen_id})
        query = relations_table.insert().values(values)

        try:
            await conn.execute(query)
        except ForeignKeyViolationError:
            raise ValidationError({'relatives': (
                f'Unable to add relatives {relative_ids}, some do not exist'
            )})

    @staticmethod
    async def remove_relatives(conn, import_id, citizen_id, relative_ids):
        if not relative_ids:
            return

        conditions = []
        for relative_id in relative_ids:
            conditions.extend([
                and_(relations_table.c.import_id == import_id,
                     relations_table.c.citizen_id == citizen_id,
                     relations_table.c.relative_id == relative_id),
                and_(relations_table.c.import_id == import_id,
                     relations_table.c.citizen_id == relative_id,
                     relations_table.c.relative_id == citizen_id)
            ])
        query = relations_table.delete().where(or_(*conditions))
        await conn.execute(query)

    @classmethod
    async def update_citizen(cls, conn, import_id, citizen_id, data):
        values = {k: v for k, v in data.items() if k != 'relatives'}
        if values:
            query = citizens_table.update().values(values).where(and_(
                citizens_table.c.import_id == import_id,
                citizens_table.c.citizen_id == citizen_id
            ))
            await conn.execute(query)
    """
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
            if not courier:
                raise HTTPNotFound()
            """
            # Обновляем таблицу citizens
            await self.update_citizen(conn, self.import_id, self.citizen_id,
                                      self.request['data'])

            if 'relatives' in self.request['data']:
                cur_relatives = set(citizen['relatives'])
                new_relatives = set(self.request['data']['relatives'])
                await self.remove_relatives(
                    conn, self.import_id, self.citizen_id,
                    cur_relatives - new_relatives
                )
                await self.add_relatives(
                    conn, self.import_id, self.citizen_id,
                    new_relatives - cur_relatives
                )
            """
            # Получаем актуальную информацию о
            courier = await self.get_courier(conn, self.courier_id)
        return Response(body={'data': courier})
