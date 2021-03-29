from store.api.handlers.base import BaseView

from http import HTTPStatus
from typing import Generator
from sqlalchemy import select

from aiohttp.web_response import Response
from aiohttp.web_exceptions import HTTPBadRequest
from aiohttp_apispec import docs, request_schema, response_schema
from aiomisc import chunk_list

from store.api.schema import CouriersPostRequestSchema, CouriersIdsSchema
from store.db.schema import couriers_table, working_hours_table, \
    couriers_working_hours_table, regions_table, couriers_regions_table
from store.utils.pg import MAX_QUERY_ARGS
from asyncpg.exceptions import UniqueViolationError


class CouriersImportsView(BaseView):
    URL_PATH = '/couriers'
    # Так как данных может быть много, а postgres поддерживает только
    # MAX_QUERY_ARGS аргументов в одном запросе, писать в БД необходимо
    # частями.
    # Максимальное кол-во строк для вставки можно рассчитать как отношение
    # MAX_QUERY_ARGS к кол-ву вставляемых в таблицу столбцов.
    MAX_COURIERS_PER_INSERT = MAX_QUERY_ARGS // len(couriers_table.columns)
    MAX_REGIONS_PER_INSERT = MAX_QUERY_ARGS // len(regions_table.columns)
    MAX_COURIER_REGIONS_PER_INSERT = MAX_QUERY_ARGS // len(couriers_regions_table.columns)
    MAX_WORKING_HOURS_PER_INSERT = MAX_QUERY_ARGS // len(working_hours_table.columns)
    MAX_COURIERS_WORKING_HOURS_PER_INSERT = MAX_QUERY_ARGS // len(couriers_working_hours_table.columns)

    @classmethod
    def make_couriers_table_rows(cls, couriers) -> Generator:
        """
        Generates data for 'couriers' table insertion
        """
        for courier in couriers:
            yield {
                'courier_id': courier['courier_id'],
                'courier_type': courier['courier_type']
            }

    @classmethod
    def make_regions_table_rows(cls, couriers, regions) -> Generator:
        """
        Generates data for 'regions' table insertion
        """
        current_regions, new_regions = set(), set()
        for i in regions:
            current_regions.add(i['region_id'])

        for courier in couriers:
            for region in courier['regions']:
                new_regions.add(region)

        for region in new_regions - current_regions:
            yield {
                'region_id': region
            }

    @classmethod
    def make_couriers_regions_table_rows(cls, couriers) -> Generator:
        """
        Generates data for 'couriers_regions' table insertion
        """
        for courier in couriers:
            for region in courier['regions']:
                yield {
                    'courier_id': courier['courier_id'],
                    'region_id': region
                }

    @classmethod
    def make_couriers_ids(cls, couriers) -> Generator:
        for courier in couriers:
            yield {
                'id': courier['courier_id']
            }

    @classmethod
    def make_working_hours_table_rows(cls, couriers) -> Generator:
        for courier in couriers:
            for working_hour_interval in courier['working_hours']:
                start_working, stop_working = working_hour_interval.split('-')[0], working_hour_interval.split('-')[1]

                yield {
                    'time_start': str(int(start_working.split(":")[0]) * 60 + int(start_working.split(":")[1])),
                    'time_finish': str(int(stop_working.split(":")[0]) * 60 + int(stop_working.split(":")[1]))
                }

    @classmethod
    def make_couriers_working_hours_table_rows(cls, couriers, working_hours_ids) -> Generator:
        id_counter = 0
        for courier in couriers:
            for i in range(len(courier['working_hours'])):
                yield {
                    'courier_id': courier['courier_id'],
                    'working_hours_id': working_hours_ids[id_counter]
                }
                id_counter += 1

    @docs(summary='Add import with couriers information')
    @request_schema(CouriersPostRequestSchema())
    @response_schema(CouriersIdsSchema(), code=HTTPStatus.CREATED.value)
    async def post(self):
        # Транзакция требуется чтобы в случае ошибки (или отключения клиента,
        # не дождавшегося ответа) откатить частично добавленные изменения.
        async with self.pg.transaction() as conn:

            query = select([regions_table.c.region_id]).select_from(regions_table)
            regions = await self.pg.fetch(query)

            couriers = self.request['data']['data']
            couriers_rows = self.make_couriers_table_rows(couriers)
            couriers_ids = self.make_couriers_ids(couriers)
            regions_rows = self.make_regions_table_rows(couriers, regions)
            couriers_regions_rows = self.make_couriers_regions_table_rows(couriers)

            working_hours_rows = self.make_working_hours_table_rows(couriers)

            # Чтобы уложиться в ограничение кол-ва аргументов в запросе к
            # postgres, а также сэкономить память и избежать создания полной
            # копии данных присланных клиентом во время подготовки - используем
            # генератор chunk_list.
            # Он будет получать из генератора make_citizens_table_rows только
            # необходимый для 1 запроса объем данных.
            chunked_couriers_rows = chunk_list(couriers_rows, self.MAX_COURIERS_PER_INSERT)
            chunked_regions_rows = chunk_list(regions_rows, self.MAX_REGIONS_PER_INSERT)
            chunked_couriers_regions_rows = chunk_list(couriers_regions_rows, self.MAX_COURIER_REGIONS_PER_INSERT)
            chunked_working_hours_rows = chunk_list(working_hours_rows, self.MAX_WORKING_HOURS_PER_INSERT)

            query = couriers_table.insert()
            for chunk in chunked_couriers_rows:
                try:
                    await conn.execute(query.values(list(chunk)))
                # if couriers already exist, throw 400 bad request
                except UniqueViolationError:
                    raise HTTPBadRequest()

            query = regions_table.insert()
            for chunk in chunked_regions_rows:
                await conn.execute(query.values(list(chunk)))

            query = couriers_regions_table.insert()
            for chunk in chunked_couriers_regions_rows:
                await conn.execute(query.values(list(chunk)))

            ids = []
            query = working_hours_table.insert().returning(working_hours_table.c.working_hours_id)
            for chunk in chunked_working_hours_rows:
                first_id = await conn.fetchval(query.values(
                    [{'time_start': int(i['time_start']), 'time_finish': int(i['time_finish'])} for i in list(chunk)]))
                ids.append([first_id + i for i in range(len(list(chunk)))])
            ids = [y for x in ids for y in x]

            couriers_working_hours_table_rows = self.make_couriers_working_hours_table_rows(couriers, ids)
            chunked_couriers_working_hours_table_rows = chunk_list(couriers_working_hours_table_rows,
                                                                   self.MAX_COURIERS_WORKING_HOURS_PER_INSERT)

            query = couriers_working_hours_table.insert()
            for chunk in chunked_couriers_working_hours_table_rows:
                await conn.execute(query.values(list(chunk)))

            return Response(body={'couriers': list(couriers_ids)},
                            status=HTTPStatus.CREATED)
