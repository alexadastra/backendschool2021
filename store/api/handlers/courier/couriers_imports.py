from store.api.handlers.base import BaseView

from http import HTTPStatus
from typing import Generator

from aiohttp.web_response import Response
from aiohttp_apispec import docs, request_schema, response_schema
from aiomisc import chunk_list

from store.api.schema import CouriersPostRequestSchema, CouriersIdsSchema
from store.db.schema import couriers_table, couriers_imports_table, working_hours_table, \
    couriers_working_hours_table, regions_table, couriers_regions_table
from store.utils.pg import MAX_QUERY_ARGS


class CouriersImportsView(BaseView):
    URL_PATH = '/couriers'
    # Так как данных может быть много, а postgres поддерживает только
    # MAX_QUERY_ARGS аргументов в одном запросе, писать в БД необходимо
    # частями.
    # Максимальное кол-во строк для вставки можно рассчитать как отношение
    # MAX_QUERY_ARGS к кол-ву вставляемых в таблицу столбцов.
    MAX_CITIZENS_PER_INSERT = MAX_QUERY_ARGS // len(couriers_table.columns)

    @classmethod
    def make_couriers_table_rows(cls, couriers, import_id) -> Generator:
        """
        Генерирует данные готовые для вставки в таблицу citizens (с ключом
        import_id и без ключа relatives).
        """
        for courier in couriers:
            yield {
                'import_id': import_id,
                'courier_id': courier['courier_id'],
                'type': courier['type']
            }

    @classmethod
    def make_regions_table_rows(cls, couriers, import_id) -> Generator:
        """
        Generates data for 'regions' table insertion
        """
        for courier in couriers:
            for regions in courier['regions']:
                yield {
                    region_id :
                }


    @docs(summary='Add import with couriers information')
    @request_schema(ImportSchema())
    @response_schema(ImportResponseSchema(), code=HTTPStatus.CREATED.value)
    async def post(self):
        # Транзакция требуется чтобы в случае ошибки (или отключения клиента,
        # не дождавшегося ответа) откатить частично добавленные изменения.
        async with self.pg.transaction() as conn:
            # Создаем выгрузку
            query = couriers_imports_table.insert().returning(couriers_imports_table.c.import_id)
            import_id = await conn.fetchval(query)
            query = regions_table.insert().returning(regions_table.c.region_id)
            region_id = await conn.fetchval(query)
            # Генераторы make_citizens_table_rows и make_relations_table_rows
            # лениво генерируют данные, готовые для вставки в таблицы citizens
            # и relations на основе данных отправленных клиентом.
            citizens = self.request['data']['citizens']
            citizen_rows = self.make_citizens_table_rows(citizens, import_id)
            relation_rows = self.make_relations_table_rows(citizens, import_id)

            # Чтобы уложиться в ограничение кол-ва аргументов в запросе к
            # postgres, а также сэкономить память и избежать создания полной
            # копии данных присланных клиентом во время подготовки - используем
            # генератор chunk_list.
            # Он будет получать из генератора make_citizens_table_rows только
            # необходимый для 1 запроса объем данных.
            chunked_citizen_rows = chunk_list(citizen_rows,
                                              self.MAX_CITIZENS_PER_INSERT)
            chunked_relation_rows = chunk_list(relation_rows,
                                               self.MAX_RELATIONS_PER_INSERT)

            query = citizens_table.insert()
            for chunk in chunked_citizen_rows:
                await conn.execute(query.values(list(chunk)))

            query = relations_table.insert()
            for chunk in chunked_relation_rows:
                await conn.execute(query.values(list(chunk)))

        return Response(body={'data': {'import_id': import_id}},
                        status=HTTPStatus.CREATED)

