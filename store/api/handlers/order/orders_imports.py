from store.api.handlers.base import BaseView

from http import HTTPStatus
from typing import Generator

from aiohttp.web_response import Response
from aiohttp_apispec import docs, request_schema, response_schema
from aiomisc import chunk_list

from store.api.schema import OrdersPostRequest, OrdersIds
from store.db.schema import orders_table, delivery_hours_table, orders_delivery_hours_table
from store.utils.pg import MAX_QUERY_ARGS


class OrdersImportsView(BaseView):
    URL_PATH = '/orders'
    MAX_ORDERS_PER_INSERT = MAX_QUERY_ARGS // len(orders_table.columns)

    @classmethod
    def make_orders_table_rows(cls, orders) -> Generator:
        for order in orders:
            yield {
                'order_id': order['order_id'],
                'weight': order['weight'],
                'region': order['region']
            }

    @classmethod
    def make_orders_ids(cls, orders) -> Generator:
        for order in orders:
            yield {
                'id': order['order_id']
            }

    @classmethod
    def make_delivery_hours_table_rows(cls, orders) -> Generator:
        for order in orders:
            for delivery_hour_interval in order['delivery_hours']:
                start_delivering, stop_delivering = \
                    delivery_hour_interval.split('-')[0], delivery_hour_interval.split('-')[1]

                yield {
                    'time_start': str(int(start_delivering.split(":")[0]) * 60 + int(start_delivering.split(":")[1])),
                    'time_finish': str(int(stop_delivering.split(":")[0]) * 60 + int(stop_delivering.split(":")[1]))
                }

    @classmethod
    def make_orders_delivery_hours_table_rows(cls, orders, delivery_hours_ids) -> Generator:
        id_counter = 0
        for order in orders:
            for i in range(len(order['delivery_hours'])):
                yield {
                    'order_id': order['order_id'],
                    'delivery_hours_id': delivery_hours_ids[id_counter]
                }
                id_counter += 1

    @docs(summary='Add import with orders information')
    @request_schema(OrdersPostRequest())
    @response_schema(OrdersIds(), code=HTTPStatus.CREATED.value)
    async def post(self):
        async with self.pg.transaction() as conn:

            orders = self.request['data']['data']
            orders_rows = self.make_orders_table_rows(orders)
            orders_ids = self.make_orders_ids(orders)
            delivery_hours_rows = self.make_delivery_hours_table_rows(orders)

            chunked_orders_rows = chunk_list(orders_rows, self.MAX_ORDERS_PER_INSERT)
            chunked_delivery_hours_rows = chunk_list(delivery_hours_rows, self.MAX_ORDERS_PER_INSERT)

            query = orders_table.insert()
            for chunk in chunked_orders_rows:
                await conn.execute(query.values(list(chunk)))

            ids = []
            query = delivery_hours_table.insert().returning(delivery_hours_table.c.delivery_hours_id)
            for chunk in chunked_delivery_hours_rows:
                first_id = await conn.fetchval(query.values(
                    [{'time_start': int(i['time_start']), 'time_finish': int(i['time_finish'])} for i in list(chunk)]))
                ids.append([first_id + i for i in range(len(list(chunk)))])
            ids = [y for x in ids for y in x]

            orders_delivery_hours_table_rows = self.make_orders_delivery_hours_table_rows(orders, ids)
            chunked_orders_delivery_hours_table_rows = chunk_list(orders_delivery_hours_table_rows,
                                                                  self.MAX_ORDERS_PER_INSERT)

            query = orders_delivery_hours_table.insert()
            for chunk in chunked_orders_delivery_hours_table_rows:
                await conn.execute(query.values(list(chunk)))

        return Response(body={'orders': list(chunk_list(orders_ids, self.MAX_ORDERS_PER_INSERT))},
                        status=HTTPStatus.CREATED)
