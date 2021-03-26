from store.api.handlers.base import BaseView

from http import HTTPStatus
from typing import Generator
from datetime import datetime
from aiohttp.web_response import Response
from aiohttp.web_exceptions import HTTPNotFound
from aiohttp_apispec import docs, request_schema, response_schema
from sqlalchemy import and_, or_

from store.api.schema import OrdersAssignPostRequest, OrdersAssignPostResponse
from store.db.schema import orders_table, couriers_table, orders_delivery_hours_table
from store.utils.pg import MAX_QUERY_ARGS

from ..query import COURIERS_QUERY, ORDERS_QUERY


class OrdersAssignmentView(BaseView):
    URL_PATH = r'/orders/assign'

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
            'working_hours': [
                {'time_start': courier['time_start'][i], 'time_finish': courier['time_finish'][i]}
                for i in range(len(courier['time_start']))]
        }

    @staticmethod
    async def get_couriers_orders(conn, courier_id):
        query = ORDERS_QUERY.where(and_(orders_table.c.courier_id == courier_id))
        orders = await conn.fetch(query)
        return orders

    @docs(summary='Assign orders to couriers')
    @request_schema(OrdersAssignPostRequest())
    @response_schema(OrdersAssignPostResponse(), code=HTTPStatus.CREATED.value)
    async def post(self):
        # Транзакция требуется чтобы в случае ошибки (или отключения клиента,
        # не дождавшегося ответа) откатить частично добавленные изменения.
        async with self.pg.transaction() as conn:
            courier_id = self.request['data']['courier_id']
            courier = await self.get_courier(conn, courier_id)
            orders = await self.get_couriers_orders(conn, courier_id)
            if orders:
                return Response(body={'orders':
                                          [{'id': orders[i]['order_id']} for i in range(len(orders))],
                                      'assignment_time': orders[0]['assignment_time'][0].isoformat("T") + "Z"})
                # 'assignment_time': datetime.utcnow().isoformat("T") + "Z"})
