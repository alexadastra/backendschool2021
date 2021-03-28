from store.api.handlers.base import BaseView

from http import HTTPStatus

from aiohttp.web_exceptions import HTTPNotFound
from aiohttp.web_response import Response
from aiohttp_apispec import docs, response_schema
from sqlalchemy import and_

from store.api.schema import OrderItemSchema
from store.db.schema import orders_table

from ..query import ORDERS_QUERY
from ...domain import TimeIntervalsConverter


class OrdersView(BaseView):
    URL_PATH = r'/orders/{order_id:\d+}'

    @property
    def order_id(self):
        return int(self.request.match_info.get('order_id'))

    @classmethod
    async def get_order(cls, conn, order_id):
        query = ORDERS_QUERY.where(and_(orders_table.c.order_id == order_id))
        order = await conn.fetchrow(query)
        if not order:
            raise HTTPNotFound()
        return {
            'order_id': order['order_id'],
            'courier_id': -1 if not order['courier_id'] else order['courier_id'],
            'weight': order['weight'],
            'region': order['region'],
            'delivery_hours': TimeIntervalsConverter.int_to_string_array(time_start_intervals=order['time_start'],
                                                                         time_finish_intervals=order['time_finish']),
            'assign_time': "" if not order['assignment_time'] else order['assignment_time'],
            'complete_time': "" if not order['completion_time'] else order['completion_time']
        }

    @docs(summary='Get courier information')
    # @request_schema()
    @response_schema(OrderItemSchema(), code=HTTPStatus.OK.value)
    async def get(self):
        # Транзакция требуется чтобы в случае ошибки (или отключения клиента,
        # не дождавшегося ответа) откатить частично добавленные изменения.
        async with self.pg.transaction() as conn:
            order = await self.get_order(conn, self.order_id)

            return Response(body=order)
