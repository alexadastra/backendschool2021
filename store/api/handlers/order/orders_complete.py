from store.api.handlers.base import BaseView

from http import HTTPStatus

from aiohttp.web_response import Response
from aiohttp.web_exceptions import HTTPBadRequest
from aiohttp_apispec import docs, request_schema, response_schema
from sqlalchemy import and_

from store.api.schema import OrdersCompletePostRequestSchema, OrdersCompletePostResponseSchema
from store.db.schema import orders_table

from ...domain import ISODatetimeFormatConverter
from ..query import COURIERS_ORDERS_LAST_COMPLETION_TIME


class OrdersCompletionView(BaseView):
    URL_PATH = r'/orders/complete'

    @classmethod
    async def define_delivery_start_time(cls, conn, courier_id, order):
        query = COURIERS_ORDERS_LAST_COMPLETION_TIME.where(and_(
            orders_table.c.completion_time != None, orders_table.c.assignment_time == order['assignment_time'],
            orders_table.c.courier_id == courier_id)
        )
        time = await conn.fetchval(query)
        delivery_start_time = order['assignment_time'] if not time else time
        values = {'delivery_start_time': delivery_start_time}
        query = orders_table.update().values(values).where(orders_table.c.order_id == order['order_id'])
        await conn.execute(query)

    @docs(summary='Set order as complete')
    @request_schema(OrdersCompletePostRequestSchema())
    @response_schema(OrdersCompletePostResponseSchema(), code=HTTPStatus.OK.value)
    async def post(self):
        # Транзакция требуется чтобы в случае ошибки (или отключения клиента,
        # не дождавшегося ответа) откатить частично добавленные изменения.
        async with self.pg.transaction() as conn:
            courier_id = self.request['data']['courier_id']
            order_id = self.request['data']['order_id']

            query = orders_table.select().where(
                and_(orders_table.c.courier_id == courier_id, orders_table.c.order_id == order_id))
            order = await conn.fetchrow(query)

            if not order or not order['assignment_time']:
                return HTTPBadRequest()

            if not order['completion_time']:
                completion_time = \
                    await ISODatetimeFormatConverter.parse_iso_string(self.request['data']['complete_time'])
                if not await ISODatetimeFormatConverter.compare_iso_strings(order['assignment_time'], completion_time):
                    raise HTTPBadRequest()

                await self.define_delivery_start_time(conn, courier_id, order)

                query = orders_table.update()\
                    .values({'completion_time': completion_time}).where(orders_table.c.order_id == order_id)
                await conn.execute(query)

            return Response(body={'order_id': order_id})
