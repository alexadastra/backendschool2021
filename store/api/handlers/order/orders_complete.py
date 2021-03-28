from store.api.handlers.base import BaseView

from http import HTTPStatus

from aiohttp.web_response import Response
from aiohttp.web_exceptions import HTTPBadRequest
from aiohttp_apispec import docs, request_schema, response_schema
from sqlalchemy import and_

from store.api.schema import OrdersCompletePostRequestSchema, OrdersCompletePostResponseSchema
from store.db.schema import orders_table

from ...domain import ISODatetimeFormatConverter


class OrdersCompletionView(BaseView):
    URL_PATH = r'/orders/complete'

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
                query = orders_table.update()\
                    .values({'completion_time': completion_time}).where(orders_table.c.order_id == order_id)
                await conn.execute(query)

            return Response(body={'order_id': order_id})
