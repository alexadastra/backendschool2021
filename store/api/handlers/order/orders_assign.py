from store.api.handlers.base import BaseView

from http import HTTPStatus
from aiohttp.web_response import Response
from aiohttp.web_exceptions import HTTPNotFound
from aiohttp_apispec import docs, request_schema, response_schema
from sqlalchemy import and_, or_

from store.api.schema import OrdersAssignPostRequest, OrdersAssignPostResponse
from store.db.schema import orders_table, couriers_table

from ..query import COURIERS_QUERY, ORDERS_QUERY, AvailableOrdersDefiner
from ...domain import ISODatetimeFormatConverter


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
        return await conn.fetch(query)

    @staticmethod
    async def assign_orders(conn, orders_ids, courier_id, assignment_time):
        values = {'courier_id': courier_id, 'assignment_time': assignment_time}
        conditions = or_(*list([orders_table.c.order_id == order_id for order_id in orders_ids]))
        query = orders_table.update().values(values).where(conditions)
        await conn.execute(query)

    @docs(summary='Assign orders to couriers')
    @request_schema(OrdersAssignPostRequest())
    @response_schema(OrdersAssignPostResponse(), code=HTTPStatus.OK.value)
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
                                      'assign_time': orders[0]['assignment_time'][0].isoformat("T") + "Z"})

            orders_to_assign_ids = await AvailableOrdersDefiner().get_orders(conn, courier)
            if len(orders_to_assign_ids) == 0:
                return Response(text='[]', content_type='application/json')

            assignment_time = ISODatetimeFormatConverter.get_now()
            await self.assign_orders(conn, orders_to_assign_ids, courier_id, assignment_time)

            return Response(body={'orders': [{'id': id_} for id_ in orders_to_assign_ids],
                                  'assign_time': await ISODatetimeFormatConverter.parse_datetime(assignment_time)})
