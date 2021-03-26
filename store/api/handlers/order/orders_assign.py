from store.api.handlers.base import BaseView

from http import HTTPStatus
from typing import Generator
from datetime import datetime
from aiohttp.web_response import Response
from aiohttp.web_exceptions import HTTPNotFound
from aiohttp_apispec import docs, request_schema, response_schema
from sqlalchemy import and_, or_

from store.api.schema import OrdersAssignPostRequest, OrdersAssignPostResponse
from store.db.schema import orders_table, couriers_table, orders_delivery_hours_table, delivery_hours_table, \
    working_hours_table, couriers_working_hours_table
from store.utils.pg import MAX_QUERY_ARGS

from ..query import COURIERS_QUERY, ORDERS_QUERY, AVAILABLE_ORDERS_QUERY


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

    @staticmethod
    def get_overlap(a, b):
        return min(a[1], b[1]) - max(a[0], b[0]) > 0

    @staticmethod
    async def get_available_orders(conn, courier):
        if not courier:
            return
        region_conditions = []
        for region in courier['regions']:
            region_conditions.extend(and_(orders_table.c.region == region,
                                          couriers_working_hours_table.c.courier_id == courier['courier_id']))

        # according to the task, ends of interval are not counted,
        # so working_hours and delivery_hours are intersected if
        # min(working_finish, delivery_finish) - max(working_start, delivery_start) > 0
        # hours_conditions = []
        # for i in range(len(courier['time_start'])):
        #     hours_conditions.extend(and_())

        hours_conditions = []

        for working_hours in courier['working_hours']:
            hours_conditions.append(and_(
                    working_hours['time_start'] > delivery_hours_table.c.time_start,
                    working_hours['time_finish'] > delivery_hours_table.c.time_finish,
                    delivery_hours_table.c.time_finish - working_hours['time_start'] > 0
            ))
            hours_conditions.append(and_(
                    working_hours['time_start'] > delivery_hours_table.c.time_start,
                    working_hours['time_finish'] <= delivery_hours_table.c.time_finish,
            ))
            hours_conditions.append(and_(
                    working_hours['time_start'] <= delivery_hours_table.c.time_start,
                    working_hours['time_finish'] > delivery_hours_table.c.time_finish,
            ))
            hours_conditions.append(and_(
                    working_hours['time_start'] <= delivery_hours_table.c.time_start,
                    working_hours['time_finish'] <= delivery_hours_table.c.time_finish,
                    working_hours['time_finish'] - delivery_hours_table.c.time_start > 0
            ))

        query = AVAILABLE_ORDERS_QUERY.where(and_(
            or_(*region_conditions),
            or_(*hours_conditions),
            ))
        available_orders = await conn.fetch(query)

        # for order in region_available_orders:
        #    for delivery_hour_interval in order['delivery_hours']:
        #        for working_hour_interval in courier['working_hours']:
        #            if OrdersAssignmentView.get_overlap(delivery_hour_interval['time_start'])

        return available_orders

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
                                      'assignment_time': orders[0]['assignment_time'][0].isoformat("T") + "Z"})
                # 'assignment_time': datetime.utcnow().isoformat("T") + "Z"})

            orders = await self.get_available_orders(conn, courier)
            if not orders:
                return Response(body={[]})
