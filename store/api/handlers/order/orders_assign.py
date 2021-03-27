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

from ..query import COURIERS_QUERY, ORDERS_QUERY, AVAILABLE_ORDERS_QUERY
from ...domain import CouriersOrdersResolver, CourierConfigurator


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
    def get_overlap(a, b):
        return min(a[1], b[1]) - max(a[0], b[0]) > 0

    @staticmethod
    async def get_available_orders(conn, courier):
        if not courier:
            return

        region_conditions = or_(*list([orders_table.c.region == region for region in courier['regions']]))

        # according to the task, ends of interval are not counted,
        # so working_hours and delivery_hours are intersected if
        # min(working_finish, delivery_finish) - max(working_start, delivery_start) > 0
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
                # condition delivery_time_finish - delivery_time_start > 0,
                # as it is checked in POST /orders request validation
            ))
            hours_conditions.append(and_(
                working_hours['time_start'] <= delivery_hours_table.c.time_start,
                working_hours['time_finish'] > delivery_hours_table.c.time_finish
                # condition working_time_finish - working_time_start > 0,
                # as it is checked in POST /couriers request validation
            ))
            hours_conditions.append(and_(
                working_hours['time_start'] <= delivery_hours_table.c.time_start,
                working_hours['time_finish'] <= delivery_hours_table.c.time_finish,
                working_hours['time_finish'] - delivery_hours_table.c.time_start > 0
            ))

        query = AVAILABLE_ORDERS_QUERY.where(and_(
            region_conditions,
            or_(*hours_conditions),
            orders_table.c.courier_id == None,
            orders_table.c.weight <= courier['carrying_capacity']
        ))
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
                                      'assignment_time': orders[0]['assignment_time'][0].isoformat("T") + "Z"})

            courier['carrying_capacity'] = await CourierConfigurator.get_courier_carrying_capacity(courier['type'])

            orders = await self.get_available_orders(conn, courier)
            if not orders:
                return Response(text='[]', content_type='application/json')

            orders_to_assign_ids = await CouriersOrdersResolver(
                orders_={orders[i]['order_id']: orders[i]['weight'] for i in range(len(orders))},
                max_weight=courier['carrying_capacity']).resolve_orders()

            assignment_time = datetime.utcnow()
            await self.assign_orders(conn, orders_to_assign_ids, courier_id, assignment_time)

            return Response(body={'orders': [{'id': id_} for id_ in orders_to_assign_ids],
                                  'assignment_time': assignment_time.isoformat("T") + "Z"})
