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

from ..query import AVAILABLE_ORDERS_QUERY
from ...domain import CouriersOrdersResolver, CourierConfigurator


class AvailableOrdersDefiner:
    @staticmethod
    async def get_available_orders(conn, courier, courier_id=None):
        if not courier:
            return

        if not courier['working_hours'] or not courier['regions']:
            return []

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
            orders_table.c.courier_id == courier_id,
            orders_table.c.weight <= courier['carrying_capacity']
        ))
        return await conn.fetch(query)

    async def get_orders(self, conn, courier, courier_id=None):
        courier['carrying_capacity'] = await CourierConfigurator.get_courier_carrying_capacity(courier['type'])

        orders = await self.get_available_orders(conn, courier, courier_id)
        if not orders:
            return []

        orders_to_assign_ids = await CouriersOrdersResolver(
            orders_={orders[i]['order_id']: orders[i]['weight'] for i in range(len(orders))},
            max_weight=courier['carrying_capacity']).resolve_orders()

        return orders_to_assign_ids
