from http import HTTPStatus

import pytest
import datetime
from store.utils.testing.couriers_testing import generate_couriers, import_couriers
from store.utils.testing.orders_testing import assign_orders, import_orders, complete_orders
from store.api.domain.iso_datetime_formats_converter import ISODatetimeFormatConverter

CASES = (
    (
        {'courier_id': 1, 'courier_type': 'foot', 'regions': [], 'working_hours': []},
        [],
        [],
        HTTPStatus.OK,
        []
    ),

    (
        {'courier_id': 1, 'courier_type': 'foot', 'regions': [], 'working_hours': []},
        [{'order_id': 1, 'weight': 0.1, 'region': 1, 'delivery_hours': ["09:00-18:00"]}],
        [],
        HTTPStatus.OK,
        []
    ),

    (
        {'courier_id': 1, 'courier_type': 'foot', 'regions': [], 'working_hours': []},
        [{'order_id': 1, 'weight': 0.1, 'region': 1, 'delivery_hours': ["09:00-18:00"]}],
        [],
        HTTPStatus.OK,
        []
    ),

    (
        {'courier_id': 1, 'courier_type': 'foot', 'regions': [1], 'working_hours': ["09:00-18:00"]},
        [{'order_id': 1, 'weight': 0.1, 'region': 1, 'delivery_hours': ["09:00-18:00"]}],
        [{'id': 1, 'time': 10}],
        HTTPStatus.OK,
        [1]
    ),

    (
        {'courier_id': 1, 'courier_type': 'foot', 'regions': [1], 'working_hours': ["09:00-18:00"]},
        [{'order_id': 1, 'weight': 0.1, 'region': 1, 'delivery_hours': ["09:00-18:00"]},
         {'order_id': 2, 'weight': 0.2, 'region': 1, 'delivery_hours': ["09:00-18:00"]}],
        [{'id': 1, 'time': 10}, {'id': 2, 'time': 20}],
        HTTPStatus.OK,
        [1, 2]
    ),

    (
        {'courier_id': 1, 'courier_type': 'foot', 'regions': [1], 'working_hours': ["09:00-18:00"]},
        [{'order_id': 1, 'weight': 0.1, 'region': 1, 'delivery_hours': ["09:00-18:00"]}],
        [{'id': 1, 'time': 0}],
        HTTPStatus.BAD_REQUEST,
        []
    ),
)


@pytest.mark.parametrize('courier, orders, order_complete, expected_status, orders_completed_ids', CASES)
async def test_orders_post_complete(api_client, courier, orders, order_complete, expected_status, orders_completed_ids):
    await import_couriers(api_client, [courier], HTTPStatus.CREATED)
    await import_orders(api_client, orders, HTTPStatus.CREATED)

    assignment = await assign_orders(api_client, courier['courier_id'])
    if assignment['orders']:
        assign_time = await ISODatetimeFormatConverter.parse_iso_string(assignment['assign_time'])

    ids = []
    for order in order_complete:
        complete_time = assign_time + datetime.timedelta(minutes=order['time'])
        order_id = await complete_orders(api_client,
                              courier['courier_id'],
                              order['id'],
                              complete_time.isoformat("T") + "Z", expected_status)
        second_order_id = await complete_orders(api_client,
                                         courier['courier_id'],
                                         order['id'],
                                         complete_time.isoformat("T") + "Z", expected_status)
        third_order_id = await complete_orders(api_client,
                                         courier['courier_id'],
                                         order['id'],
                                         complete_time.isoformat("T") + "Z", expected_status)
        assert order_id == second_order_id and second_order_id == third_order_id
        if expected_status == HTTPStatus.OK:
            ids.append(order_id['order_id'])

    if expected_status == HTTPStatus.OK:
        ids.sort()
        orders_completed_ids.sort()
        assert orders_completed_ids == ids
