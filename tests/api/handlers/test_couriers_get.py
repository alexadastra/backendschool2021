from http import HTTPStatus
import datetime
import pytest
from store.api.domain import ISODatetimeFormatConverter

from store.utils.testing.orders_testing import (
    generate_order, get_order,
    import_orders, compare_orders, assign_orders, complete_orders
)
from store.utils.testing.couriers_testing import (
    patch_courier, generate_courier, import_couriers, compare_couriers, get_courier
)

LONGEST_STR = 'ё' * 256
CASES = (
    (
        {'courier_id': 1, 'courier_type': 'foot', 'regions': [], 'working_hours': []},
        [],
        [],
        HTTPStatus.OK,
        {'courier_id': 1, 'courier_type': 'foot', 'regions': [], 'working_hours': [], "earnings": 0}
    ),

    (
        {'courier_id': 1, 'courier_type': 'foot', 'regions': [1], 'working_hours': ["09:00-18:00"]},
        [{'order_id': 1, 'weight': 0.1, 'region': 1, 'delivery_hours': ["09:00-18:00"]}],
        [],
        HTTPStatus.OK,
        {'courier_id': 1, 'courier_type': 'foot', 'regions': [1], 'working_hours': ["09:00-18:00"], "earnings": 0}
    ),

    (
        {'courier_id': 1, 'courier_type': 'foot', 'regions': [1], 'working_hours': ["09:00-18:00"]},
        [{'order_id': 1, 'weight': 0.1, 'region': 1, 'delivery_hours': ["09:00-18:00"]}],
        [{'id': 1, 'time': 0}],
        HTTPStatus.OK,
        {'courier_id': 1, 'courier_type': 'foot', 'regions': [1],
         'working_hours': ["09:00-18:00"], "rating": 5.0, "earnings": 1000}
    ),

    (
        {'courier_id': 1, 'courier_type': 'foot', 'regions': [1], 'working_hours': ["09:00-18:00"]},
        [{'order_id': 1, 'weight': 0.1, 'region': 1, 'delivery_hours': ["09:00-18:00"]}],
        [{'id': 1, 'time': 10}],
        HTTPStatus.OK,
        {'courier_id': 1, 'courier_type': 'foot', 'regions': [1],
         'working_hours': ["09:00-18:00"], "rating": 4.17, "earnings": 1000}
    ),

    (
        {'courier_id': 1, 'courier_type': 'foot', 'regions': [1], 'working_hours': ["09:00-18:00"]},
        [{'order_id': 1, 'weight': 0.1, 'region': 1, 'delivery_hours': ["09:00-18:00"]}],
        [{'id': 1, 'time': 60}],
        HTTPStatus.OK,
        {'courier_id': 1, 'courier_type': 'foot', 'regions': [1],
         'working_hours': ["09:00-18:00"], "rating": 0.0, "earnings": 1000}
    ),

    (
        {'courier_id': 1, 'courier_type': 'foot', 'regions': [1], 'working_hours': ["09:00-18:00"]},
        [{'order_id': 1, 'weight': 0.1, 'region': 1, 'delivery_hours': ["09:00-18:00"]},
         {'order_id': 2, 'weight': 0.1, 'region': 1, 'delivery_hours': ["09:00-18:00"]}],
        [{'id': 1, 'time': 10}],
        HTTPStatus.OK,
        {'courier_id': 1, 'courier_type': 'foot', 'regions': [1],
         'working_hours': ["09:00-18:00"], "rating": 4.17, "earnings": 0}
    ),

)


@pytest.mark.parametrize('courier, orders_assign, orders_complete_ids, expected_status, expected_courier', CASES)
async def test_couriers_get(api_client, courier, orders_assign, orders_complete_ids, expected_status, expected_courier):
    await import_couriers(api_client, [courier], HTTPStatus.CREATED)
    await import_orders(api_client, orders_assign, HTTPStatus.CREATED)

    assignment = await assign_orders(api_client, courier['courier_id'])
    if assignment['orders']:
        assign_time = await ISODatetimeFormatConverter.parse_iso_string(assignment['assign_time'])

        for order in orders_complete_ids:
            complete_time = assign_time + datetime.timedelta(minutes=order['time'])
            await complete_orders(api_client,
                                  courier['courier_id'],
                                  order['id'],
                                  complete_time.isoformat("T") + "Z"
                                  )

    actual_courier = await get_courier(api_client, courier['courier_id'], expected_status)
    if expected_status == HTTPStatus.OK:
        assert compare_couriers(actual_courier, expected_courier)
