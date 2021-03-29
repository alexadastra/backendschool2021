from http import HTTPStatus

import pytest
from store.utils.testing.couriers_testing import generate_couriers, import_couriers

from store.utils.testing.orders_testing import assign_orders, import_orders

CASES = (
    (
        {'courier_id': 1, 'courier_type': 'foot', 'regions': [], 'working_hours': []},
        [],
        HTTPStatus.OK,
        []
    ),

    (
        {'courier_id': 1, 'courier_type': 'foot', 'regions': [], 'working_hours': []},
        [{'order_id': 1, 'weight': 0.1, 'region': 1, 'delivery_hours': ["09:00-18:00"]}],
        HTTPStatus.OK,
        []
    ),
)


@pytest.mark.parametrize('courier, orders, expected_status, orders_assigned_ids', CASES)
async def test_orders_post_assign(api_client, courier, orders, expected_status, orders_assigned_ids):
    await import_couriers(api_client, [courier], HTTPStatus.CREATED)
    await import_orders(api_client, orders, HTTPStatus.CREATED)

    first_assignment = await assign_orders(api_client, courier['courier_id'], expected_status)
    second_assignment = await assign_orders(api_client, courier['courier_id'], expected_status)
    third_assignment = await assign_orders(api_client, courier['courier_id'], expected_status)

    assert first_assignment == second_assignment and second_assignment == third_assignment
