from http import HTTPStatus

import pytest

from store.utils.testing.orders_testing import (
    generate_order, get_order,
    import_orders, compare_orders,
)
from store.utils.testing.couriers_testing import (
    patch_courier, generate_courier, import_couriers, compare_couriers
)

CASES = (
    (
        {'courier_id': 1, 'courier_type': 'foot', 'regions': [], 'working_hours': []},
        list([]),
        dict({'regions': [12, 24, 25]}),
        HTTPStatus.OK,
        dict({'courier_id': 1, 'courier_type': 'foot', 'regions': [12, 24, 25], 'working_hours': []}),
        list([])
    ),

    (
        {'courier_id': 1, 'courier_type': 'foot', 'regions': [], 'working_hours': []},
        list([]),
        dict({}),
        HTTPStatus.OK,
        dict({'courier_id': 1, 'courier_type': 'foot', 'regions': [], 'working_hours': []}),
        list([])
    ),
)


@pytest.mark.parametrize('courier, orders, patch, expected_status, expected_courier, orders_left', CASES)
async def test_couriers_patch(api_client, courier, orders, patch, expected_status, expected_courier, orders_left):
    await import_couriers(api_client, [courier], HTTPStatus.CREATED)
    await import_orders(api_client, orders, HTTPStatus.CREATED)

    actual_courier = await patch_courier(api_client, courier['courier_id'], patch, expected_status)
    if expected_status == HTTPStatus.OK:
        assert compare_couriers(actual_courier, expected_courier)
