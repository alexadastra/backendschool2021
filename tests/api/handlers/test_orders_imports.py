from http import HTTPStatus
from store.utils.pg import MAX_INTEGER
import pytest

from store.utils.testing.orders_testing import (
    generate_order, generate_orders, get_order,
    import_orders, compare_orders
)

LONGEST_STR = 'ё' * 256
CASES = (
    # one single order
    (
        [
            generate_order()
        ],
        HTTPStatus.CREATED
    ),

    # few orders
    (
        [
            generate_order(order_id=1, region=2, delivery_hours=["09:00-18:00"]),
            generate_order(order_id=2, region=3, delivery_hours=["09:00-13:00", "19:00-21:00"]),
            generate_order(order_id=3, region=1, delivery_hours=["09:00-18:00"])
        ],
        HTTPStatus.CREATED
    ),

    # no orders
    (
        [],
        HTTPStatus.CREATED
    ),

    # incorrect id
    (
        [
            generate_order(order_id=-1),
        ],
        HTTPStatus.BAD_REQUEST
    ),

    # not unique ids
    (
        [
            generate_order(order_id=1),
            generate_order(order_id=1),
        ],
        HTTPStatus.BAD_REQUEST
    ),

    # invalid field
    (
        [{
            "order_id": 1,
            "weight": 0.23,
            "region": 12,
            "delivery_hours": [
                "09:00-18:00"
            ],
            "gdfh": "xgfch"
        }],
      HTTPStatus.BAD_REQUEST
    ),

    # missing field
    (
        [{
            "order_id": 1,
            "weight": 0.23,
            "delivery_hours": [
                "09:00-18:00"
            ],
        }],
      HTTPStatus.BAD_REQUEST
    ),

    # empty delivery hours
    (
        [{
            "order_id": 1,
            "weight": 0.23,
            "region": 1,
            "delivery_hours": [],
        }],
      HTTPStatus.BAD_REQUEST
    ),

    # too heavy order
    (
        [{
            "order_id": 1,
            "weight": 100,
            "region": 12,
            "delivery_hours": [
                "09:00-18:00"
            ],
        }],
      HTTPStatus.BAD_REQUEST
    ),

    # too light order
    (
        [{
            "order_id": 1,
            "weight": 0.0001,
            "region": 12,
            "delivery_hours": [
                "09:00-18:00"
            ],
        }],
      HTTPStatus.BAD_REQUEST
    ),



    # standart input
    (
        [
        {
            "order_id": 1,
            "weight": 0.23,
            "region": 12,
            "delivery_hours": [
                "09:00-18:00"
            ]
        },
        {
            "order_id": 2,
            "weight": 15,
            "region": 1,
            "delivery_hours": [
                "09:00-18:00"
            ]
        },
        {
            "order_id": 3,
            "weight": 0.01,
            "region": 22,
            "delivery_hours": [
                "09:00-12:00",
                "16:00-21:30"
            ]
        }
        ],
        HTTPStatus.CREATED
    )

)


@pytest.mark.parametrize('orders, expected_status', CASES)
async def test_orders_import(api_client, orders, expected_status):
    data = await import_orders(api_client, orders, expected_status)

    # Проверяем, что данные успешно импортированы
    if expected_status == HTTPStatus.CREATED:
        for order in orders:
            imported_order = await get_order(api_client, order['order_id'])
            imported_order = {k: imported_order[k] for k in imported_order.keys() if k in order.keys()}
            assert compare_orders(order, imported_order)

"""
# the largest input possible
    (
        generate_orders(
            orders_num=10000,
            start_order_id=MAX_INTEGER - 10000,
        ),
        HTTPStatus.CREATED
    ),
"""