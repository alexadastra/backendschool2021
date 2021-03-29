from http import HTTPStatus
from store.utils.pg import MAX_INTEGER
import pytest

from store.utils.testing.orders_testing import (
    generate_order, generate_orders, get_order,
    import_orders, compare_orders
)

LONGEST_STR = 'ё' * 256
CASES = (
    # Житель без родственников.
    # Обработчик должен корректно создавать выгрузку с одним жителем.
    (
        [
            generate_order()
        ],
        HTTPStatus.CREATED
    ),

    # Житель с несколькими родственниками.
    # Обработчик должен корректно добавлять жителей и создавать
    # родственные связи.
    (
        [
            generate_order(order_id=1, region=2, delivery_hours=["09:00-18:00"]),
            generate_order(order_id=2, region=3, delivery_hours=["09:00-13:00", "19:00-21:00"]),
            generate_order(order_id=3, region=1, delivery_hours=["09:00-18:00"])
        ],
        HTTPStatus.CREATED
    ),

    # Выгрузка с максимально длинными/большими значениями.
    # aiohttp должен разрешать запросы такого размера, а обработчик не должен
    # на них падать.



    # Пустая выгрузка
    # Обработчик не должен падать на таких данных.
    (
        [],
        HTTPStatus.CREATED
    ),

    # Дата рождения некорректная (в будущем)
    (
        [
            generate_order(order_id=-1),
        ],
        HTTPStatus.BAD_REQUEST
    ),

    # citizen_id не уникален в рамках выгрузки
    (
        [
            generate_order(order_id=1),
            generate_order(order_id=1),
        ],
        HTTPStatus.BAD_REQUEST
    ),

    (
        generate_orders(
            orders_num=10000,
            start_order_id=MAX_INTEGER - 10000,
        ),
        HTTPStatus.CREATED
    ),

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
