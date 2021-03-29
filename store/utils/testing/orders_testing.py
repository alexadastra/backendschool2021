from enum import EnumMeta
from http import HTTPStatus
from random import choice, randint, randrange, shuffle
from typing import Any, Dict, Iterable, List, Mapping, Optional, Union

import faker
from aiohttp.test_utils import TestClient
from aiohttp.typedefs import StrOrURL
from aiohttp.web_urldispatcher import DynamicResource

from store.api.handlers import (
    OrdersImportsView, OrdersView, OrdersAssignmentView, OrdersCompletionView
)
from store.api.schema import (
    OrdersIdsSchema, OrdersGetResponseSchema, OrdersAssignPostResponseSchema, OrdersCompletePostResponseSchema
)
from store.utils.pg import MAX_INTEGER

fake = faker.Faker('ru_RU')


def url_for(path: str, **kwargs) -> str:
    """
    Генерирует URL для динамического aiohttp маршрута с параметрами.
    """
    kwargs = {
        key: str(value)  # Все значения должны быть str (для DynamicResource)
        for key, value in kwargs.items()
    }
    return str(DynamicResource(path).url_for(**kwargs))


def generate_order(
        order_id: Optional[int] = None,
        weight: Optional[str] = None,
        region: Optional[int] = None,
        delivery_hours: Optional[List[str]] = None
) -> Dict[str, Any]:
    if order_id is None:
        order_id = randint(0, MAX_INTEGER)

    if weight is None:
        weight = randint(1, 5000) / 100

    if region is None:
        region = randint(0, 100)

    if delivery_hours is None:
        delivery_hours = []

    return {
        'order_id': order_id,
        'weight': weight,
        'region': region,
        'delivery_hours': delivery_hours
    }


def generate_orders(
        orders_num: int,
        unique_regions: int = 10,
        unique_delivery_hours: int = 10,
        start_order_id: int = 0,
        **order_kwargs
) -> List[Dict[str, Any]]:
    # regions = [randrange(1, unique_regions) for _ in range(unique_regions)]

    max_order_id = start_order_id + orders_num - 1
    orders = {}
    for order_id in range(start_order_id, max_order_id + 1):
        # citizen_kwargs['town'] = orders_kwargs.get('town', choice(towns))
        orders[order_id] = generate_order(order_id=order_id, **order_kwargs)

    return list(orders.values())


def normalize_orders(orders):
    return {**orders, 'delivery_hours': sorted(orders['delivery_hours'])}


def compare_orders(left: Mapping, right: Mapping) -> bool:
    return normalize_orders(left) == normalize_orders(right)


def compare_orders_groups(left: Iterable, right: Iterable) -> bool:
    left = [normalize_orders(order) for order in left]
    left.sort(key=lambda order: order['order_id'])

    right = [normalize_orders(order) for order in right]
    right.sort(key=lambda order: order['order_id'])
    return left == right


async def import_orders(
        client: TestClient,
        orders: List[Mapping[str, Any]],
        expected_status: Union[int, EnumMeta] = HTTPStatus.CREATED,
        **request_kwargs
) -> Optional[List[dict]]:
    response = await client.post(
        OrdersImportsView.URL_PATH, json={'data': orders}, **request_kwargs
    )
    assert response.status == expected_status

    if response.status == HTTPStatus.CREATED:
        data = await response.json()
        errors = OrdersIdsSchema().validate(data)
        assert errors == {}
        return data


async def get_order(
        client: TestClient,
        order_id: int,
        expected_status: Union[int, EnumMeta] = HTTPStatus.OK,
        **request_kwargs
) -> dict:
    response = await client.get(
        url_for(OrdersView.URL_PATH, order_id=order_id),
        **request_kwargs
    )
    assert response.status == expected_status

    if response.status == HTTPStatus.OK:
        data = await response.json()
        errors = OrdersGetResponseSchema().validate(data)
        assert errors == {}
        return data


async def assign_orders(
        client: TestClient,
        courier_id: int,
        expected_status: Union[int, EnumMeta] = HTTPStatus.OK,
        **request_kwargs
) -> dict:
    response = await client.post(
        OrdersAssignmentView.URL_PATH, json={'courier_id': courier_id}, **request_kwargs,
    )
    assert response.status == expected_status

    if response.status == HTTPStatus.OK:
        data = await response.json()
        errors = OrdersAssignPostResponseSchema().validate(data)
        assert errors == {}
        return data


async def complete_orders(
        client: TestClient,
        courier_id: int,
        order_id: int,
        time: str,
        expected_status: Union[int, EnumMeta] = HTTPStatus.OK,
        **request_kwargs
) -> dict:
    response = await client.post(
        OrdersCompletionView.URL_PATH, json={'order_id': order_id,
                                             'courier_id': courier_id,
                                             'complete_time': time}, **request_kwargs,
    )
    assert response.status == expected_status

    if response.status == HTTPStatus.OK:
        data = await response.json()
        errors = OrdersCompletePostResponseSchema().validate(data)
        assert errors == {}
        return data
