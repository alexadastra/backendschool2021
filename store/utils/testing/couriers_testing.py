from enum import EnumMeta
from http import HTTPStatus
from random import choice, randint, randrange, shuffle
from typing import Any, Dict, Iterable, List, Mapping, Optional, Union

import faker
from aiohttp.test_utils import TestClient
from aiohttp.typedefs import StrOrURL
from aiohttp.web_urldispatcher import DynamicResource

from store.api.handlers import (
    CouriersImportsView, CouriersView
)
from store.api.schema import (
    CouriersIdsSchema, CourierGetResponseSchema, CourierItemSchema,
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


def generate_courier(
        courier_id: Optional[int] = None,
        courier_type: Optional[str] = None,
        regions: Optional[List[int]] = None,
        working_hours: Optional[List[str]] = None
) -> Dict[str, Any]:
    if courier_id is None:
        courier_id = randint(0, MAX_INTEGER)

    if courier_type is None:
        courier_type = choice(('foot', 'bike', 'car'))

    if regions is None:
        regions = []

    if working_hours is None:
        working_hours = []

    return {
        'courier_id': courier_id,
        'courier_type': courier_type,
        'regions': regions,
        'working_hours': working_hours
    }


def generate_couriers(
        couriers_num: int,
        unique_regions: int = 10,
        unique_working_hours: int = 10,
        start_courier_id: int = 0,
        **courier_kwargs
) -> List[Dict[str, Any]]:
    # regions = [randrange(1, unique_regions) for _ in range(unique_regions)]

    max_courier_id = start_courier_id + couriers_num - 1
    couriers = {}
    for courier_id in range(start_courier_id, max_courier_id + 1):
        # citizen_kwargs['town'] = courier_kwargs.get('town', choice(towns))
        couriers[courier_id] = generate_courier(courier_id=courier_id, **courier_kwargs)

    return list(couriers.values())


def normalize_courier(courier):
    return {**courier, 'regions': sorted(courier['regions']), 'working_hours': sorted(courier['working_hours'])}


def compare_couriers(left: Mapping, right: Mapping) -> bool:
    return normalize_courier(left) == normalize_courier(right)


def compare_courier_groups(left: Iterable, right: Iterable) -> bool:
    left = [normalize_courier(courier) for courier in left]
    left.sort(key=lambda courier: courier['courier_id'])

    right = [normalize_courier(courier) for courier in right]
    right.sort(key=lambda courier: courier['courier_id'])
    return left == right


async def import_couriers(
        client: TestClient,
        couriers: List[Mapping[str, Any]],
        expected_status: Union[int, EnumMeta] = HTTPStatus.CREATED,
        **request_kwargs
) -> Optional[List[dict]]:
    response = await client.post(
        CouriersImportsView.URL_PATH, json={'data': couriers}, **request_kwargs
    )
    assert response.status == expected_status

    if response.status == HTTPStatus.CREATED:
        data = await response.json()
        errors = CouriersIdsSchema().validate(data)
        assert errors == {}
        return data


async def get_courier(
        client: TestClient,
        courier_id: int,
        expected_status: Union[int, EnumMeta] = HTTPStatus.OK,
        **request_kwargs
) -> dict:
    response = await client.get(
        url_for(CouriersView.URL_PATH, courier_id=courier_id),
        **request_kwargs
    )
    assert response.status == expected_status

    if response.status == HTTPStatus.OK:
        data = await response.json()
        errors = CourierGetResponseSchema().validate(data)
        assert errors == {}
        return data


async def get_courier_for_testing(
        client: TestClient,
        courier_id: int,
        expected_status: Union[int, EnumMeta] = HTTPStatus.OK,
        **request_kwargs
) -> dict:
    courier = await get_courier(client, courier_id, expected_status, **request_kwargs)
    return {'courier_id': courier['courier_id'], 'courier_type': courier['courier_type'],
            'regions': courier['regions'], 'working_hours': courier['working_hours']}


async def patch_courier(
        client: TestClient,
        courier_id: int,
        data: Mapping[str, Any],
        expected_status: Union[int, EnumMeta] = HTTPStatus.OK,
        str_or_url: StrOrURL = CouriersView.URL_PATH,
        **request_kwargs
):
    response = await client.patch(
        url_for(str_or_url, courier_id=courier_id),
        json=data,
        **request_kwargs
    )
    assert response.status == expected_status
    if response.status == HTTPStatus.OK:
        data = await response.json()
        errors = CourierItemSchema().validate(data)
        assert errors == {}
        return data
