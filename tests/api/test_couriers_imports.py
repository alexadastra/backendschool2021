from datetime import date, timedelta
from http import HTTPStatus

import pytest

from store.utils.pg import MAX_INTEGER
from store.utils.testing import (
    compare_courier_groups, generate_courier, generate_couriers, get_courier,
    import_couriers, compare_couriers
)

LONGEST_STR = 'ё' * 256
CASES = (
    # Житель без родственников.
    # Обработчик должен корректно создавать выгрузку с одним жителем.
    (
        [
            generate_courier()
        ],
        HTTPStatus.CREATED
    ),

    # Житель с несколькими родственниками.
    # Обработчик должен корректно добавлять жителей и создавать
    # родственные связи.
    (
        [
            generate_courier(courier_id=1, regions=[2, 3]),
            generate_courier(courier_id=2, regions=[], working_hours=["09:00-13:00", "19:00-21:00"]),
            generate_courier(courier_id=3, regions=[1], working_hours=["09:00-18:00"])
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
            generate_courier(courier_id=-1),
        ],
        HTTPStatus.BAD_REQUEST
    ),

    # citizen_id не уникален в рамках выгрузки
    (
        [
            generate_courier(courier_id=1),
            generate_courier(courier_id=1),
        ],
        HTTPStatus.BAD_REQUEST
    ),

)


@pytest.mark.parametrize('couriers, expected_status', CASES)
async def test_import(api_client, couriers, expected_status):
    data = await import_couriers(api_client, couriers, expected_status)

    # Проверяем, что данные успешно импортированы
    if expected_status == HTTPStatus.CREATED:
        for courier in couriers:
            imported_citizens = await get_courier(api_client, courier['courier_id'])
            assert compare_couriers(courier, imported_citizens)

"""
    (
        generate_couriers(
            couriers_num=10000,
            start_courier_id=MAX_INTEGER - 10000,
            type='foot',
            regions=[MAX_INTEGER for i in range(MAX_INTEGER)],
            working_hours=["09:00-18:00" for i in range(MAX_INTEGER)]
        ),
        HTTPStatus.CREATED
    )
"""