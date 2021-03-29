from http import HTTPStatus
from store.utils.pg import MAX_INTEGER
import pytest

from store.utils.testing.couriers_testing import (
    generate_courier, generate_couriers, get_courier_for_testing,
    import_couriers, compare_couriers
)

LONGEST_STR = 'ё' * 256
CASES = (
    # just courier
    # handler must import it
    (
        [
            generate_courier()
        ],
        HTTPStatus.CREATED
    ),

    # few couriers
    # handler must import them
    (
        [
            generate_courier(courier_id=1, regions=[2, 3]),
            generate_courier(courier_id=2, regions=[], working_hours=["09:00-13:00", "19:00-21:00"]),
            generate_courier(courier_id=3, regions=[1], working_hours=["09:00-18:00"])
        ],
        HTTPStatus.CREATED
    ),

    # no couriers
    # handler must not fall and do nothing
    (
        [],
        HTTPStatus.CREATED
    ),

    # incorrect id
    (
        [
            generate_courier(courier_id=-1),
        ],
        HTTPStatus.BAD_REQUEST
    ),

    # not unique ids
    (
        [
            generate_courier(courier_id=1),
            generate_courier(courier_id=1),
        ],
        HTTPStatus.BAD_REQUEST
    ),

    # invalid field
    (
        [
        {
            "courier_id": 1,
            "courier_type": "foot",
            "regions": [
                1,
                12,
                22
            ],
            "working_hours": [
                "11:35-14:05",
                "09:00-11:00"
            ],
            "dfgbfb": "dsbgfbdr"
        }
        ],
        HTTPStatus.BAD_REQUEST
    ),

    # missing field
    (
        [
            {
            "courier_id": 1,
            "regions": [
                1,
                12,
                22
            ],
            "working_hours": [
                "11:35-14:05",
                "09:00-11:00"
            ]
            }
        ],
        HTTPStatus.BAD_REQUEST
    ),



    # standart input
    (
    [
        {
            "courier_id": 1,
            "courier_type": "foot",
            "regions": [
                1,
                12,
                22
            ],
            "working_hours": [
                "11:35-14:05",
                "09:00-11:00"
            ]
        },
        {
            "courier_id": 2,
            "courier_type": "bike",
            "regions": [
                22
            ],
            "working_hours": [
                "09:00-18:00"
            ]
        },
        {
            "courier_id": 3,
            "courier_type": "car",
            "regions": [
                12,
                22,
                23,
                33
            ],
            "working_hours": []
        }
    ],
        HTTPStatus.CREATED
    )

)


@pytest.mark.parametrize('couriers, expected_status', CASES)
async def test_couriers_import(api_client, couriers, expected_status):
    data = await import_couriers(api_client, couriers, expected_status)

    # check that data imported successfully
    if expected_status == HTTPStatus.CREATED:
        for courier in couriers:
            imported_courier = await get_courier_for_testing(api_client, courier['courier_id'])
            assert compare_couriers(courier, imported_courier)

"""
# the largest input possible
    (
        generate_couriers(
            couriers_num=10000,
            start_courier_id=MAX_INTEGER - 10000,
            # regions=[MAX_INTEGER for i in range(10000)],
            # working_hours=["09:00-18:00" for i in range(10000)]
        ),
        HTTPStatus.CREATED
    ),
"""