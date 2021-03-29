from store.api.domain.couriers_orders_resolver import CouriersOrdersResolver
import pytest

CASES = (
    (
        {10: 3, 11: 4, 21: 5, 13: 8, 41: 9},
        [1, 6, 4, 7, 6],
        13.5,
        [11, 13]
    ),

    (
        {0: 3.1, 1: 4.1, 2: 5.1, 3: 8.1, 4: 9.1},
        [1, 6, 4, 7, 6],
        13.5,
        [1, 3]
    ),

    (
        {0: 3.1, 1: 4.1, 2: 5.1, 3: 8.1, 4: 9.1},
        None,
        13.5,
        [0, 1, 2]
    ),

    (
        {0: 3.1, 1: 4.1, 2: 5.1, 3: 8.1, 4: 9.1},
        [1, 1, 1, 1, 1],
        13.5,
        [0, 1, 2]
    ),

    (
        {0: 20},
        [1],
        19,
        []
    ),

    (
        {0: 20},
        [1],
        20,
        [0]
    ),

    (
        {},
        [],
        0,
        []
    )
)


@pytest.mark.parametrize('w, p, max_w, expected_w', CASES)
async def test_knapsack_resolver(w, p, max_w, expected_w):
    actual_w = await CouriersOrdersResolver(orders_=w, max_weight=max_w, values_=p).resolve_orders()
    actual_w.sort()
    expected_w.sort()
    assert actual_w == expected_w
