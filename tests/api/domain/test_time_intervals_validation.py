from store.api.domain.time_intarvals_converter import TimeIntervalsConverter
from marshmallow import ValidationError
import pytest

CASES = (
    (
        ["09:00-18:00"],
        None
    ),

    (
        ["9:00-18:00"],
        ValidationError
    ),

    (
        ["09:69-18:96"],
        ValidationError
    ),

    (
        ["09:00-18:420"],
        ValidationError
    ),

    (
        ["19:00-08:00"],
        ValidationError
    ),

    (
        ["22:00-00:42"],
        ValidationError
    ),

    (
        ["22:00-00:00"],
        ValidationError
    ),

    (
        ["16:40-16:40"],
        ValidationError
    ),

    (
        ["16:40-16:40", "09:00-18:00"],
        ValidationError
    ),

    (
        ["09:00 - 18:00"],
        ValidationError
    ),

    (
        ["09.00-18.00"],
        ValidationError
    ),
)


@pytest.mark.parametrize('time_intervals, expected_exception', CASES)
def test_time_intervals_validation(time_intervals, expected_exception):
    try:
        TimeIntervalsConverter.validate_hour_intervals(time_intervals, "test")
    except ValidationError:
        assert expected_exception == ValidationError
