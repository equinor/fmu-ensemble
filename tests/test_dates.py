from datetime import datetime as dt

import pytest
from fmu.ensemble.util.dates import _fallback_date_roll, date_range

# These tests are duplicated from https://github.com/equinor/res2df/blob/master/tests/test_summary.py


@pytest.mark.parametrize(
    "rollme, direction, freq, expected",
    [
        (
            dt(3000, 1, 1),
            "forward",
            "yearly",
            dt(3000, 1, 1),
        ),
        (
            dt(3000, 1, 1),
            "forward",
            "monthly",
            dt(3000, 1, 1),
        ),
        (
            dt(3000, 1, 2),
            "forward",
            "yearly",
            dt(3001, 1, 1),
        ),
        (
            dt(3000, 1, 2),
            "forward",
            "monthly",
            dt(3000, 2, 1),
        ),
        (
            dt(3000, 1, 1),
            "back",
            "yearly",
            dt(3000, 1, 1),
        ),
        (
            dt(3000, 1, 1),
            "back",
            "monthly",
            dt(3000, 1, 1),
        ),
        (
            dt(3000, 12, 31),
            "back",
            "yearly",
            dt(3000, 1, 1),
        ),
        (
            dt(3000, 2, 2),
            "back",
            "monthly",
            dt(3000, 2, 1),
        ),
        pytest.param(
            dt(3000, 2, 2),
            "forward",
            "daily",
            None,
            marks=pytest.mark.xfail(raises=ValueError),
        ),
        pytest.param(
            dt(3000, 2, 2),
            "upwards",
            "yearly",
            None,
            marks=pytest.mark.xfail(raises=ValueError),
        ),
    ],
)
def test_fallback_date_roll(rollme, direction, freq, expected):
    """The pandas date rolling does not always work for years beyound 2262. The
    code should fallback automatically to hide that Pandas limitation"""
    assert _fallback_date_roll(rollme, direction, freq) == expected


@pytest.mark.parametrize(
    "start, end, freq, expected",
    [
        (
            dt(3000, 1, 1),
            dt(3002, 1, 1),
            "yearly",
            [
                dt(3000, 1, 1),
                dt(3001, 1, 1),
                dt(3002, 1, 1),
            ],
        ),
        (
            dt(2999, 11, 1),
            dt(3000, 2, 1),
            "monthly",
            [
                dt(2999, 11, 1),
                dt(2999, 12, 1),
                dt(3000, 1, 1),
                dt(3000, 2, 1),
            ],
        ),
        pytest.param(
            dt(3000, 1, 1),
            dt(3000, 2, 1),
            "weekly",
            None,
            marks=pytest.mark.xfail(raises=ValueError),
        ),
        (
            # Crossing the problematic time boundary:
            dt(2260, 1, 1),
            dt(2263, 1, 1),
            "yearly",
            [
                dt(2260, 1, 1),
                dt(2261, 1, 1),
                dt(2262, 1, 1),
                dt(2263, 1, 1),
            ],
        ),
        (
            dt(3000, 1, 1),
            dt(3000, 1, 1),
            "yearly",
            [
                dt(3000, 1, 1),
            ],
        ),
        (
            dt(2000, 1, 1),
            dt(2000, 1, 1),
            "yearly",
            [
                dt(2000, 1, 1),
            ],
        ),
        (
            dt(2000, 1, 1),
            dt(1000, 1, 1),
            "yearly",
            [],
        ),
        (
            dt(3000, 1, 1),
            dt(2000, 1, 1),
            "yearly",
            [],
        ),
        (
            dt(2300, 5, 6),
            dt(2302, 3, 1),
            "yearly",
            [
                dt(2300, 5, 6),
                dt(2301, 1, 1),
                dt(2302, 1, 1),
                dt(2302, 3, 1),
            ],
        ),
        (
            dt(2304, 5, 6),
            dt(2302, 3, 1),
            "yearly",
            [],
        ),
        (
            dt(2302, 3, 1),
            dt(2302, 3, 1),
            "yearly",
            [dt(2302, 3, 1)],
        ),
    ],
)
def test_date_range(start, end, freq, expected):
    """When dates are beyond year 2262,
    the function _fallback_date_range() is triggered."""
    assert date_range(start, end, freq) == expected
