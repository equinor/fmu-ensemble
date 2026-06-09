from datetime import datetime as dt

import pandas as pd
import pytest

from fmu.ensemble.util.dates import _fallback_date_roll, date_range

# These tests are duplicated from https://github.com/equinor/res2df/blob/master/tests/test_summary.py

PANDAS_BELOW_3 = int(pd.__version__.split(".")[0]) < 3


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
            dt(2304, 5, 6),
            dt(2302, 3, 1),
            "yearly",
            [],
        ),
    ],
)
def test_date_range(start, end, freq, expected):
    """Date ranges that behave identically on all supported Pandas versions."""
    assert date_range(start, end, freq) == expected


@pytest.mark.skipif(
    not PANDAS_BELOW_3,
    reason="Only relevant for Pandas < 3 (fallback behavior beyond year 2262)",
)
@pytest.mark.parametrize(
    "start, end, freq, expected",
    [
        pytest.param(
            # Pandas < 3 has no fallback for weekly frequency beyond year 2262
            dt(3000, 1, 1),
            dt(3000, 2, 1),
            "weekly",
            None,
            marks=pytest.mark.xfail(raises=ValueError),
        ),
        (
            # The pure-Python fallback includes the off-boundary endpoints
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
            dt(2302, 3, 1),
            dt(2302, 3, 1),
            "yearly",
            [dt(2302, 3, 1)],
        ),
    ],
)
def test_date_range_pandas_below_3(start, end, freq, expected):
    """Beyond year 2262 Pandas < 3 falls back to _fallback_date_range(), which
    includes off-boundary endpoints and only supports yearly/monthly frequency."""
    assert date_range(start, end, freq) == expected


@pytest.mark.skipif(
    PANDAS_BELOW_3, reason="Pandas >= 3 handles dates beyond year 2262 natively"
)
@pytest.mark.parametrize(
    "start, end, freq, expected",
    [
        (
            # Pandas >= 3 supports weekly frequency beyond year 2262 natively
            dt(3000, 1, 1),
            dt(3000, 2, 1),
            "weekly",
            [
                dt(3000, 1, 6),
                dt(3000, 1, 13),
                dt(3000, 1, 20),
                dt(3000, 1, 27),
            ],
        ),
        (
            # Native pandas.date_range only yields on-boundary dates
            dt(2300, 5, 6),
            dt(2302, 3, 1),
            "yearly",
            [
                dt(2301, 1, 1),
                dt(2302, 1, 1),
            ],
        ),
        (
            dt(2302, 3, 1),
            dt(2302, 3, 1),
            "yearly",
            [],
        ),
    ],
)
def test_date_range_pandas_3(start, end, freq, expected):
    """Pandas >= 3 has no year-2262 limitation and uses native pandas.date_range,
    which does not add off-boundary endpoints."""
    assert date_range(start, end, freq) == expected
