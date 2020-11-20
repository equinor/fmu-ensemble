"""Test general utility functions in use by fmu.ensemble"""
import datetime
import logging

import numpy as np

import pytest

from fmu.ensemble.util import flatten, parse_number, shortcut2path
from fmu.ensemble.util.dates import normalize_dates
from fmu.ensemble.util.rates import cumcolumn_to_ratecolumn

logger = logging.getLogger(__name__)


def test_cumcolumn_to_ratecolumn():
    """Test computation of volumetric rates from cumulative vectors"""
    assert not cumcolumn_to_ratecolumn("FOPR")
    assert cumcolumn_to_ratecolumn("FOPT") == "FOPR"
    assert not cumcolumn_to_ratecolumn("FWCT")
    assert not cumcolumn_to_ratecolumn("WOPR:A-H")
    assert not cumcolumn_to_ratecolumn("FOPT:FOPT:FOPT")
    assert cumcolumn_to_ratecolumn("WOPT:A-1H") == "WOPR:A-1H"
    assert cumcolumn_to_ratecolumn("WOPTH:A-2H") == "WOPRH:A-2H"


def test_datenormalization():
    """Test normalization of dates, where
    dates can be ensured to be on dategrid boundaries"""

    date = datetime.date

    start = date(1997, 11, 5)
    end = date(2020, 3, 2)

    assert normalize_dates(start, end, "monthly") == (
        date(1997, 11, 1),
        date(2020, 4, 1),
    )
    assert normalize_dates(start, end, "yearly") == (date(1997, 1, 1), date(2021, 1, 1))

    # Check it does not touch already aligned dates
    assert normalize_dates(date(1997, 11, 1), date(2020, 4, 1), "monthly") == (
        date(1997, 11, 1),
        date(2020, 4, 1),
    )
    assert normalize_dates(date(1997, 1, 1), date(2021, 1, 1), "yearly") == (
        date(1997, 1, 1),
        date(2021, 1, 1),
    )


def test_flatten():
    """Test that a dictionary can be flattened with a key-separator"""
    assert flatten({}) == {}
    assert flatten({"foo": "bar"}) == {"foo": "bar"}
    assert flatten({"foo": {"bar": "com"}}) == {"foo_bar": "com"}
    assert flatten({"foo": {"bar": "com"}}, sep="-") == {"foo-bar": "com"}
    assert flatten({"foo": {"bar": "com"}}, parent_key="bart") == {
        "bart_foo_bar": "com"
    }
    assert flatten({"foo": {"bar": "com"}}, parent_key="bart", sep="-") == {
        "bart-foo-bar": "com"
    }


def test_parse_number():
    """Test number parsing, from strings to ints/floats"""
    assert parse_number("1") == 1
    assert parse_number("1e10") == 1e10
    assert parse_number("1.2") == 1.2
    assert parse_number("foobar") == "foobar"

    assert isinstance(parse_number("2.00"), float)
    assert isinstance(parse_number("2"), int)
    assert isinstance(parse_number("1e10"), float)
    with pytest.raises(TypeError):
        parse_number([])

    assert isinstance(parse_number(np.nan), float)


def test_shortcut2path():
    """Test the shortcut-functionality used for looking up
    internalized data in realizations or ensemble objects"""
    assert shortcut2path(["foo/bar/com"], "com") == "foo/bar/com"
    assert shortcut2path([], "foo") == "foo"
    assert shortcut2path(["bar"], "foo") == "foo"
    assert shortcut2path(["foo1/bar/ambig", "foo2/bar/ambig"], "ambig") == "ambig"
