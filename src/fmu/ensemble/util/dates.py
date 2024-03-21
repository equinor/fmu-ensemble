"""Common utility functions used in fmu.ensemble"""

import datetime
import logging
from typing import List, Tuple

import dateutil
import pandas as pd

logger = logging.getLogger(__name__)

"""Mapping from fmu-ensemble custom offset strings to Pandas DateOffset strings.
See
https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#dateoffset-objects
"""
PD_FREQ_MNEMONICS = {
    "monthly": "MS",
    "yearly": "YS",
    "daily": "D",
    "weekly": "W-MON",
}


def date_range(start_date, end_date, freq):
    """Wrapper for pandas.date_range to allow for extra fmu-ensemble specific mnemonics
    'yearly', 'daily', 'weekly', mapped over to pandas DateOffsets

    Args:
        start_date (datetime.date)
        end_date (datetime.date)
        freq (str): monthly, daily, weekly, yearly, or a Pandas date offset
            frequency.

    Returns:
        list of datetimes
    """
    try:
        return pd.date_range(
            start_date, end_date, freq=PD_FREQ_MNEMONICS.get(freq, freq)
        )
    except pd.errors.OutOfBoundsDatetime:
        return _fallback_date_range(start_date, end_date, freq)


def unionize_smry_dates(eclsumsdates, freq, normalize, start_date=None, end_date=None):
    """
    Unionize lists of dates into one datelist encompassing the date
    range from all datelists, with cropping, resampling to wanted
    frequency, and potential normalized dates to the frequency (rollback/rollforward)

    Args:
        eclsumsdates (list of lists of datetimes)
        freq (str): Requested frequency
        normalize (bool): Normalize daterange to frequency or not.
        start_date (datetime.date or str): Overridden if freq=='first'
        end_date (datetime.date or str): Overridden if freq=='last'

    Return:
        list of datetime.date
    """
    if not eclsumsdates:
        return []

    if start_date and isinstance(start_date, str):
        start_date = dateutil.parser.isoparse(start_date).date()
    if start_date and not isinstance(start_date, datetime.date):
        raise TypeError("start_date had unknown type")

    if end_date and isinstance(end_date, str):
        end_date = dateutil.parser.isoparse(end_date).date()
    if end_date and not isinstance(end_date, datetime.date):
        raise TypeError("end_date had unknown type")

    if freq in ("report", "raw"):
        datetimes = set()
        for eclsumdatelist in eclsumsdates:
            datetimes = datetimes.union(eclsumdatelist)
        datetimes = list(datetimes)
        datetimes.sort()
        if start_date:
            # Convert to datetime (at 00:00:00)
            start_date = datetime.datetime.combine(
                start_date, datetime.datetime.min.time()
            )
            datetimes = [x for x in datetimes if x > start_date]
            datetimes = [start_date] + datetimes
        if end_date:
            end_date = datetime.datetime.combine(end_date, datetime.datetime.min.time())
            datetimes = [x for x in datetimes if x < end_date]
            datetimes = datetimes + [end_date]
        return datetimes
    if freq == "last":
        end_date = max([max(x) for x in eclsumsdates]).date()
        return [end_date]
    if freq == "first":
        start_date = min([min(x) for x in eclsumsdates]).date()
        return [start_date]
    # These are datetime.datetime, not datetime.date
    start_smry = min([min(x) for x in eclsumsdates])
    end_smry = max([max(x) for x in eclsumsdates])

    (start_n, end_n) = normalize_dates(start_smry.date(), end_smry.date(), freq)

    if not start_date and not normalize:
        start_date_range = start_smry.date()
    elif not start_date and normalize:
        start_date_range = start_n
    else:
        start_date_range = start_date

    if not end_date and not normalize:
        end_date_range = end_smry.date()
    elif not end_date and normalize:
        end_date_range = end_n
    else:
        end_date_range = end_date

    datetimes = date_range(start_date_range, end_date_range, freq)

    # Convert from Pandas' datetime64 to datetime.date:
    datetimes = [x.date() for x in datetimes]

    # pd.date_range will not include random dates that do not
    # fit on frequency boundary. Force include these if
    # supplied as user arguments.
    if start_date and start_date not in datetimes:
        datetimes = [start_date] + datetimes
    if end_date and end_date not in datetimes:
        datetimes = datetimes + [end_date]
    return datetimes


def normalize_dates(
    start_date: datetime.date, end_date: datetime.date, freq: str
) -> Tuple[datetime.date, datetime.date]:
    """
    Normalize start and end date according to frequency
    by extending the time range.

    So for [1997-11-5, 2020-03-02] and monthly freqency
    this will transform your dates to
    [1997-11-1, 2020-04-01]

    For yearly frequency will be [1997-01-01, 2021-01-01].

    Args:
        start_date: datetime.date
        end_date: datetime.date
        freq: string with either 'monthly', 'yearly', 'weekly'
            or any other frequency offset accepted by Pandas

    Return:
        Tuple of normalized (start_date, end_date)
    """
    offset = pd.tseries.frequencies.to_offset(PD_FREQ_MNEMONICS.get(freq, freq))
    try:
        start_normalized = offset.rollback(start_date).date()
    except pd.errors.OutOfBoundsDatetime:
        # Pandas only supports datetime up to year 2262
        start_normalized = _fallback_date_roll(
            datetime.datetime.combine(start_date, datetime.time()), "back", freq
        ).date()
    try:
        end_normalized = offset.rollforward(end_date).date()
    except pd.errors.OutOfBoundsDatetime:
        # Pandas only supports datetime up to year 2262
        end_normalized = _fallback_date_roll(
            datetime.datetime.combine(end_date, datetime.time()), "forward", freq
        ).date()

    return (start_normalized, end_normalized)


def _fallback_date_roll(
    rollme: datetime.datetime, direction: str, freq: str
) -> datetime.datetime:
    """Fallback function for rolling dates forward or backward onto a
    date frequency boundary.

    This function reimplements pandas' DateOffset.roll_forward() and backward()
    only for monthly and yearly frequency. This is necessary as Pandas does not
    support datetimes beyond year 2262 due to all datetimes in Pandas being
    represented by nanosecond accuracy.

    This function is a fallback only, to keep support for using all Pandas timeoffsets
    in situations where years beyond 2262 is not a issue."""
    if direction not in ["back", "forward"]:
        raise ValueError(f"Unknown direction {direction}")

    if freq == "yearly":
        if direction == "forward":
            if rollme <= datetime.datetime(year=rollme.year, month=1, day=1):
                return datetime.datetime(year=rollme.year, month=1, day=1)
            return datetime.datetime(year=rollme.year + 1, month=1, day=1)
        return datetime.datetime(year=rollme.year, month=1, day=1)

    if freq == "monthly":
        if direction == "forward":
            if rollme <= datetime.datetime(year=rollme.year, month=rollme.month, day=1):
                return datetime.datetime(year=rollme.year, month=rollme.month, day=1)
            return datetime.datetime(
                year=rollme.year, month=rollme.month, day=1
            ) + dateutil.relativedelta.relativedelta(  # type: ignore
                months=1
            )
        return datetime.datetime(year=rollme.year, month=rollme.month, day=1)

    raise ValueError(
        "Only yearly or monthly frequencies are "
        "supported for simulations beyond year 2262"
    )


def _fallback_date_range(
    start: datetime.date, end: datetime.date, freq: str
) -> List[datetime.datetime]:
    """Fallback routine for generating date ranges beyond Pandas datetime64[ns]
    year-2262 limit.

    Assumes that the start and end times already fall on a frequency boundary.
    """
    if start == end:
        return [datetime.datetime.combine(start, datetime.datetime.min.time())]
    if end < start:
        return []
    if freq == "yearly":
        dates = [datetime.datetime.combine(start, datetime.datetime.min.time())] + [
            datetime.datetime(year=year, month=1, day=1)
            for year in range(start.year + 1, end.year + 1)
        ]
        if datetime.datetime.combine(end, datetime.datetime.min.time()) != dates[-1]:
            dates = dates + [
                datetime.datetime.combine(end, datetime.datetime.min.time())
            ]
        return dates
    if freq == "monthly":
        dates = []
        date = datetime.datetime.combine(start, datetime.datetime.min.time())
        enddatetime = datetime.datetime.combine(end, datetime.datetime.min.time())
        while date <= enddatetime:
            dates.append(date)
            date = date + dateutil.relativedelta.relativedelta(months=1)  # type: ignore
        return dates
    raise ValueError("Unsupported frequency for datetimes beyond year 2262")
