"""Common utility functions used in fmu.ensemble"""


import dateutil
import datetime
import pandas as pd

from ..etc import Interaction

xfmu = Interaction()
logger = xfmu.functionlogger(__name__)


def unionize_smry_dates(eclsumsdates, freq, normalize, start_date=None, end_date=None):
    """
    Unionize lists of dates into one datelist encompassing the date
    range from all datelists, with cropping, resampling to wanted
    frequency, and potential normalized dates to the frequency (rollback/rollforward)

    Args:
        eclsumsdates (list of lists of datetimes)
        freq (str): Requested frequency
        normalize (bool): Normalize daterange to frequency or not.
        start_date (datetime.date or str):
        end_date (datetime.date or str)

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

    pd_freq_mnenomics = {"monthly": "MS", "yearly": "YS", "daily": "D"}

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

    if freq not in pd_freq_mnenomics:
        raise ValueError("Requested frequency %s not supported" % freq)
    datetimes = pd.date_range(
        start_date_range, end_date_range, freq=pd_freq_mnenomics[freq]
    )
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


def normalize_dates(start_date, end_date, freq):
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
        freq: string with either 'monthly' or 'yearly'.
            Anything else will return the input as is
    Return:
        Tuple of normalized (start_date, end_date)
    """

    if freq == "monthly":
        start_date = start_date.replace(day=1)

        # Avoid rolling forward if we are already at day 1 in a month
        if end_date != end_date.replace(day=1):
            end_date = end_date.replace(day=1) + dateutil.relativedelta.relativedelta(
                months=1
            )
    elif freq == "yearly":
        start_date = start_date.replace(day=1, month=1)
        # Avoid rolling forward if we are already at day 1 in a year
        if end_date != end_date.replace(day=1, month=1):
            end_date = end_date.replace(
                day=1, month=1
            ) + dateutil.relativedelta.relativedelta(years=1)
    elif freq == "daily":
        # This we don't need to normalize, but we should not give any warnings
        pass
    elif freq == "first" or freq == "last":
        # This we don't need to normalize, but we should not give any warnings
        pass
    else:
        logger.warning("Unrecognized frequency %s for date normalization", str(freq))
    return (start_date, end_date)
