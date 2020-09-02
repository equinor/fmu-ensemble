"""Common utility functions used in fmu.ensemble"""


import os
import dateutil
import datetime
import calendar
import pandas as pd

from .etc import Interaction

fmux = Interaction()
logger = fmux.basiclogger(__name__)


def compute_volumetric_rates(realization, column_keys, time_index, time_unit):
    """Compute volumetric rates from cumulative summary vectors

    Column names that are not referring to cumulative summary
    vectors are silently ignored.

    A Dataframe is returned with volumetric rates, that is rate
    values that can be summed up to the cumulative version. The
    'T' in the column name is switched with 'R'. If you ask for
    FOPT, you will get FOPR in the returned dataframe.

    Rates in the returned dataframe are valid **forwards** in time,
    opposed to rates coming directly from the Eclipse simulator which
    are valid backwards in time.

    If time_unit is set, the rates will be scaled to represent
    either daily, monthly or yearly rates. These will sum up to the
    cumulative as long as you multiply with the correct number
    of days, months or year between each consecutive date index.
    Month lengths and leap years are correctly handled.

    The returned dataframe is indexed by DATE.

    Args:
        realization (ScratchRealization or VirtualRealization): The realization
            object containing rates to compute from.
        column_keys: str or list of strings, cumulative summary vectors
        time_index: str or list of datetimes
        time_unit: str or None. If None, the rates returned will
            be the difference in cumulative between each included
            time step (where the time interval can vary arbitrarily)
            If set to 'days', 'months' or 'years', the rates will
            be scaled to represent a daily, monthly or yearly rate that
            is compatible with the date index and the cumulative data.

    Returns:
        A dataframe indexed by DATE with cumulative columns.
    """
    if isinstance(time_unit, str):
        if time_unit not in ["days", "months", "years"]:
            raise ValueError(
                "Unsupported time_unit " + time_unit + " for volumetric rates"
            )

    column_keys = realization._glob_smry_keys(column_keys)

    # Be strict and only include certain summary vectors that look
    # cumulative by their name:
    column_keys = [x for x in column_keys if cumcolumn_to_ratecolumn(x)]
    if not column_keys:
        logger.error("No valid cumulative columns given to volumetric computation")
        return pd.DataFrame()

    cum_df = realization.get_smry(column_keys=column_keys, time_index=time_index)
    # get_smry() for realizations return a dataframe indexed by 'DATE'

    # Compute row-wise difference, shift back one row
    # to get the NaN to the end, and then drop the NaN.
    # The "rate" given for a specific date is then
    # valid from that date until the next date.
    diff_cum = cum_df.diff().shift(-1).fillna(value=0)

    if time_unit:
        # Calculate the relative timedelta between consecutive
        # DateIndices. relativedeltas are correct in terms
        # of number of years and number of months, but it will
        # only give us integer months, and then leftover days.
        rel_deltas = [
            dateutil.relativedelta.relativedelta(t[1], t[0])
            for t in zip(diff_cum.index, diff_cum.index[1:])
        ]
        whole_days = [
            (t[1] - t[0]).days for t in zip(diff_cum.index, diff_cum.index[1:])
        ]
        # Need to know which years are leap years for our index:
        dayspryear = [
            365 if not calendar.isleap(x.year) else 366
            for x in pd.to_datetime(diff_cum.index[1:])
        ]
        # Float-contribution to years from days:
        days = [
            t[0] / float(t[1]) for t in zip([r.days for r in rel_deltas], dayspryear)
        ]
        floatyearsnodays = [r.years + r.months / 12.0 for r in rel_deltas]
        floatyears = [x + y for x, y in zip(floatyearsnodays, days)]

        # Calculate month-difference:
        floatmonthsnodays = [r.years * 12.0 + r.months for r in rel_deltas]
        # How many days pr. month? We check this for the right
        # end of the relevant time interval.
        daysprmonth = [
            calendar.monthrange(t.year, t.month)[1] for t in diff_cum.index[1:]
        ]
        days = [
            t[0] / float(t[1]) for t in zip([r.days for r in rel_deltas], daysprmonth)
        ]
        floatmonths = [x + y for x, y in zip(floatmonthsnodays, days)]

        diff_cum["DAYS"] = whole_days + [0]
        diff_cum["MONTHS"] = floatmonths + [0]
        diff_cum["YEARS"] = floatyears + [0]
        for vec in column_keys:
            diff_cum[vec] = diff_cum[vec] / diff_cum[time_unit.upper()]
        # Drop temporary columns
        diff_cum.drop(["DAYS", "MONTHS", "YEARS"], inplace=True, axis=1)
        # Set NaN at the final row to zero
        diff_cum.fillna(value=0, inplace=True)

    # Translate the column vectors, 'FOPT' -> 'FOPR' etc.
    rate_names = []
    for vec in diff_cum.columns:
        ratename = cumcolumn_to_ratecolumn(vec)
        if ratename:
            rate_names.append(ratename)
    diff_cum.columns = rate_names
    diff_cum.index.name = "DATE"
    return diff_cum


def cumcolumn_to_ratecolumn(smrycolumn):
    """Converts a cumulative summary column name to the
    corresponding rate column name.

    Returns None if the input summary column name
    is not assumed to be cumulative.

    Example: "FOPT" will be mapped to "FOPR"

    Args:
        smrycolumn (str): Name of summary vector/column

    Returns:
        str: rate column or None
    """
    # Split by colon into components:
    comps = smrycolumn.split(":")
    if len(comps) > 2:
        # Do not support more than one colon.
        return None
    if "CT" in comps[0]:
        # No watercuts.
        return None
    if "T" not in comps[0]:
        return None
    comps[0] = comps[0].replace("T", "R")
    if len(comps) > 1:
        return comps[0] + ":" + comps[1]
    return comps[0]


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


def flatten(dictionary, parent_key="", sep="_"):
    """Flatten nested dictionaries by introducing new keys
    with the accumulated path.

    e.g. {"foo": {"bar": "com"}} becomes {"foo-bar": "com"}

    Args:
        dictionary (dict): Possibly nested dictionary
        parent_key (str): If provided, append as top level key
        sep (str): Separator used in merged keys.

    Returns:
        dict with only one level.
    """
    # pylint: disable=import-outside-toplevel
    try:
        # Python3.3
        from collections.abc import MutableMapping
    except ImportError:
        # Python 2.7:
        from collections import MutableMapping
    items = []
    for key, value in dictionary.items():
        new_key = parent_key + sep + key if parent_key else key
        if isinstance(value, MutableMapping):
            items.extend(flatten(value, new_key, sep=sep).items())
        else:
            items.append((new_key, value))
    return dict(items)


def parse_number(value):
    """Try to parse the string first as an integer, then as float,
    if both fails, return the original string.

    Caveats: Know your Python numbers:
    https://stackoverflow.com/questions/379906/how-do-i-parse-a-string-to-a-float-or-int-in-python

    Beware, this is a minefield.

    Args:
        value (str)

    Returns:
        int, float or string
    """
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        # int(afloat) fails on some, e.g. NaN
        try:
            if int(value) == value:
                return int(value)
            return value
        except ValueError:
            return value  # return float
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value


def shortcut2path(keys, shortpath):
    """
    Convert short pathnames to fully qualified pathnames
    within the datastore.

    If the fully qualified localpath is

        'share/results/volumes/simulator_volume_fipnum.csv'

    then you can also access this with these alternatives:

     * simulator_volume_fipnum
     * simulator_volume_fipnum.csv
     * share/results/volumes/simulator_volume_fipnum

    but only as long as there is no ambiguity. In case
    of ambiguity, the shortpath will be returned.

    Args:
        keys (list of str): List if all keys in the internal datastore
        shortpath (str): The search string, the short pathname that
            should resolve to a fully qualified localpath

    Returns:
        Fully qualified path if found in keys, returns the shortpath
        input untouched if nothing is found, or of the shortpath is
        already fully qualified.
    """
    basenames = list(map(os.path.basename, keys))
    if basenames.count(shortpath) == 1:
        short2path = {os.path.basename(x): x for x in keys}
        return short2path[shortpath]
    noexts = ["".join(x.split(".")[:-1]) for x in keys]
    if noexts.count(shortpath) == 1:
        short2path = {"".join(x.split(".")[:-1]): x for x in keys}
        return short2path[shortpath]
    basenamenoexts = ["".join(os.path.basename(x).split(".")[:-1]) for x in keys]
    if basenamenoexts.count(shortpath) == 1:
        short2path = {"".join(os.path.basename(x).split(".")[:-1]): x for x in keys}
        return short2path[shortpath]
    # If we get here, we did not find anything that
    # this shorthand could point to. Return as is, and let the
    # calling function handle further errors.
    return shortpath
