"""Common utility functions for rates used in fmu.ensemble"""


import calendar
import dateutil
import logging

import pandas as pd

logger = logging.getLogger(__name__)


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

    # pylint: disable=protected-access
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
