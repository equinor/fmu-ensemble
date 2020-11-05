"""Common utility functions used in fmu.ensemble"""


import os
from collections.abc import MutableMapping


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
