"""Module for handling linear combinations of realizations. """

import fnmatch
import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class RealizationCombination(object):
    """The class is used to perform linear operations on realizations.

    When instantiated, the linear combination will not actually be
    computed before the results are actually asked for - lazy
    evaluation.
    """

    def __init__(self, ref, scale=None, add=None, sub=None):
        """Set up an object for a linear combination of realizations.

        Each instance of this object can only hold one operation,
        either addition/substraction of two
        ensembles/ensemblecombinations or a scaling of one.

        ScratchRealization and VirtualRealization can be combined freely.

        A long expression of ensembles will lead to an evaluation tree
        consisting of instances of this class with actual realizations
        at the leaf nodes.

        See https://en.wikipedia.org/wiki/Binary_expression_tree

        Args:
            scale: float for scaling the realization(combination)
            add: something to add
            sub: something to substract

        """

        self.ref = ref
        if scale:
            self.scale = scale
        else:
            self.scale = 1

        if add:
            self.add = add
        else:
            self.add = None

        # Alternatively, substraction could be implemented as a combination
        # of __mult__ and __add__
        if sub:
            self.sub = sub
        else:
            self.sub = None

    def keys(self):
        """Return the intersection of all keys available in reference
        realization(combination) and the other
        """
        combkeys = set()
        combkeys = combkeys.union(self.ref.keys())
        if self.add:
            combkeys = combkeys.intersection(self.add.keys())
        if self.sub:
            combkeys = combkeys.intersection(self.sub.keys())
        return combkeys

    def get_df(self, localpath, merge=None):
        """Obtain given data from the realizationcombination,
        doing the actual computation of realizationdata on the fly.

        Warning: In order to add dataframes together with meaning,
        using pandas.add, the index of the frames must be correctly set,
        and this can be tricky for some datatypes (f.ex. volumetrics table
        where you want to add together volumes for correct zone
        and fault segment).

        If you have the columns "DATE", "ZONE" and/or "REGION", it
        will be regarded as an index column.

        Args:
            localpath (str): refers to the internalized name of the
                data wanted in the realizations.
            merge (list or str): Optional data to be merged in for the data
                The merge will happen before combination. Be careful
                with index guessing and merged data.

        Returns:
            pd.DataFrame, str, float, int or dict. None if datatype is
                a string which we cannot combine.

        Raises:
            KeyError if data is not found. This can also happen
            for the requested data to merge in. TypeError if scalar values
            are strings and they are multiplied with scalar.
        """
        # We can pandas.add when the index is set correct.
        # WE MUST GUESS!
        indexlist = []
        indexcandidates = ["DATE", "ZONE", "REGION"]
        refdf = self.ref.get_df(localpath, merge=merge)
        if isinstance(refdf, pd.DataFrame):
            for index in indexcandidates:
                if index in refdf.columns:
                    indexlist.append(index)
            refdf = refdf.set_index(indexlist)
            refdf = refdf.select_dtypes(include="number")
        elif isinstance(refdf, dict):
            # Convert from dicts to Series, for linear algebra to be defined
            refdf = pd.Series(refdf)
        if isinstance(refdf, (int, float, np.number)):
            result = self.scale * refdf
        elif isinstance(refdf, str):
            logger.warning("String data %s ignored", localpath)
            return None
        else:
            # Pandas dataframe or series:
            result = refdf.mul(self.scale)
        if self.add:
            otherdf = self.add.get_df(localpath, merge=merge)
            if isinstance(otherdf, pd.DataFrame):
                otherdf = otherdf.set_index(indexlist)
                otherdf = otherdf.select_dtypes(include="number")
            elif isinstance(otherdf, dict):
                otherdf = pd.Series(otherdf)
            if isinstance(otherdf, (int, float, np.number)):
                result = result + otherdf
            else:
                result = result.add(otherdf)
        if self.sub:
            otherdf = self.sub.get_df(localpath, merge=merge)
            if isinstance(otherdf, pd.DataFrame):
                otherdf = otherdf.set_index(indexlist)
                otherdf = otherdf.select_dtypes(include="number")
            elif isinstance(otherdf, dict):
                otherdf = pd.Series(otherdf)
            if isinstance(otherdf, (int, float, np.number)):
                result = result - otherdf
            else:
                result = result.sub(otherdf)
        if isinstance(result, pd.DataFrame):
            # Delete rows where everything is NaN, which will be case when
            # some data row does not exist in all realizations.
            result.dropna(axis="index", how="all", inplace=True)
            # Also delete columns where everything is NaN, happens when
            # column data are not similar
            result.dropna(axis="columns", how="all", inplace=True)
            return result.reset_index()
        if isinstance(result, pd.Series):
            return result.dropna().to_dict()
        return result

    def to_virtual(self, keyfilter=None):
        """Evaluate the current linear combination and return as
        a VirtualRealization.

        Args:
            keyfilter (list or str): If supplied, only keys matching wildcards
                in this argument will be included. Use this for speed reasons
                when only some data is needed. Default is to include everything.
                If you supply "unsmry", it will match every key that
                includes this string by prepending and appending '*' to your pattern

        Returns:
            VirtualRealization
        """
        # pylint: disable=import-outside-toplevel
        from .virtualrealization import VirtualRealization

        if keyfilter is None:
            keyfilter = "*"
        if isinstance(keyfilter, str):
            keyfilter = [keyfilter]
        if not isinstance(keyfilter, list):
            raise TypeError("keyfilter in to_virtual() must be list or string")

        vreal = VirtualRealization(description=str(self))
        for key in self.keys():
            if sum(
                [fnmatch.fnmatch(key, "*" + pattern + "*") for pattern in keyfilter]
            ):
                vreal.append(key, self.get_df(key))
        return vreal

    def get_smry_dates(
        self, freq="monthly", normalize=True, start_date=None, end_date=None
    ):
        """Create a union of dates available in the
        involved ensembles
        """
        dates = set(self.ref.get_smry_dates(freq, normalize, start_date, end_date))
        if self.add:
            dates = dates.union(
                set(self.add.get_smry_dates(freq, normalize, start_date, end_date))
            )
        if self.sub:
            dates = dates.union(
                set(self.add.get_smry_dates(freq, normalize, start_date, end_date))
            )
        dates = list(dates)
        dates.sort()
        return dates

    def get_smry(self, column_keys=None, time_index=None):
        """
        Loads the Eclipse summary data directly from the underlying
        realization data.

        Args:
            column_keys (str or list): column key wildcards. Default
                is '*', which will match all vectors in the Eclipse
                output.
            time_index (str or list of DateTime): time_index mnemonic or
                a list of explicit datetime at which the summary data
                is requested (interpolated or extrapolated)

        Returns:
            pd.DataFrame: Indexed rows, has at least the column DATE
        """
        if isinstance(time_index, str):
            time_index = self.get_smry_dates(time_index)
        indexlist = ["DATE"]
        refdf = self.ref.get_smry(
            time_index=time_index, column_keys=column_keys
        ).set_index(indexlist)
        result = refdf.mul(self.scale)
        if self.add:
            otherdf = self.add.get_smry(
                time_index=time_index, column_keys=column_keys
            ).set_index(indexlist)
            result = result.add(otherdf)
        if self.sub:
            otherdf = self.sub.get_smry(
                time_index=time_index, column_keys=column_keys
            ).set_index(indexlist)
            result = result.sub(otherdf)
        return result.reset_index()

    def get_smry_meta(self, column_keys=None):
        """
        Provide metadata for summary data vectors.

        A dictionary indexed by summary vector names is returned, and each
        value is another dictionary with potentially the metadata types:
        * unit (string)
        * is_total (bool)
        * is_rate (bool)
        * is_historical (bool)
        * get_num (int) (only provided if not None)
        * keyword (str)
        * wgname (str og None)

        Args:
            column_keys: List or str of column key wildcards
        """
        meta = self.ref.get_smry_meta(column_keys=column_keys)
        if self.add:
            meta.update(self.add.get_smry_meta(column_keys=column_keys))
        if self.sub:
            meta.update(self.sub.get_smry_meta(column_keys=column_keys))
        return meta

    @property
    def parameters(self):
        """Access the data obtained from parameters.txt

        Returns:
            dict with data from parameters.txt
        """
        return self.get_df("parameters.txt")

    def __getitem__(self, localpath):
        """Direct access to the realization data structure

        Calls get_df(localpath).
        """
        return self.get_df(localpath)

    def __repr__(self):
        """Try to give out a linear expression"""
        # NB: Implementation in this method requires scaling not to happen
        # simultaneously as adds or subs.
        scalestring = ""
        addstring = ""
        substring = ""
        if self.scale != 1:
            scalestring = str(self.scale) + " * "
        if self.add:
            addstring = " + " + str(self.add)
        if self.sub:
            substring = " - " + str(self.sub)
        return scalestring + str(self.ref) + addstring + substring

    def __sub__(self, other):
        return RealizationCombination(self, sub=other)

    def __add__(self, other):
        return RealizationCombination(self, add=other)

    def __radd__(self, other):
        return RealizationCombination(self, add=other)

    def __rsub__(self, other):
        return RealizationCombination(self, sub=other)

    def __mul__(self, other):
        return RealizationCombination(self, scale=float(other))

    def __rmul__(self, other):
        return RealizationCombination(self, scale=float(other))
