# -*- coding: utf-8 -*-
"""Module for handling linear combinations of realizations.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pandas as pd

from .etc import Interaction
from fmu.ensemble.virtualrealization import VirtualRealization

xfmu = Interaction()
logger = xfmu.functionlogger(__name__)


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

    def get_df(self, localpath):
        """Evaluate the realization combination on a specific dataset

        On realizations, some datatypes can be dictionaries!
        """
        # We can pandas.add when the index is set correct.
        # WE MUST GUESS!
        indexlist = []
        indexcandidates = ["DATE", "ZONE", "REGION"]
        refdf = self.ref.get_df(localpath)
        if isinstance(refdf, pd.DataFrame):
            for index in indexcandidates:
                if index in refdf.columns:
                    indexlist.append(index)
            refdf = refdf.set_index(indexlist)
            refdf = refdf.select_dtypes(include="number")
        else:  # Convert from dict to Series
            refdf = pd.Series(refdf)
        result = refdf.mul(self.scale)
        if self.add:
            otherdf = self.add.get_df(localpath)
            if isinstance(otherdf, pd.DataFrame):
                otherdf = otherdf.set_index(indexlist)
                otherdf = otherdf.select_dtypes(include="number")
            else:
                otherdf = pd.Series(otherdf)
            result = result.add(otherdf)
        if self.sub:
            otherdf = self.sub.get_df(localpath)
            if isinstance(otherdf, pd.DataFrame):
                otherdf = otherdf.set_index(indexlist)
                otherdf = otherdf.select_dtypes(include="number")
            else:
                otherdf = pd.Series(otherdf)
            result = result.sub(otherdf)
        if isinstance(result, pd.DataFrame):
            # Delete rows where everything is NaN, which will be case when
            # some data row does not exist in all realizations.
            result.dropna(axis="index", how="all", inplace=True)
            # Also delete columns where everything is NaN, happens when
            # column data are not similar
            result.dropna(axis="columns", how="all", inplace=True)
            return result.reset_index()
        return result.dropna().to_dict()

    def to_virtual(self):
        """Evaluate the current linear combination and return as
        a virtualrealizatione.
        """
        vreal = VirtualRealization(description=str(self))
        for key in self.keys():
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
        realization data, independent of whether you have issued
        load_smry() first in the realization.

        If you involve VirtualRealization in this operation, this
        this will fail. You have to use internalized data, that is
        get_df().

        Later, resampling of data in VirtualRealization might get implemented.
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

    def __getitem__(self, localpath):
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
