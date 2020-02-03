# -*- coding: utf-8 -*-
"""Module for handling linear combinations of ensembles.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pandas as pd

from fmu.ensemble.virtualensemble import VirtualEnsemble
from .etc import Interaction

xfmu = Interaction()
logger = xfmu.functionlogger(__name__)


class EnsembleCombination(object):
    """The class is used to perform linear operations on ensembles.

    When instantiated, the linear combination will not actually be
    computed before the results are actually asked for - lazy
    evaluation.
    """

    def __init__(self, ref, scale=None, add=None, sub=None):
        """Set up an object for a linear combination of ensembles.

        Each instance of this object can only hold one operation,
        either addition/substraction of two
        ensembles/ensemblecombinations or a scaling of one.

        ScratchEnsembles and VirtualEnsembles can be combined freely.

        A long expression of ensembles will lead to an evaluation tree
        consisting of instances of this class with actual ensembles
        at the leaf nodes.

        Args:
            scale: float for scaling the ensemble or ensemblecombination
            add: ensemble or ensemblecombinaton with a positive sign
            sub: ensemble or ensemblecombination with a negative sign.

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
        ensemble(combination) and the other
        """
        combkeys = set()
        combkeys = combkeys.union(self.ref.keys())
        if self.add:
            combkeys = combkeys.intersection(self.add.keys())
        if self.sub:
            combkeys = combkeys.intersection(self.sub.keys())
        return combkeys

    def get_df(self, localpath):
        """Evaluate the ensemble combination on a specific dataset
        """
        # We can pandas.add when the index is set correct.
        # WE MUST GUESS!
        indexlist = []
        indexcandidates = ["REAL", "DATE", "ZONE", "REGION"]
        for index in indexcandidates:
            if index in self.ref.get_df(localpath).columns:
                indexlist.append(index)
        logger.debug("get_df() inferred index columns to %s", str(indexlist))
        refdf = self.ref.get_df(localpath).set_index(indexlist)
        refdf = refdf.select_dtypes(include="number")
        result = refdf.mul(self.scale)
        if self.add:
            otherdf = self.add.get_df(localpath).set_index(indexlist)
            otherdf = otherdf.select_dtypes(include="number")
            result = result.add(otherdf)
        if self.sub:
            otherdf = self.sub.get_df(localpath).set_index(indexlist)
            otherdf = otherdf.select_dtypes(include="number")
            result = result.sub(otherdf)
        # Delete rows where everything is NaN, which will be case when
        # realization (multi-)indices does not match up in both ensembles.
        result.dropna(axis="index", how="all", inplace=True)
        # Also delete columns where everything is NaN, happens when
        # column data are not similar
        result.dropna(axis="columns", how="all", inplace=True)
        return result.reset_index()

    def to_virtual(self):
        """Evaluate the current linear combination and return as
        a virtual ensemble.
        """
        vens = VirtualEnsemble(name=str(self))
        for key in self.keys():
            logger.info("Calculating ensemblecombination on %s", key)
            vens.append(key, self.get_df(key))
        vens.update_realindices()
        return vens

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
                set(self.sub.get_smry_dates(freq, normalize, start_date, end_date))
            )
        dates = list(dates)
        dates.sort()
        return dates

    def get_smry(self, column_keys=None, time_index=None):
        """
        Loads the Eclipse summary data directly from the underlying
        ensemble data, independent of whether you have issued
        load_smry() first in the ensembles.

        If you involve VirtualEnsembles in this operation, this
        this will fail.

        Later resampling of data in VirtualEnsembles might get implemented.
        """
        if isinstance(time_index, str):
            time_index = self.get_smry_dates(time_index)
        indexlist = ["REAL", "DATE"]
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

    def get_smry_stats(self, column_keys=None, time_index="monthly"):
        """
        Function to extract the ensemble statistics (Mean, Min, Max, P10, P90)
        for a set of simulation summary vectors (column key).

        Compared to the agg() function, this function only works on summary
        data (time series), and will only operate on actually requested data,
        independent of what is internalized. It accesses the summary files
        directly and can thus obtain data at any time frequency.

        Args:
            column_keys: list of column key wildcards
            time_index: list of DateTime if interpolation is wanted
               default is None, which returns the raw Eclipse report times
               If a string is supplied, that string is attempted used
               via get_smry_dates() in order to obtain a time index.
        Returns:
            A MultiLevel dataframe. Outer index is 'minimum', 'maximum',
            'mean', 'p10', 'p90', inner index are the dates. Column names
            are the different vectors. Quantiles follow the scientific
            standard, opposite to the oil industry standard.

        TODO: add warning message when failed realizations are removed
        """
        # Obtain an aggregated dataframe for only the needed columns over
        # the entire ensemble.

        dframe = (
            self.get_smry(time_index=time_index, column_keys=column_keys)
            .drop(columns="REAL")
            .groupby("DATE")
        )
        mean = dframe.mean()
        p90 = dframe.quantile(q=0.90)
        p10 = dframe.quantile(q=0.10)
        maximum = dframe.max()
        minimum = dframe.min()

        return pd.concat(
            [mean, p10, p90, maximum, minimum],
            keys=["mean", "p10", "p90", "maximum", "minimum"],
            names=["statistic"],
            sort=False,
        )

    def agg(self, aggregation, keylist=None, excludekeys=None):
        """Aggregator, this is a wrapper that will
        call .to_virtual() on your behalf and call the corresponding
        agg() in VirtualEnsemble.
        """
        return self.to_virtual().agg(aggregation, keylist, excludekeys)

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
        return EnsembleCombination(self, sub=other)

    def __add__(self, other):
        return EnsembleCombination(self, add=other)

    def __radd__(self, other):
        return EnsembleCombination(self, add=other)

    def __rsub__(self, other):
        return EnsembleCombination(self, sub=other)

    def __mul__(self, other):
        return EnsembleCombination(self, scale=float(other))

    def __rmul__(self, other):
        return EnsembleCombination(self, scale=float(other))
