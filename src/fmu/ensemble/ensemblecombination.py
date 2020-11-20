"""Module for handling linear combinations of ensembles"""

import fnmatch
import logging

import pandas as pd

logger = logging.getLogger(__name__)


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

    def get_df(self, localpath, merge=None):
        """Obtain given data from the ensemblecombination,
        doing the actual computation of ensemble on the fly.

        Warning: In order to add dataframes together with meaning,
        using pandas.add, the index of the frames must be correctly set,
        and this can be tricky for some datatypes (f.ex. volumetrics table
        where you want to add together volumes for correct zone
        and fault segment).

        If you have the columns "REAL", "DATE", "ZONE" and/or "REGION", it
        will be regarded as an index column.

        Args:
            localpath (str): refers to the internalized name of the
                data wanted in each ensemble.
            merge (list or str): Optional data to be merged in for the data
                The merge will happen as deep as possible (in realization
                objects in case of ScratchEnsembles), and all ensemble
                combination computations happen after merging. Be careful
                with index guessing and merged data.
        """
        # We can pandas.add when the index is set correct.
        # WE MUST GUESS!
        indexlist = []
        indexcandidates = ["REAL", "DATE", "ZONE", "REGION"]
        for index in indexcandidates:
            if index in self.ref.get_df(localpath).columns:
                indexlist.append(index)
        logger.debug("get_df() inferred index columns to %s", str(indexlist))
        refdf = self.ref.get_df(localpath, merge=merge).set_index(indexlist)
        refdf = refdf.select_dtypes(include="number")
        result = refdf.mul(self.scale)
        if self.add:
            otherdf = self.add.get_df(localpath, merge=merge).set_index(indexlist)
            otherdf = otherdf.select_dtypes(include="number")
            result = result.add(otherdf)
        if self.sub:
            otherdf = self.sub.get_df(localpath, merge=merge).set_index(indexlist)
            otherdf = otherdf.select_dtypes(include="number")
            result = result.sub(otherdf)
        # Delete rows where everything is NaN, which will be case when
        # realization (multi-)indices does not match up in both ensembles.
        result.dropna(axis="index", how="all", inplace=True)
        # Also delete columns where everything is NaN, happens when
        # column data are not similar
        result.dropna(axis="columns", how="all", inplace=True)
        return result.reset_index()

    def to_virtual(self, keyfilter=None):
        """Evaluate the current linear combination and return as
        a virtual ensemble.

        Args:
            keyfilter (list or str): If supplied, only keys matching wildcards
                in this argument will be included. Use this for speed reasons
                when only some data is needed. Default is to include everything.
                If you supply "unsmry", it will match every key that
                includes this string by prepending and appending '*' to your pattern

        Returns:
            VirtualEnsemble
        """
        # pylint: disable=import-outside-toplevel
        from .virtualensemble import VirtualEnsemble

        if keyfilter is None:
            keyfilter = "*"
        if isinstance(keyfilter, str):
            keyfilter = [keyfilter]
        if not isinstance(keyfilter, list):
            raise TypeError("keyfilter in to_virtual() must be list or string")

        vens = VirtualEnsemble(name=str(self))
        for key in self.keys():
            if sum(
                [fnmatch.fnmatch(key, "*" + pattern + "*") for pattern in keyfilter]
            ):
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
        ensemble data. The ensembles can be ScratchEnsemble or
        VirtualEnsemble, if scratch it will access binary
        summary files directly, if virtual ensembles, summary
        data must have been loaded earlier.

        Args:
            column_keys (str or list): column key wildcards. Default
                is '*', which will match all vectors in the Eclipse
                output.
            time_index (str or list of DateTime): time_index mnemonic or
                a list of explicit datetime at which the summary data
                is requested (interpolated or extrapolated)

        Returns:
            pd.DataFrame. Indexed by rows, has at least the columns REAL
                and DATE if not empty.
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
        * wgname (str or None)

        Args:
            column_keys: List or str of column key wildcards
        """
        meta = self.ref.get_smry_meta(column_keys=column_keys)
        if self.add:
            meta.update(self.add.get_smry_meta(column_keys=column_keys))
        if self.sub:
            meta.update(self.sub.get_smry_meta(column_keys=column_keys))
        return meta

    def agg(self, aggregation, keylist=None, excludekeys=None):
        """Aggregator, this is a wrapper that will
        call .to_virtual() on your behalf and call the corresponding
        agg() in VirtualEnsemble.
        """
        return self.to_virtual().agg(aggregation, keylist, excludekeys)

    def get_volumetric_rates(
        self, column_keys=None, time_index="monthly", time_unit=None
    ):
        """Compute volumetric rates from cumulative summary
        vectors.

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

        Args:
            column_keys: str or list of strings, cumulative summary vectors
            time_index: str or list of datetimes
            time_unit: str or None. If None, the rates returned will
                be the difference in cumulative between each included
                time step (where the time interval can vary arbitrarily)
                If set to 'days', 'months' or 'years', the rates will
                be scaled to represent a daily, monthly or yearly rate that
                is compatible with the date index and the cumulative data.

        """
        return self.to_virtual(keyfilter="unsmry").get_volumetric_rates(
            column_keys=column_keys, time_index=time_index, time_unit=time_unit
        )

    @property
    def parameters(self):
        """Return parameters from the ensemble as a class property"""
        try:
            return self.get_df("parameters.txt")
        except KeyError:
            return pd.DataFrame()

    def __len__(self):
        """Estimate the number of realizations in this
        ensemble combinations.

        This is not always well defined in cases of strange
        combinations of which data is available in which realization,
        so after actual computation of a virtual ensemble, the number
        of realizations can be less that what this estimate returns

        Returns:
            int, number of realizations (upper limit)
        """
        return len(self.get_realindices())

    def get_realindices(self):
        """Return the integer indices for realizations in this ensemble

        There is no guarantee that all realizations returned here
        will be valid for all datatypes after computation.

        Returns:
            list of integers
        """
        indices = set(self.ref.get_realindices())
        if self.add:
            indices = indices.intersection(set(self.add.get_realindices()))
        if self.sub:
            indices = indices.intersection(set(self.sub.get_realindices()))
        return list(indices)

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
        """Substract another ensemble from this combination"""
        return EnsembleCombination(self, sub=other)

    def __add__(self, other):
        """Add another ensemble from this combination"""
        return EnsembleCombination(self, add=other)

    def __radd__(self, other):
        """Add another ensemble from this combination"""
        return EnsembleCombination(self, add=other)

    def __rsub__(self, other):
        """Substract another ensemble from this combination"""
        return EnsembleCombination(self, sub=other)

    def __mul__(self, other):
        """Scale this EnsembleCombination by a scalar value"""
        return EnsembleCombination(self, scale=float(other))

    def __rmul__(self, other):
        """Scale this EnsembleCombination by a scalar value"""
        return EnsembleCombination(self, scale=float(other))
