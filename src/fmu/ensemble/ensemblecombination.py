# -*- coding: utf-8 -*-
"""Module for handling linear combinations of ensembles.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from fmu.config import etc
from fmu.ensemble.virtualensemble import VirtualEnsemble

xfmu = etc.Interaction()
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
        indexcandidates = ['REAL', 'DATE', 'ZONE', 'REGION']
        for index in indexcandidates:
            if index in self.ref.get_df(localpath).columns:
                indexlist.append(index)
        refdf = self.ref.get_df(localpath).set_index(indexlist)
        refdf = refdf.select_dtypes(include='number')
        result = refdf.mul(self.scale)
        if self.add:
            otherdf = self.add.get_df(localpath).set_index(indexlist)
            otherdf = otherdf.select_dtypes(include='number')
            result = result.add(otherdf)
        if self.sub:
            otherdf = self.sub.get_df(localpath).set_index(indexlist)
            otherdf = otherdf.select_dtypes(include='number')
            result = result.sub(otherdf)
        # Delete rows where everything is NaN, which will be case when
        # realization (multi-)indices does not match up in both ensembles.
        result.dropna(axis='index', how='all', inplace=True)
        # Also delete columns where everything is NaN, happens when
        # column data are not similar
        result.dropna(axis='columns', how='all', inplace=True)
        return result.reset_index()

    def to_virtual(self):
        """Evaluate the current linear combination and return as
        a virtual ensemble.
        """
        vens = VirtualEnsemble(name=str(self))
        for key in self.keys():
            vens.append(key, self.get_df(key))
        return vens

    def get_smry_dates(self, freq='monthly'):
        """Create a union of dates available in the
        involved ensembles
        """
        dates = set(self.ref.get_smry_dates(freq))
        if self.add:
            dates = dates.union(set(self.add.get_smry_dates(freq)))
        if self.sub:
            dates = dates.union(set(self.add.get_smry_dates(freq)))
        dates = list(dates)
        dates.sort()
        return dates

    def get_smry(self, time_index=None, column_keys=None):
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
        indexlist = ['REAL', 'DATE']
        refdf = self.ref.get_smry(time_index=time_index,
                                  column_keys=column_keys).set_index(indexlist)
        result = refdf.mul(self.scale)
        if self.add:
            otherdf = self.add\
                          .get_smry(time_index=time_index,
                                    column_keys=column_keys)\
                          .set_index(indexlist)
            result = result.add(otherdf)
        if self.sub:
            otherdf = self.sub\
                          .get_smry(time_index=time_index,
                                    column_keys=column_keys)\
                          .set_index(indexlist)
            result = result.sub(otherdf)
        return result.reset_index()

    def __getitem__(self, localpath):
        return self.get_df(localpath)

    def __repr__(self):
        """Try to give out a linear expression"""
        # NB: Implementation in this method requires scaling not to happen
        # simultaneously as adds or subs.
        scalestring = ''
        addstring = ''
        substring = ''
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
