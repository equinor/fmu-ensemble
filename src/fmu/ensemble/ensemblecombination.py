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

    def find_combined(self):
        """
        The function finds the corresponding realizations
        which have completed successfully in the all ensembles.
        """
        ref_ok = set(self.ref.get_ok().query('OK == True')['REAL'].tolist())
        operations = self.subs + self.adds
        for operator in operations:
            ior_ok = set(operator.get_ok().query('OK == True')['REAL'].tolist()) # noqa
            ref_ok = list(ref_ok & ior_ok)
        return ref_ok

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
        return result.reset_index()

    def to_virtual(self):
        """Evaluate the current linear combination and return as
        a virtual ensemble.
        """
        vens = VirtualEnsemble(name=str(self))
        for key in self.keys():
            vens.append(key, self.get_df(key))
        return vens

    def get_smry(self, column_keys=None):
        """
        This function performs airthmetic operations on the
        ensembles and return the corresponding dataframe with the
        requested simulation summary keys.
        """
        return None
        time_index = self.ref.get_smry_dates(freq='daily')
        ref = self.ref.from_smry(time_index=time_index,
                                 column_keys=column_keys,
                                 stacked=True)
        ref = ref[ref['REAL'].isin(self.combined)]
        dates = ref['DATE']
        real = ref['REAL']
        ref.drop(columns=['DATE'], inplace=True)
        if self.subs:
            for sub in self.subs:
                ior = sub.from_smry(time_index=time_index,
                                    column_keys=column_keys,
                                    stacked=True)
                ior.drop(columns=['DATE'], inplace=True)
                ior = ior[ior['REAL'].isin(self.combined)]
                ref = ior - ref
        if self.adds:
            for add in self.adds:
                ior = add.from_smry(time_index=time_index,
                                    column_keys=column_keys,
                                    stacked=True)
                ior.drop(columns=['DATE'], inplace=True)
                ior = ior[ior['REAL'].isin(self.combined)]
                ref = ior + ref
        ref.insert(0, 'DATE', dates)
        ref['REAL'] = real

        return ref

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
