# -*- coding: utf-8 -*-
"""Module for book-keeping and aggregation of ensembles
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import re
import glob
import pandas as pd

from fmu.config import etc
from .ensemble import ScratchEnsemble

xfmu = etc.Interaction()
logger = xfmu.functionlogger(__name__)


class EnsembleSet(object):
    """An ensemble set is any collection of ensembles.

    Ensembles might be both ScratchEnsembles or VirtualEnsembles.
    """

    def __init__(self, ensembleset_name, ensembles):
        """Initiate an ensemble set from a list of ensembles

        Args:
            ensemblesetname: string with the name of the ensemble set
            ensembles: list of existing Ensemble objects. Can be empty.
        """
        self._name = ensembleset_name
        self._ensembles = {}  # Dictionary indexed by each ensemble's name.
        for ensemble in ensembles:
            self._ensembles[ensemble.name] = ensemble

    @property
    def name(self):
        """Return the name of the ensembleset,
        as initialized"""
        return self._name

    def __len__(self):
        return len(self._ensembles)

    def __getitem__(self, name):
        return self._ensembles[name]

    def __repr__(self):
        return "<EnsembleSet {}, {} ensembles:\n{}>".format(
            self.name, len(self), self._ensembles)

    def add_ensembles_frompath(self, paths):
        """Convenience function for adding multiple ensembles.

        Tailored for the realization-*/iter-* disk structure.

        Args:
            path: str or list of strings with path to the
                directory containing the realization-*/iter-*
                structure
        """
        if isinstance(paths, str):
            if 'realization' not in paths:
                paths = paths + '/realization-*/iter-*'
            paths = [paths]
        globbedpaths = [glob.glob(path) for path in paths]
        globbedpaths = list(set([item for sublist in globbedpaths
                                 for item in sublist]))
        realidxregexp = re.compile(r'.*realization-(\d+).*')
        iteridxregexp = re.compile(r'.*iter-(\d+).*')

        reals = set()
        iters = set()
        for path in globbedpaths:
            realidxmatch = re.match(realidxregexp, path)
            if realidxmatch:
                reals.add(int(realidxmatch.group(1)))
            iteridxmatch = re.match(iteridxregexp, path)
            if iteridxmatch:
                iters.add(int(iteridxmatch.group(1)))

        # Initialize ensemble objects for each iter found:
        for iterr in iters:
            ens = ScratchEnsemble('iter-' + str(iterr),
                                  [x for x in globbedpaths
                                   if 'iter-' + str(iterr) in x])
            self._ensembles[ens.name] = ens

    def add_ensemble(self, ensembleobject):
        """Add a single ensemble to the ensemble set

        Name is taken from the ensembleobject.
        """
        self._ensembles[ensembleobject.name] = ensembleobject

    @property
    def parameters(self):
        """Getter for get_parameters(convert_numeric=True)
        """
        return self.get_parameters(self)

    def get_parameters(self, convert_numeric=True):
        """Collect contents of the parameters.txt files
        from each of the ensembles. Return as one dataframe
        tagged with realization index, columnname REAL,
        and ensemble name in ENSEMBLE

        Args:
            convert_numeric: If set to True, numerical columns
                will be searched for and have their dtype set
                to integers or floats.
        """
        ensparamsdictlist = []
        for _, ensemble in self._ensembles.items():
            params = ensemble.get_parameters(convert_numeric)
            params['ENSEMBLE'] = ensemble.name
            ensparamsdictlist.append(params)
        return pd.concat(ensparamsdictlist)

    def get_csv(self, filename):
        """Load CSV data from each realization in each
        ensemble, and aggregate.

        Args:
            filename: string, filename local to realization
        Returns:
           dataframe: Merged CSV from each realization.
               Realizations with missing data are ignored.
               Empty dataframe if no data is found
        """
        dflist = []
        for _, ensemble in self._ensembles.items():
            dframe = ensemble.get_csv(filename)
            dframe['ENSEMBLE'] = ensemble.name
            dflist.append(dframe)
        return pd.concat(dflist)
