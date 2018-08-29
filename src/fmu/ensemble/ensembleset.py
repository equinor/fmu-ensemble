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

    def keys(self):
        """
        Return the union of all keys available in the ensembles.

        Keys refer to the realization datastore, a dictionary
        of dataframes or dicts.
        """
        allkeys = set()
        for ensemble in self._ensembles.values():
            allkeys = allkeys.union(ensemble.keys())
        return allkeys

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
        """Getter for ensemble.parameters(convert_numeric=True)
        """
        return self.get_df('parameters.txt')

    def from_txt(self, localpath, convert_numeric=True,
                 force_reread=False):
        """Parse and internalize a txt-file from disk

        Parses text files on the form
        <key> <value>
        in each line."""
        return self._from_file(localpath, 'txt', convert_numeric,
                               force_reread)

    def from_csv(self, localpath, convert_numeric=True,
                 force_reread=False):
        """Parse and internalize a CSV file from disk"""
        return self._from_file(localpath, 'csv', convert_numeric,
                               force_reread)

    def _from_file(self, localpath, fformat, convert_numeric=True,
                   force_reread=False):
        """Internal function for from_*()"""
        for _, ensemble in self._ensembles.items():
            ensemble._from_file(localpath, fformat, convert_numeric,
                                force_reread)
        return self.get_df(localpath)

    def get_df(self, localpath):
        """Collect contents of dataframes from each ensemble

        Args:
            localpath: path to the text file, relative to each realization
            convert_numeric: If set to True, numerical columns
                will be searched for and have their dtype set
                to integers or floats.
            force_reread: Force reread from file system. If
                False, repeated calls to this function will
                returned cached results.
        """
        ensdflist = []
        for _, ensemble in self._ensembles.items():
            ensdf = ensemble.get_df(localpath)
            ensdf.insert(0, 'ENSEMBLE', ensemble.name)
            ensdflist.append(ensdf)
        return pd.concat(ensdflist, sort=False)

    def get_csv_deprecated(self, filename):
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
        return pd.concat(dflist, sort=False)

    def from_smry(self, time_index=None, column_keys=None):
        """
        Fetch summary data from all ensembles

        Wraps around Ensemble.from_smry() which wraps
        Realization.from_smry(), which wraps ert.ecl.EclSum.pandas_frame()

        The time index is determined at realization level. If you
        ask for 'monthly', you will from each realization get its
        months. At ensemble or ensembleset-level, the number of
        monthly report dates between realization can vary

        The pr. realization results will be cached by each
        realization object, and can be retrieved through get_df().

        Args:
            time_index: list of DateTime if interpolation is wanted
               default is None, which returns the raw Eclipse report times
               If a string is supplied, that string is attempted used
               via get_smry_dates() in order to obtain a time index.
            column_keys: list of column key wildcards
        Returns:
            A DataFame of summary vectors for the ensembleset.
            The column 'ENSEMBLE' will denote each ensemble's name
        """
        # Future: Multithread this:
        for nam, ensemble in self._ensembles.items():
            ensemble.from_smry(time_index=time_index,
                               column_keys=column_keys)
        if isinstance(time_index, list):
            time_index = 'custom'
        return self.get_df('share/results/tables/unsmry-' +
                           time_index + '.csv')

    def get_smry_dates(self, freq='monthly'):
        """Return list of datetimes from an ensembleset

        Datetimes from each realization in each ensemble can
        be returned raw, or be resampled.

        Args:
           freq: string denoting requested frequency for
               the returned list of datetime. 'report' will
               yield the sorted union of all valid timesteps for
               all realizations. Other valid options are
               'daily', 'monthly' and 'yearly'.
        Returns:
            list of datetime.date.
        """

        rawdates = set()
        for _, ensemble in self._ensembles.items():
            rawdates = rawdates.union(ensemble.get_smry_dates(freq='report'))
        rawdates = list(rawdates)
        rawdates.sort()
        if freq == 'report':
            return rawdates
        else:
            # Later optimization: Wrap eclsum.start_date in the
            # ensemble object.
            start_date = min(rawdates)
            end_date = max(rawdates)
            pd_freq_mnenomics = {'monthly': 'MS',
                                 'yearly': 'YS', 'daily': 'D'}
            if freq not in pd_freq_mnenomics:
                raise ValueError('Requested frequency %s not supported' % freq)
            datetimes = pd.date_range(start_date, end_date,
                                      freq=pd_freq_mnenomics[freq])
            # Convert from Pandas' datetime64 to datetime.date:
            return [x.date() for x in datetimes]
