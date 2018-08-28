# -*- coding: utf-8 -*-
"""Module for parsing an ensemble from FMU. This class represents an
ensemble, which is nothing but a collection of realizations.

The typical task of this class is book-keeping of each realization,
and abilities to aggregate any information that each realization can
provide.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import glob
import pandas as pd

from fmu.config import etc
from .realization import ScratchRealization

xfmu = etc.Interaction()
logger = xfmu.functionlogger(__name__)


class ScratchEnsemble(object):
    """An ensemble is a collection of Realizations.

    Ensembles are initialized from path(s) pointing to
    filesystem locations containing realizations.

    Ensemble objects can be grouped into EnsembleSet.

    Realizations in an ensembles are uniquely determined
    by their realization index (integer).

    Attributes:
        files: A dataframe containing discovered files.

    Example:
        >>> import fmu.ensemble
        >>> myensemble = ensemble.Ensemble('ensemblename',
                    '/scratch/fmu/foobert/r089/casename/realization-*/iter-0')
    """

    def __init__(self, ensemble_name, paths):
        """Initialize an ensemble from disk

        Upon initialization, only a subset of the files on
        disk will be discovered.

        Args:
            ensemble_name (str): Name identifier for the ensemble.
                Optional to have it consistent with f.ex. iter-0 in the path.
            paths (list/str): String or list of strings with wildcards
                to file system. Absolute or relative paths.
        """
        self._name = ensemble_name  # ensemble name
        self._realizations = {}  # dict of ScratchRealization objects,
        # indexed by realization indices as integers.
        self._ens_df = pd.DataFrame()

        if isinstance(paths, str):
            paths = [paths]

        # Glob incoming paths to determine
        # paths for each realization (flatten and uniqify)
        globbedpaths = [glob.glob(path) for path in paths]
        globbedpaths = list(set([item for sublist in globbedpaths
                                 for item in sublist]))
        logger.info("Loading ensemble from dirs: %s",
                    " ".join(globbedpaths))

        # Search and locate minimal set of files
        # representing the realizations.
        count = self.add_realizations(paths)

        logger.info('ScratchEnsemble initialized with %d realizations',
                    count)

    def __getitem__(self, realizationindex):
        """Get one of the realizations.

        Indexed by integers."""
        return self._realizations[realizationindex]

    def keys(self):
        """
        Return the union of all keys available in realizations.

        Keys refer to the realization datastore, a dictionary
        of dataframes or dicts.
        """
        allkeys = set()
        for realization in self._realizations.values():
            allkeys = allkeys.union(realization.keys())
        return allkeys

    def add_realizations(self, paths):
        """Utility function to add realizations to the ensemble.

        Realizations are identified by their integer index.
        If the realization index already exists, it will be replaced
        when calling this function.

        This function passes on initialization to ScratchRealization
        and stores a reference to those generated objects.

        Args:
            paths (list/str): String or list of strings with wildcards
                to file system. Absolute or relative paths.

        Returns:
            count (int): Number of realizations successfully added.
        """
        if isinstance(paths, list):
            globbedpaths = [glob.glob(path) for path in paths]
            # Flatten list and uniquify:
            globbedpaths = list(set([item for sublist in globbedpaths
                                     for item in sublist]))
        else:
            globbedpaths = glob.glob(paths)

        count = 0
        for realdir in globbedpaths:
            realization = ScratchRealization(realdir)
            count += 1
            self._realizations[realization.index] = realization
        logger.info('add_realization() found %d realizations',
                    len(self._realizations))
        return count

    def remove_realizations(self, realindices):
        """Remove specific realizations from the ensemble

        Args:
            realindices : int or list of ints for the realization
            indices to be removed
        """
        if isinstance(realindices, int):
            realindices = [realindices]
        popped = 0
        for index in realindices:
            self._realizations.pop(index, None)
            popped += 1
        logger.info('removed %d realization(s)', popped)

    @property
    def parameters(self):
        """Getter for get_parameters(convert_numeric=True)
        """
        return self.from_txt('parameters.txt')

    def from_txt(self, localpath, convert_numeric=True,
                 force_reread=False):
        """Parse a key-value text file from disk

        Parses text files on the form
        <key> <value>
        in each line.
        """
        return self._from_file(localpath, 'txt',
                               convert_numeric, force_reread)

    def from_csv(self, localpath, convert_numeric=True,
                 force_reread=False):
        """Parse a CSV file from disk"""
        return self._from_file(localpath, 'csv',
                               convert_numeric, force_reread)

    def _from_file(self, localpath, fformat, convert_numeric=True,
                   force_reread=False):
        """Generalization of from_txt() and from_csv()

        Args:
            localpath: path to the text file, relative to each realization
            fformat: string identifying the file format. Supports 'txt'
                and 'csv'.
            convert_numeric: If set to True, numerical columns
                will be searched for and have their dtype set
                to integers or floats.
            force_reread: Force reread from file system. If
                False, repeated calls to this function will
                returned cached results.
        Returns:
            Dataframe with all parameters, indexed by realization index.
        """
        for index, realization in self._realizations.items():
            try:
                if fformat == 'csv':
                    realization.from_csv(localpath, convert_numeric,
                                         force_reread)
                elif fformat == 'txt':
                    realization.from_txt(localpath, convert_numeric,
                                         force_reread)
                else:
                    raise ValueError('Unrecognized file format ' + fformat)
            except IOError:
                # At ensemble level, we allow files to be missing in
                # some realizations
                logger.warn('Could not read %s for realization %d', localpath,
                            index)
        return self.get_df(localpath)

    def find_files(self, paths, metadata=None):
        """Discover realization files. The files dataframes
        for each realization will be updated.

        Certain functionality requires up-front file discovery,
        e.g. ensemble archiving and ensemble arithmetic.

        CSV files for single use does not have to be discovered.

        Args:
            paths: str or list of str with filenames (will be globbed)
                that are relative to the realization directory.
            metadata: dict with metadata to assign for the discovered
                files. The keys will be columns, and its values will be
                assigned as column values for the discovered files.
        """
        logger.warning("find_files() might become deprecated")
        for _, realization in self._realizations.items():
            realization.find_files(paths, metadata)

    def __repr__(self):
        return "<ScratchEnsemble {}, {} realizations>".format(self.name,
                                                              len(self))

    def __len__(self):
        return len(self._realizations)

    def get_smrykeys(self, vector_match=None):
        """
        Return a union of all Eclipse Summary vector names
        in all realizations (union).

        Args:
            vector_match: `Optional`. String (or list of strings)
               with wildcard filter. If None, all vectors are returned
        Returns:
            list of strings with summary vectors. Empty list if no
            summary file or no matched summary file vectors
        """
        if isinstance(vector_match, str):
            vector_match = [vector_match]
        result = set()
        for index, realization in self._realizations.items():
            eclsum = realization.get_eclsum()
            if eclsum:
                if vector_match is None:
                    result = result.union(set(eclsum.keys()))
                else:
                    for vector in vector_match:
                        result = result.union(set(eclsum.keys(vector)))
            else:
                logger.warn('No EclSum available for realization %d', index)
        return list(result)

    def get_df(self, localpath):
        """Load data from each realization and concatenate vertically

        Each row is tagged by the realization index.

        Args:
            localpath: string, filename local to realization
        Returns:
           dataframe: Merged data from each realization.
               Realizations with missing data are ignored.
               Empty dataframe if no data is found

        """
        dflist = {}
        for index, realization in self._realizations.items():
            try:
                dframe = realization.get_df(localpath)
                if isinstance(dframe, dict):
                    dframe = pd.DataFrame(index=[1], data=dframe)
                dflist[index] = dframe
            except ValueError:
                logger.warning('No data %s for realization %d',
                               localpath, index)
        if len(dflist):
            # Merge a dictionary of dataframes. The dict key is
            # the realization index, and end up in a MultiIndex
            df = pd.concat(dflist).reset_index()
            df.rename(columns={'level_0': 'REAL'}, inplace=True)
            del df['level_1']  # This is the indices from each real
            return df
        else:
            raise ValueError("No data found for " + localpath)

    def from_smry(self, time_index='raw', column_keys=None, stacked=True):
        """
        Fetch summary data from all realizations.

        The pr. realization results will be cached by each
        realization object, and can be retrieved through get_df().

        Wraps around Realization.from_smry() which wraps around
        ert.ecl.EclSum.pandas_frame()

        Beware that the default time_index or ensembles is 'monthly',
        differing from realizations which use raw dates by default.

        Args:
            time_index: list of DateTime if interpolation is wanted
               default is None, which returns the raw Eclipse report times
               If a string is supplied, that string is attempted used
               via get_smry_dates() in order to obtain a time index.
            column_keys: list of column key wildcards
            stacked: boolean determining the dataframe layout. If
                true, the realization index is a column, and dates are repeated
                for each realization in the DATES column.
                If false, a dictionary of dataframes is returned, indexed
                by vector name, and with realization index as columns.
                This only works when time_index is the same for all
                realizations. Not implemented yet!

        Returns:
            A DataFame of summary vectors for the ensemble, or
            a dict of dataframes if stacked=False.
        """
        if not stacked:
            raise NotImplementedError
        # Future: Multithread this!
        for index, realization in self._realizations.items():
            # We do not store the returned DataFrames here,
            # instead we look them up afterwards using get_df()
            # Downside is that we have to compute the name of the
            # cached object as it is not returned.
            realization.from_smry(time_index=time_index,
                                  column_keys=column_keys)
        if isinstance(time_index, list):
            time_index = 'custom'
        return self.get_df('share/results/tables/unsmry-' +
                           time_index + '.csv')

    def get_smry_dates(self, freq='monthly'):
        """Return list of datetimes for an ensemble according to frequency

        Args:
           freq: string denoting requested frequency for
               the returned list of datetime. 'report' or 'raw' will
               yield the sorted union of all valid timesteps for
               all realizations. Other valid options are
               'daily', 'monthly' and 'yearly'.
               'last' will give out the last date (maximum).
        Returns:
            list of datetimes.
        """
        if freq == 'report' or freq == 'raw':
            dates = set()
            for _, realization in self._realizations.items():
                dates = dates.union(realization.get_eclsum().dates)
            dates = list(dates)
            dates.sort()
            return dates
        elif freq == 'last':
            end_date = max([real[1].get_eclsum().end_date
                            for real in self._realizations.items()])
            return [end_date]
        else:
            start_date = min([real[1].get_eclsum().start_date
                              for real in self._realizations.items()])
            end_date = max([real[1].get_eclsum().end_date
                            for real in self._realizations.items()])
            pd_freq_mnenomics = {'monthly': 'MS',
                                 'yearly': 'YS',
                                 'daily': 'D'}
            if freq not in pd_freq_mnenomics:
                raise ValueError('Requested frequency %s not supported' % freq)
            datetimes = pd.date_range(start_date, end_date,
                                      freq=pd_freq_mnenomics[freq])
            # Convert from Pandas' datetime64 to datetime.date:
            return [x.date() for x in datetimes]

    def get_wellnames(self, well_match=None):
        """
        Return a union of all Eclipse Summary well names
        in all realizations (union). In addition, can return a list
        based on matches to an input string pattern.

        Args:
            well_match: `Optional`. String (or list of strings)
               with wildcard filter. If None, all wells are returned
        Returns:
            list of strings with eclipse well names. Empty list if no
            summary file or no matched well names.

        """

        if isinstance(well_match, str):
            well_match = [well_match]
        result = set()
        for _, realization in self._realizations.items():
            eclsum = realization.get_eclsum()
            if eclsum:
                if well_match is None:
                    result = result.union(set(eclsum.wells()))
                else:
                    for well in well_match:
                        result = result.union(set(eclsum.wells(well)))

        return sorted(list(result))

    def get_groupnames(self, group_match=None):
        """
        Return a union of all Eclipse Summary group names
        in all realizations (union). In addition, can return a list
        based on matches to an input string pattern.

        Args:
            well_match: `Optional`. String (or list of strings)
               with wildcard filter. If None, all wells are returned
        Returns:
            list of strings with eclipse well names. Empty list if no
            summary file or no matched well names.

        """

        if isinstance(group_match, str):
            group_match = [group_match]
        result = set()
        for _, realization in self._realizations.items():
            eclsum = realization.get_eclsum()
            if eclsum:
                if group_match is None:
                    result = result.union(set(eclsum.groups()))
                else:
                    for group in group_match:
                        result = result.union(set(eclsum.groups(group)))

        return sorted(list(result))

    @property
    def files(self):
        """Return a concatenation of files in each realization"""
        filedflist = []
        for realidx, realization in self._realizations.items():
            realfiles = realization.files.copy()
            realfiles.insert(0, 'REAL', realidx)
            filedflist.append(realfiles)
        return pd.concat(filedflist, ignore_index=True, sort=False)

    @property
    def name(self):
        """The ensemble name."""
        return self._name

    @name.setter
    def name(self, newname):
        if isinstance(newname, str):
            self._name = newname
        else:
            raise ValueError('Name input is not a string')


def _convert_numeric_columns(dataframe):
    """Discovers and searches for numeric columns
    among string columns in an incoming dataframe.
    Columns with mostly integer

    Args:
        dataframe : any dataframe with strings as column datatypes

    Returns:
        A dataframe where some columns have had their datatypes
        converted to numerical types (int/float). Some values
        might contain numpy.nan.
    """
    logger.warn("_convert_numeric_columns() not implemented")
    return dataframe
