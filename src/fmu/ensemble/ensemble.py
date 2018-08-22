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
        for index in realindices:
            self._realizations.pop(index, None)

    @property
    def parameters(self):
        """Getter for get_parameters(convert_numeric=True)
        """
        return self.get_parameters(self)

    def get_parameters(self, convert_numeric=True):
        """Collect contents of the parameters.txt files
        the ensemble contains, and return as one dataframe
        tagged with realization index, columnname REAL

        Args:
            convert_numeric: If set to True, numerical columns
                will be searched for and have their dtype set
                to integers or floats.
        Returns:
            Dataframe with all parameters, indexed by realization index.
        """
        paramsdictlist = []
        for index, realization in self._realizations.items():
            params = realization.parameters
            params['REAL'] = index
            paramsdictlist.append(params)
        return pd.DataFrame(paramsdictlist)

    def get_status_data(self):
        """Collects the contents of the STATUS files and jobs.json
        from all realizations.

        Each row in the dataframe is a finished FORWARD_MODEL
        The STATUS files are parsed and information is extracted.
        Job duration is calculated, but jobs above 24 hours
        get incorrect durations.

        Returns:
            A dataframe with information from the STATUS files.
            Each row represents one job in one of the realizations.
        """
        statusdict = {}  # dict of status dataframes pr. realization
        for realidx, realization in self._realizations.items():
            statusdict[realidx] = realization.get_status()
            statusdict[realidx]['REAL'] = realidx  # Tag it!
        return pd.concat(statusdict, ignore_index=True)

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
        for _, realization in self._realizations.items():
            realization.find_files(paths, metadata)

    def __repr__(self):
        return "<Ensemble {}, {} realizations>".format(self.name, len(self))

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
        for _, realization in self._realizations.items():
            eclsum = realization.get_eclsum()
            if eclsum:
                if vector_match is None:
                    result = result.union(set(eclsum.keys()))
                else:
                    for vector in vector_match:
                        result = result.union(set(eclsum.keys(vector)))
        return list(result)

    def get_csv(self, filename):
        """Load CSV data from each realization and concatenated vertically

        Each row is tagged by the realization index.
        The loaded CSV files does not have to be discovered,
        and will not be discovered either by this call.

        Args:
            filename: string, filename local to realization
        Returns:
           dataframe: Merged CSV from each realization.
               Realizations with missing data are ignored.
               Empty dataframe if no data is found

        """
        dflist = []
        for index, realization in self._realizations.items():
            dframe = realization.get_csv(filename)
            dframe.insert(0, 'REAL', index)
            dflist.append(dframe)
        return pd.concat(dflist)

    def get_smry(self, time_index=None, column_keys=None, stacked=True):
        """
        Aggregates summary data from all realizations.

        Wraps around Realization.get_smry() which wraps around
        ert.ecl.EclSum.pandas_frame()

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
        if isinstance(time_index, str):
            time_index = self.get_smry_dates(time_index)
        if stacked:
            dflist = []
            for index, realization in self._realizations.items():
                dframe = realization.get_smry(time_index=time_index,
                                              column_keys=column_keys)
                dframe.insert(0, 'REAL', index)
                dframe.index.name = 'DATE'
                dflist.append(dframe)
            return pd.concat(dflist).reset_index()
        else:
            raise NotImplementedError

    def get_smry_dates(self, freq='monthly'):
        """Return list of datetimes for an ensemble according to frequency

        Args:
           freq: string denoting requested frequency for
               the returned list of datetime. 'report' will
               yield the sorted union of all valid timesteps for
               all realizations. Other valid options are
               'daily', 'monthly' and 'yearly'.
               'last' will give out the last date (maximum).
        Returns:
            list of datetimes.
        """
        if freq == 'report':
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
        files = pd.DataFrame()
        for realidx, realization in self._realizations.items():
            realfiles = realization.files.copy()
            realfiles.insert(0, 'REAL', realidx)
            files = files.append(realfiles)
        return files

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
