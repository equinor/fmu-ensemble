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

import re
import os
import glob
from collections import defaultdict
import pandas as pd
from ecl import EclDataType
from ecl.eclfile import EclKW

from fmu.config import etc
from .realization import ScratchRealization
from .virtualrealization import VirtualRealization
from .virtualensemble import VirtualEnsemble
from .ensemblecombination import EnsembleCombination
from .observation_parser import observations_parser


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
        self._global_active = None
        self._global_size = None
        self._global_grid = None

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

        # Remove failed realization from the ensemble
        list_of_failed = self.get_ok().query("OK == False")['REAL'].values
        if list_of_failed.size:
            logger.warning('The following failed realizations and were ' +
                           'removed from %s\n%s',  self._name,
                           ",".join(list_of_failed))
            logger.warning('This behaviour will change in the future, then ' +
                           'you need to explicitly filter non-OK away')
            self.remove_realizations(list_of_failed)

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
        return list(allkeys)

    def shortcut2path(self, shortpath):
        """
        Convert short pathnames to fully qualified pathnames
        within the datastore.

        If the fully qualified localpath is
            'share/results/volumes/simulator_volume_fipnum.csv'
        then you can also access this with these alternatives:
         * simulator_volume_fipnum
         * simulator_volume_fipnum.csv
         * share/results/volumes/simulator_volume_fipnum

        but only as long as there is no ambiguity. In case
        of ambiguity, the shortpath will be returned.

        CODE DUPLICATION from realization.py
        """
        basenames = map(os.path.basename, self.keys())
        if basenames.count(shortpath) == 1:
            short2path = {os.path.basename(x): x for x in self.keys()}
            return short2path[shortpath]
        noexts = [''.join(x.split('.')[:-1]) for x in self.keys()]
        if noexts.count(shortpath) == 1:
            short2path = {''.join(x.split('.')[:-1]): x
                          for x in self.keys()}
            return short2path[shortpath]
        basenamenoexts = [''.join(os.path.basename(x).split('.')[:-1])
                          for x in self.keys()]
        if basenamenoexts.count(shortpath) == 1:
            short2path = {''.join(os.path.basename(x).split('.')[:-1]): x
                          for x in self.keys()}
            return short2path[shortpath]
        # If we get here, we did not find anything that
        # this shorthand could point to. Return as is, and let the
        # calling function handle further errors.
        return shortpath

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
        logger.info('add_realizations() found %d realizations',
                    len(self._realizations))
        return count

    def remove_data(self, localpaths):
        """Remove certain datatypes from each realizations
        datastores. This modifies the underlying realization
        objects, and is equivalent to

        >>> del realization[localpath]

        on each realization in the ensemble.

        Args:
            localpath: string with full localpath to
                the data, or list of strings.
        """
        if isinstance(localpaths, str):
            localpaths = [localpaths]
        for localpath in localpaths:
            for _, real in self._realizations.items():
                del real[localpath]

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

    def to_virtual(self, name=None):
        """Convert the ScratchEnsemble to a VirtualEnsemble.

        This means that all imported data in realizations is
        aggregated and stored as dataframes in the virtual ensemble's
        data store.
        """
        vens = VirtualEnsemble(name=name)

        for key in self.keys():
            vens.append(key, self.get_df(key))

        return vens

    @property
    def parameters(self):
        """Getter for get_parameters(convert_numeric=True)
        """
        return self.from_txt('parameters.txt')

    def from_scalar(self, localpath, convert_numeric=False,
                    force_reread=False):
        """Parse a single value from a file.

        The value can be a string or a number.

        Empty files are treated as existing, with an empty string as
        the value, different from non-existing files.

        Parsing is performed individually in each realization
        """
        return self.from_file(localpath, 'scalar',
                              convert_numeric, force_reread)

    def from_txt(self, localpath, convert_numeric=True,
                 force_reread=False):
        """Parse a key-value text file from disk and internalize data

        Parses text files on the form
        <key> <value>
        in each line.

        Parsing is performed individually in each realization
        """
        return self.from_file(localpath, 'txt',
                              convert_numeric, force_reread)

    def from_csv(self, localpath, convert_numeric=True,
                 force_reread=False):
        """Parse a CSV file from disk and internalize data in a dataframe

        Parsing is performed individually in each realization."""
        return self.from_file(localpath, 'csv',
                              convert_numeric, force_reread)

    def from_file(self, localpath, fformat, convert_numeric=False,
                  force_reread=False):
        """Function for calling from_file() in every realization

        This function may utilize multithreading.

        Args:
            localpath: path to the text file, relative to each realization
            fformat: string identifying the file format. Supports 'txt'
                and 'csv'.
            convert_numeric: If set to True, numerical columns
                will be searched for and have their dtype set
                to integers or floats. If scalars, only numerical
                data will be loaded.
            force_reread: Force reread from file system. If
                False, repeated calls to this function will
                returned cached results.
        Returns:
            Dataframe with all parameters, indexed by realization index.
        """
        for index, realization in self._realizations.items():
            try:
                realization.from_file(localpath, fformat,
                                      convert_numeric, force_reread)
            except ValueError:
                # This would at least occur for unsupportd fileformat,
                # and that we should not skip.
                logger.critical('from_file() failed')
                raise ValueError  # (this might hide traceback from try:)
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
        """Load data from each realization and aggregate vertically

        Each row is tagged by the realization index in the column 'REAL'

        Args:
            localpath: string, filename local to the realizations
        Returns:
           dataframe: Merged data from each realization.
               Realizations with missing data are ignored.
               Empty dataframe if no data is found

        """
        dflist = {}
        for index, realization in self._realizations.items():
            try:
                data = realization.get_df(localpath)
                if isinstance(data, dict):
                    data = pd.DataFrame(index=[1], data=data)
                elif isinstance(data, str) or isinstance(data, int) \
                     or isinstance(data, float):
                    data = pd.DataFrame(index=[1], columns=[localpath],
                                        data=data)
                if isinstance(data, pd.DataFrame):
                    dflist[index] = data
                else:
                    raise ValueError("Unkown datatype returned " +
                                     "from realization")
            except ValueError:
                # No logging here, those error messages
                # should have appeared at construction using from_*()
                pass
        if dflist:
            # Merge a dictionary of dataframes. The dict key is
            # the realization index, and end up in a MultiIndex
            dframe = pd.concat(dflist, sort=False).reset_index()
            dframe.rename(columns={'level_0': 'REAL'}, inplace=True)
            del dframe['level_1']  # This is the indices from each real
            return dframe
        else:
            raise ValueError("No data found for " + localpath)

    def get_ok(self):
        """ collate the ok status for the ensemble """
        ens_ok = defaultdict(list)
        for index, realization in self._realizations.items():
            ens_ok['REAL'].append(index)
            ens_ok['OK'].append(realization.get_ok())
        return pd.DataFrame(ens_ok)

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
        for _, realization in self._realizations.items():
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

    def drop(self, localpath, **kwargs):
        """Delete elements from internalized data.

        Shortcuts are allowed for localpath. If the data pointed to is
        a DataFrame, you can delete columns, or rows containing certain
        elements

        If the data pointed to is a dictionary, keys can be deleted.

        Args:
            localpath: string, path to internalized data. If no other options
                are supplied, that dataset is deleted in its entirety
            column: string with a column name to drop. Only for dataframes
            columns: list of strings with column names to delete
            rowcontains: rows where one column contains this string will be
                dropped. The comparison is on strings only, and all cells in
                the dataframe is converted to strings for the comparison.
                Thus it might work on dates, but be careful with numbers.
            key: string with a keyname in a dictionary. Will not work for
                dataframes
            keys: list of strings of keys to delete from a dictionary
        """
        if self.shortcut2path(localpath) not in self.keys():
            raise ValueError("%s not found" % localpath)
        for _, realization in self._realizations.items():
            try:
                realization.drop(localpath, **kwargs)
            except ValueError:
                pass  # Allow localpath to be missing in some realizations

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
        # Build list of eclsum objects that are not None
        eclsums = []
        for _, realization in self._realizations.items():
            if realization.get_eclsum():
                eclsums.append(realization.get_eclsum())
        if freq == 'report' or freq == 'raw':
            dates = set()
            for eclsum in eclsums:
                dates = dates.union(eclsum.dates)
            dates = list(dates)
            dates.sort()
            return dates
        elif freq == 'last':
            end_date = max([eclsum.end_date for eclsum in eclsums])
            return [end_date]
        else:
            start_date = min([eclsum.start_date for eclsum in eclsums])
            end_date = max([eclsum.end_date for eclsum in eclsums])
            pd_freq_mnenomics = {'monthly': 'MS',
                                 'yearly': 'YS',
                                 'daily': 'D'}
            if freq not in pd_freq_mnenomics:
                raise ValueError('Requested frequency %s not supported' % freq)
            datetimes = pd.date_range(start_date, end_date,
                                      freq=pd_freq_mnenomics[freq])
            # Convert from Pandas' datetime64 to datetime.date:
            return [x.date() for x in datetimes]

    def get_smry_stats(self, column_keys=None, time_index='monthly'):
        """
        Function to extract the ensemble statistics (Mean, Min, Max, P10, P90)
        for a set of simulation summary vectors (column key).

        Output format of the function is tailored towards webviz_fan_chart
        (data layout and column naming)

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
            A dictionary. Index by column key to the corresponding ensemble
            summary statistics dataframe. Each dataframe has the dates in a
        column called 'index', and statistical data in 'min', 'max', 'mean',
        'p10', 'p90'. The column 'p10' contains the oil industry version of
        'p10', and is calculated using the Pandas p90 functionality.

        TODO: add warning message when failed realizations are removed
        """
        # Obtain an aggregated dataframe for only the needed columns over
        # the entire ensemble.

        dframe = self.get_smry(time_index=time_index, column_keys=column_keys)

        data = {}  # dict to be returned
        for key in dframe.columns.drop('DATE').drop('REAL'):
            dates = dframe.groupby('DATE').first().index.values
            name = [key] * len(dates)
            mean = dframe.groupby('DATE').mean()[key].values
            p10 = dframe.groupby('DATE').quantile(q=0.90)[key].values
            p90 = dframe.groupby('DATE').quantile(q=0.10)[key].values
            maximum = dframe.groupby('DATE').max()[key].values
            minimum = dframe.groupby('DATE').min()[key].values

            data[key] = pd.DataFrame({
                'index': dates,
                'name': name,
                'mean': mean,
                'p10': p10,
                'p90': p90,
                'max': maximum,
                'min': minimum
            })

        return data

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

    def agg(self, aggregation, keylist=None, excludekeys=None):
        """Aggregate the ensemble data into one VirtualRealization

        All data will be attempted aggregated. String data will typically
        be dropped in the result.

        Arguments:
            aggregation: string, supported modes are
                'mean', 'median', 'p10', 'p90', 'min',
                'max', 'std, 'var', 'pXX' where X is a number
            keylist: list of strings, indicating which keys
                in the internal datastore to include. If list is empty
                (default), all data will be attempted included.
            excludekeys: list of strings that should be excluded if
                keylist is empty, otherwise ignored
        Returns:
            VirtualRealization. Its name will include the aggregation operator

        WARNING: This code is duplicated in virtualensemble.py
        """
        quantilematcher = re.compile(r'p(\d\d)')
        supported_aggs = ['mean', 'median', 'min', 'max', 'std', 'var']
        if aggregation not in supported_aggs and \
           not quantilematcher.match(aggregation):
            raise ValueError("{arg} is not a".format(arg=aggregation) +
                             "supported ensemble aggregation")

        # Generate a new empty object:
        vreal = VirtualRealization(self.name + " " + aggregation)

        # Determine keys to use
        if isinstance(keylist, str):
            keylist = [keylist]
        if not keylist:  # Empty list means all keys.
            if not isinstance(excludekeys, list):
                excludekeys = [excludekeys]
            keys = set(self.keys()) - set(excludekeys)
        else:
            keys = keylist

        for key in keys:
            # Aggregate over this ensemble:
            # Ensure we operate on fully qualified localpath's
            key = self.shortcut2path(key)
            data = self.get_df(key)

            # This column should never appear in aggregated data
            del data['REAL']

            # Look for data we should group by. This would be beneficial
            # to get from a metadata file, and not by pure guesswork.
            groupbycolumncandidates = ['DATE', 'FIPNUM', 'ZONE', 'REGION',
                                       'JOBINDEX', 'Zone', 'Region_index']

            # Pick up string columns (or non-numeric values)
            # (when strings are used as values, this breaks, but it is also
            # meaningless to aggregate them. Most likely, strings in columns
            # is a label we should group over)
            stringcolumns = [x for x in data.columns if
                             data.dtypes[x] == 'object']

            groupby = [x for x in groupbycolumncandidates
                       if x in data.columns]

            # Add string columns
            if key != 'STATUS':  # STATUS dataframe contains too many strings..
                groupby = list(set(groupby + stringcolumns))

            dtypes = data.dtypes.unique()
            if not (int in dtypes or float in dtypes):
                logger.info("No numerical data to aggregate in %s", key)
                continue
            if len(groupby):
                logger.info("Grouping %s by %s", key, groupby)
                aggobject = data.groupby(groupby)
            else:
                aggobject = data

            if quantilematcher.match(aggregation):
                quantile = int(quantilematcher.match(aggregation).group(1))
                aggregated = aggobject.quantile(1 - quantile/100.0)
            else:
                # Passing through the variable 'aggregation' to
                # Pandas, thus supporting more than we have listed in
                # the docstring.
                aggregated = aggobject.agg(aggregation)

            if len(groupby):
                aggregated.reset_index(inplace=True)

            vreal.append(key, aggregated)
        return vreal

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

    def __sub__(self, other):
        result = EnsembleCombination(ref=self, sub=other)
        return result

    def __add__(self, other):
        result = EnsembleCombination(ref=self, add=other)
        return result

    def __mul__(self, other):
        result = EnsembleCombination(ref=self, scale=float(other))
        return result

    def __rsub__(self, other):
        result = EnsembleCombination(ref=self, sub=other)
        return result

    def __radd__(self, other):
        result = EnsembleCombination(ref=self, add=other)
        return result

    def __rmul__(self, other):
        result = EnsembleCombination(ref=self, scale=float(other))
        return result

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
            return pd.concat(dflist, sort=False).reset_index()
        else:
            raise NotImplementedError

    def from_obs_yaml(self, localpath):
        self.obs = observations_parser(localpath)
        return self.obs

    def ensemble_mismatch(self):
        dflist = []
        for index, realization in self._realizations.items():

            dframe = realization.realization_mismatch(self.obs)
            dflist.append(dframe)

        return pd.concat(dflist, sort=False).reset_index()

    def get_eclgrid(self, props, report=0, agg='mean', active_only=False):
        """
        Returns the grid (i,j,k) and (x,y), and any requested init
        and/or unrst property. The values are aggregated over the
        ensemble (mean/ std currently supported).

        Args:
            props: list of column key wildcards
            report: int. for unrst props only. Report step for given date.
                    Use the function get_unrst_report_dates to get an overview
                    of the report steps availible.
            agg: String. "mean" or "std".
            active_only: bool. True if activate cells only.
        Returns:
            A dictionary. Index by grid attribute, and contains a list
            corresponding to a set of values for each grid cells.
        """
        if not self._global_grid:
            self._global_grid = self.cell_layers(active_only=active_only)

        grid = self._global_grid
        grid = self._unwrap_grid(grid)
        for prop in props:
            print('Reading the grid property: '+prop)
            if prop in self.init_keys:
                grid[prop] = self.get_init(prop, agg=agg)
            if prop in self.unrst_keys:
                grid[prop] = self.get_unrst(prop, agg=agg, report=report)
        return pd.DataFrame(grid)

    @property
    def global_active(self):
        """
        :returns: An EclKw with, for each cell,
            the number of realizations where the cell is active.
        """
        if not self._global_active:
            self._global_active = EclKW('eactive',
                                        self.global_size,
                                        EclDataType.ECL_INT)
            for realization in self._realizations.values():
                self._global_active += realization.actnum

        return self._global_active

    @property
    def global_size(self):
        """
        :returns: global size of the realizations in the Ensemble.  see
            :func:`fmu_postprocessing.modelling.Realization.global_size()`.
        """
        if not self._realizations:
            return 0
        if self._global_size is None:
            self._global_size = self._realizations.values()[0].global_size
        return self._global_size

    @property
    def grid(self):
        """
        :returns: The grid of the ensemble, see
            :func:`fmu.ensemble.Realization.get_grid()`.
        """
        if not self._realizations:
            return None
        return self._realizations.values()[0].get_grid()

    def _is_active_cell(self, cell):
        """
        :returns: true if the given cell is an active cell
            in all self._realizations.
        """
        grid = self.grid
        active = self.global_active
        ijk = (cell['i'], cell['j'], cell['k'])
        return active[grid.get_global_index(ijk=ijk)] > 0

    def cell_layers(self, active_only=False):
        """
        :param active_only: `optional parameter`. Only return cells
            active in at least one realization.
        :returns: All cells in the grid, see
                see :func:`fmu.ensemble.Realization.cell_layers()`.
        """
        if not self._realizations:
            return None
        all_cells = self._realizations.values()[0].cell_layers(active_only=active_only)
        if active_only:
            return [filter(self._is_active_cell, layer) for layer in all_cells]

        return all_cells

    @property
    def init_keys(self):
        """ Keys availible in the eclipse init file """
        if not self._realizations:
            return None
        all_keys = set.union(
            *[set(realization.get_init().keys())
              for _, realization in self._realizations.iteritems()])
        return all_keys

    @property
    def unrst_keys(self):
        """ Keys availible in the eclipse unrst file """
        if not self._realizations:
            return None
        all_keys = set.union(
            *[set(realization.get_unrst().keys())
              for _, realization in self._realizations.iteritems()])
        return all_keys

    def get_unrst_report_dates(self):
        """ returns unrst report step and the corresponding date """
        if not self._realizations:
            return None
        all_report_dates = set.union(
            *[set(realization.report_dates)
              for _, realization in self._realizations.iteritems()])
        all_report_dates = list(all_report_dates)
        all_report_dates.sort()
        dframe = pd.DataFrame(all_report_dates, columns=['Dates'])
        dframe.index.names = ['Report']
        return dframe

    def get_init(self, prop, agg):
        """
        :param prop: A time independent property,
        :returns: Dictionary with ``mean`` or ``std_dev`` as keys,
            and corresponding values for given property as values.
        :raises ValueError: If prop is not found.
        """

        keywords = [(realization.get_global_init_keyword(prop))
                    for _, realization in self._realizations.iteritems()]

        if agg == 'mean':
            mean = self._keyword_mean(prop,
                                      keywords,
                                      self.global_active)
            return pd.Series(mean.numpy_copy(), name=prop)
        if agg == 'std':
            std_dev = self._keyword_std_dev(prop,
                                            keywords,
                                            self.global_active,
                                            mean)
            return pd.Series(std_dev.numpy_copy(), name=prop)

    def get_unrst(self, prop, report, agg):
        """
        :param prop: A time dependent property, see
            `fmu_postprocessing.modelling.SimulationGrid.TIME_DEPENDENT`.
        :returns: Dictionary with ``mean`` and ``std_dev`` as keys,
            and corresponding values for given property as values.
        :raises ValueError: If prop is not in `TIME_DEPENDENT`.
        """

        keywords = [realization.get_global_unrst_keyword(prop, report)
                    for _, realization in self._realizations.iteritems()]

        if agg == 'mean':
            mean = self._keyword_mean(prop,
                                      keywords,
                                      self.global_active)
            return pd.Series(mean.numpy_copy(), name=prop)
        if agg == 'std':
            std_dev = self._keyword_std_dev(prop,
                                            keywords,
                                            self.global_active,
                                            mean)
            return pd.Series(std_dev.numpy_copy(), name=prop)

    def _keyword_mean(self, name, keywords, num_realizations):
        """
        :returns: Mean values of keywords.
        :param name: Name of resulting Keyword.
        :param keywords: List of keywords.
        :param num_realizations: A EclKW with, for each cell, The number of
            realizations where the cell is active.
        """
        mean = EclKW(name, len(num_realizations), EclDataType.ECL_FLOAT)
        for keyword in keywords:
            mean += keyword
        mean.safe_div(num_realizations)
        return mean

    def _keyword_std_dev(self, name, keywords, num_realizations, mean):
        """
        :returns: Standard deviation of keywords.
        :param name: Name of resulting Keyword.
        :param keywords: List of pairs of keywords and list of active cell
        :param num_realizations: A EclKW with, for each cell, The number of
            realizations where the cell is active.
        :param mean: Mean of keywords.
        """
        std_dev = EclKW(name, len(num_realizations), EclDataType.ECL_FLOAT)
        for keyword in keywords:
            std_dev.add_squared(keyword - mean)

        std_dev.safe_div(num_realizations)
        std_dev.isqrt()
        return std_dev

    def _unwrap_grid(self, data):
        """
        code from fmu_postprocessing returns grid rows and coloumns,
        per grid layer. This function unwraps grid points to make a
        dictionary of lists, where each list corresponds to
        i,j,k,x0,y0,x1,y1,x2,y2,x3,y3
        """
        grid = defaultdict(list)
        for layer in data:
            for cell in layer:
                grid['i'].append(cell['i'])
                grid['j'].append(cell['j'])
                grid['k'].append(cell['k'])
                points = cell['points']
                grid['x0'].append(points[0][0])
                grid['y0'].append(points[0][1])
                grid['x1'].append(points[1][0])
                grid['y1'].append(points[1][1])
                grid['x2'].append(points[2][0])
                grid['y2'].append(points[2][1])
                grid['x3'].append(points[3][0])
                grid['y3'].append(points[3][1])

        return grid

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
