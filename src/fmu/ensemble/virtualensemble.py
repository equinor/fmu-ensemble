# -*- coding: utf-8 -*-
"""Module containing a VirtualEnsemble class"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import re
import fnmatch
import pandas as pd

from fmu import config
from fmu.ensemble.virtualrealization import VirtualRealization

fmux = config.etc.Interaction()
logger = fmux.basiclogger(__name__)


class VirtualEnsemble(object):
    """A computed or archived ensemble

    Computed or archived, one cannot assume to have access to the file
    system containing original data.

    Contrary to a ScratchEnsemble, a VirtualEnsemble stores aggregated
    dataframes. The column REAL signifies the realization index.
    """
    def __init__(self, name=None, data=None, longdescription=None):
        """
        Initialize a virtual ensemble.

        Typical use of this constructor is from ScratchEnsemble.to_virtual()
        Args:
            name: string, can be chosen freely
            data: dict with data to initialize with. Defaults to empty
            longdescription: string, free form multiline description.
        """
        if name:
            self._name = name
        else:
            self._name = "VirtualEnsemble"

        self._longdescription = longdescription

        # At ensemble level, this dictionary has dataframes only.
        # All dataframes have the column REAL.
        if data:
            self.data = data
        else:
            self.data = {}

        self.realindices = []

    def __len__(self):
        """Return the number of realizations included in the ensemble"""
        return len(self.realindices)

    def update_realindices(self):
        """Update the internal list of known realization indices

        Anything that adds or removes realizations must
        take responsibility for having that list consistent.

        If there is a dataframe missing the REAL column, this
        will intentionally error.
        """

        # Check all dataframes:
        idxset = set()
        for key in self.data.keys():
            idxset = idxset | \
                set(self.data[key]['REAL'].unique())
        self.realindices = list(idxset)

    def keys(self):
        """Return all keys in the internal datastore"""
        return self.data.keys()

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

        CODE DUPLICATION.
        """
        basenames = list(map(os.path.basename, self.keys()))
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

    def __getitem__(self, localpath):
        """Return a specific datatype, shorthands are allowed"""
        return self.get_df(localpath)

    def get_realization(self, realindex):
        """
        Return a virtual realization object, with data
        taken from the virtual ensemble.
        """
        vreal = VirtualRealization(description="Realization %d from %s" %
                                   (realindex, self._name))
        for key in self.data.keys():
            data = self.get_df(key)
            realizationdata = data[data['REAL'] == realindex]
            if len(realizationdata) == 1:
                # Convert scalar values to dictionaries, avoiding
                # getting length-one-series returned later on access.
                realizationdata = realizationdata.iloc[0].to_dict()
            elif len(realizationdata) > 1:
                realizationdata.reset_index(inplace=True, drop=True)
            else:
                continue
            del realizationdata['REAL']
            vreal.append(key, realizationdata)
        if vreal.keys():
            return vreal
        else:
            raise ValueError("No data for realization %d" % realindex)

    def add_realization(self, realization, realidx=None, overwrite=False):
        """Add a realization. A ScratchRealization will be effectively
        converted to a virtual realization.

        A ScratchRealization knows its realization index, and that index
        will be used unless realidx is not None. A VirtualRealization does
        not always have a index, so then it must be supplied.

        Unless overwrite is True, a ValueError will be raised
        if the realization index already exists.

        Args:
            overwrite: boolean whether an existing realization with the same
                index should be removed prior to adding
            realidx: Override the realization index for incoming realization.
                Necessary for VirtualRealization.
        """
        if realidx is None and isinstance(realization, VirtualRealization):
            raise ValueError("Can't add virtual realizations " +
                             "without specifying index")
        if not realidx:
            realidx = realization.index

        if not overwrite and realidx in self.realindices:
            raise ValueError("Error, realization index already present")
        if overwrite and realidx in self.realindices:
            self.remove_realization(realidx)

        # Add the data from the incoming realization key by key
        for key in realization.keys():
            df = realization.get_df(key)
            if isinstance(df, dict):  # dicts to go to one-row dataframes
                df = pd.DataFrame(index=[1], data=df)
            if isinstance(df, (str, int, float)):
                df = pd.DataFrame(index=[1], columns=[key], data=df)
            df['REAL'] = realidx
            if key not in self.data.keys():
                self.data[key] = df
            else:
                self.data[key] = \
                    self.data[key].append(df, ignore_index=True,
                                          sort=True)
        self.update_realindices()

    def remove_realizations(self, deleteindices):
        """Remove realizations from internal data

        This will remove all rows in all internalized data belonging
        to the set of supplied indices.

        Args:
            deleteindices: int or list of ints, realization indices to remove
        """
        if not isinstance(deleteindices, list):
            deleteindices = [deleteindices]

        indicesknown = self.realindices
        indicestodelete = list(set(deleteindices) & set(indicesknown))
        indicesnotknown = list(set(deleteindices) - set(indicestodelete))
        if indicesnotknown:
            logger.warn("Skipping undefined realization indices %s",
                        str(indicesnotknown))
        # There might be Pandas tricks to avoid this outer loop.
        for realindex in indicestodelete:
            for key in self.data:
                self.data[key] = self.data[key][self.data[key]['REAL']
                                                != realindex]
        self.update_realindices()
        logger.info("Removed %s realization(s) from VirtualEnsemble",
                    len(indicestodelete))

    def remove_data(self, localpaths):
        """Remove a certain datatype from the internal datastore

        Args:
            localpaths: string or list of strings, fully qualified localpath
                (no shorthand allowed)
        """
        if not isinstance(localpaths, list):
            localpaths = [localpaths]
        for localpath in localpaths:
            if localpath in self.data:
                del self.data[localpath]
                logger.info("Deleted %s from ensemble", localpath)
            else:
                logger.warning("Ensemble did not contain %s", localpath)

    def agg(self, aggregation, keylist=None, excludekeys=None):
        """Aggregate the ensemble data into a VirtualRealization

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

        WARNING: CODE DUPLICATION from ensemble.py
        """
        quantilematcher = re.compile(r'p(\d\d)')
        supported_aggs = ['mean', 'median', 'min', 'max', 'std', 'var']
        if aggregation not in supported_aggs and \
           not quantilematcher.match(aggregation):
            raise ValueError("{arg} is not a".format(arg=aggregation) +
                             "supported ensemble aggregation")

        # Generate a new empty object:
        vreal = VirtualRealization(self._name + " " + aggregation)

        # Determine keys to use
        if isinstance(keylist, str):
            keylist = [keylist]
        if not keylist:  # Empty list means all keys.
            if not isinstance(excludekeys, list):
                excludekeys = [excludekeys]
            keys = set(self.data.keys()) - set(excludekeys)
        else:
            keys = keylist

        for key in keys:
            # Aggregate over this ensemble:
            # Ensure we operate on fully qualified localpath's
            key = self.shortcut2path(key)
            data = self.get_df(key).drop(columns='REAL')

            # Look for data we should group by. This would be beneficial
            # to get from a metadata file, and not by pure guesswork.
            groupbycolumncandidates = ['DATE', 'FIPNUM', 'ZONE', 'REGION',
                                       'JOBINDEX', 'Zone', 'Region_index']

            groupby = [x for x in groupbycolumncandidates
                       if x in data.columns]

            dtypes = data.dtypes.unique()
            if not (int in dtypes or float in dtypes):
                logger.info("No numerical data to aggregate in %s", key)
                continue
            if len(groupby):
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

            if groupby:
                aggregated.reset_index(inplace=True)

            vreal.append(key, aggregated)
        return vreal

    def append(self, key, dataframe, overwrite=False):
        """Append a dataframe to the internal datastore

        Incoming dataframe MUST have a column called 'REAL' which
        refers to the realization indices already known to the object.
        """
        if not isinstance(dataframe, pd.DataFrame):
            raise ValueError("Can only append dataframes")
        if 'REAL' not in dataframe.columns:
            raise ValueError("REAL column not in incoming dataframe")
        if key in self.data.keys() and not overwrite:
            logger.warning('Ignoring %s data already exists', key)
            return
        self.data[key] = dataframe

    def to_disk(self):
        """Dump all data to disk, in a retrieveable manner"""
        # Mature analogue function in VirtualRealization before commencing this
        raise NotImplementedError

    def load_disk(self, directory):
        """Load data from disk.

        Data must be written like to_disk() would have
        written it.
        """
        # Mature analogue function in VirtualRealization before commencing this
        raise NotImplementedError

    def __repr__(self):
        """Representation of the object"""
        return "<VirtualEnsemble, {}>".format(self._name)

    def get_df(self, localpath):
        """Access the internal datastore which contains dataframes or dicts

        Shorthand is allowed, if the fully qualified localpath is
            'share/results/volumes/simulator_volume_fipnum.csv'
        then you can also get this dataframe returned with these alternatives:
         * simulator_volume_fipnum
         * simulator_volume_fipnum.csv
         * share/results/volumes/simulator_volume_fipnum

        but only as long as there is no ambiguity. In case of ambiguity, a
        ValueError will be raised.

        Args:
            localpath: the idenfier of the data requested

        Returns:
            dataframe or dictionary
        """
        if localpath in self.data.keys():
            return self.data[localpath]

        # Allow shorthand, but check ambiguity
        basenames = [os.path.basename(x) for x in self.data.keys()]
        if basenames.count(localpath) == 1:
            shortcut2path = {os.path.basename(x): x for x in self.data.keys()}
            return self.data[shortcut2path[localpath]]
        noexts = [''.join(x.split('.')[:-1]) for x in self.data.keys()]
        if noexts.count(localpath) == 1:
            shortcut2path = {''.join(x.split('.')[:-1]): x
                             for x in self.data.keys()}
            return self.data[shortcut2path[localpath]]
        basenamenoexts = [''.join(os.path.basename(x).split('.')[:-1])
                          for x in self.data.keys()]
        if basenamenoexts.count(localpath) == 1:
            shortcut2path = {''.join(os.path.basename(x).split('.')[:-1]): x
                             for x in self.data.keys()}
            return self.data[shortcut2path[localpath]]
        raise ValueError(localpath)

    def get_smry(self, column_keys=None, time_index='monthly'):
        """
        Function analoguous to the EclSum direct get'ters in ScratchEnsemble,
        but here we have to resort to what we have internalized.

        This will perform interpolation in each realizations data to
        the requested time_index, this is done by creating VirtualRealization
        object for all realizations, which can do the interpolation, and
        the result is merged and returned. This creates some overhead, so
        if you do not need the interpolation, stick with get_df() instead.
        """

        # Get a list ala ['yearly', 'daily']
        available_smry = [x.split('/')[-1]
                          .replace('.csv', '')
                          .replace('unsmry--', '') for x in self.keys()
                          if 'unsmry' in x]

        if (isinstance(time_index, str) and time_index not in available_smry)\
           or isinstance(time_index, list):
            # Suboptimal code, we always pick the finest available
            # time resolution:
            priorities = ['raw', 'daily', 'monthly', 'weekly', 'yearly']
            # (could also sort them by number of rows, or we could
            #  even merge them all)
            # (could have priorities as a dict, for example so we
            #  can interpolate from monthly if we ask for yearly)
            chosen_smry = ''
            for candidate in priorities:
                if candidate in available_smry:
                    chosen_smry = candidate
                    break
            if not chosen_smry:
                logger.error("No internalized summary data "
                             + "to interpolate from")
                return pd.DataFrame()
        else:
            chosen_smry = time_index

        logger.info("Using " + chosen_smry + " for interpolation")

        # Explicit creation of VirtualRealization allows for later
        # multiprocessing of the interpolation.
        # We do not use the internal function get_realization() because
        # that copies all internalized data, while we only need
        # summary data.

        smry_path = 'unsmry--' + chosen_smry
        smry = self.get_df(smry_path)
        smry_interpolated = []
        for realidx in smry['REAL'].unique():
            vreal = VirtualRealization()
            # Inject the summary data for that specific realization
            vreal.append(smry_path, smry[smry['REAL'] == realidx])

            # Now ask the VirtualRealization to do interpolation
            interp = vreal.get_smry(column_keys, time_index)
            # Assume we get back a dataframe indexed by the dates from vreal
            # We must reset that index, and ensure the index column
            # gets a correct name
            interp.index = interp.index.set_names(['DATE'])
            interp = interp.reset_index()
            interp['REAL'] = realidx
            smry_interpolated.append(interp)
        concatenated = pd.concat(smry_interpolated, ignore_index=True)
        return(concatenated)

    def get_smry_stats(self, column_keys=None, time_index='monthly'):
        """
        Function to extract the ensemble statistics (Mean, Min, Max, P10, P90)
        for a set of simulation summary vectors (column key).

        Output format of the function is tailored towards webviz_fan_chart
        (data layout and column naming)

        In a virtual ensemble, this function can only provide data it has
        internalized. There is no resampling functionality yet.

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
        """
        # Obtain an aggregated dataframe for only the needed columns over
        # the entire ensemble. This will fail if we don't have the
        # time frequency already internalized.
        dframe = self.get_smry(time_index=time_index, column_keys=column_keys)

        data = {}  # dict to be returned
        for key in column_keys:
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

    @property
    def parameters(self):
        """Quick access to parameters"""
        return self.data['parameters.txt']

    @property
    def name(self):
        """The name of the virtual ensemble as set during initialization"""
        return self._name
