# -*- coding: utf-8 -*-
"""Contains the VirtualRealization class"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import shutil
import pandas as pd

from fmu import config

fmux = config.etc.Interaction()
logger = fmux.basiclogger(__name__)


class VirtualRealization(object):
    """A computed or archived realization.

    Computed or archived, one cannot assume to have access to the file
    system containing original data.

    Datatables that in a ScratchRealization was available through the
    files dataframe, is now available as dataframes in a dict accessed
    by the localpath in the files dataframe from ScratchRealization-

    """
    def __init__(self, description=None, data=None, longdescription=None):
        self._description = description
        self._longdescription = longdescription
        if data:
            self.data = data
        else:
            self.data = {}

    def keys(self):
        """Return the keys of all data in internal datastore"""
        return self.data.keys()

    def __getitem__(self, localpath):
        """Retrieve data for a specific key. Wrapper for get_df(),
        shorthands are allowed."""
        return self.get_df(localpath)

    def __delitem__(self, localpath):
        """Delete a key from the internal datastore. The key must be fully
        qualified, no shorthands."""
        if localpath in self.keys():
            del self.data[localpath]

    def append(self, key, dataframe, overwrite=False):
        """Append data to the datastore.

        No checks performed on the dataframe coming in. If key exists,
        nothing will be appended unless overwrite is set to True
        """
        if key in self.data.keys() and not overwrite:
            logger.warning('Ignoring %s, data already exists', key)
            return
        self.data[key] = dataframe

    def __repr__(self):
        """Represent the realization. Show only the last part of the path"""
        return "<VirtualRealization, {}>".format(self._description)

    def to_disk(self, filesystempath, delete=False):
        """Write the virtual realization to the filesystem.

        All data will be dumped to the requested directory according
        to their localpaths (keys).

        Args:
            filesystempath : string with a directory, absolute or
                relative. If it exists already, it must be empty,
                otherwise we give up.
        """
        if os.path.exists(filesystempath):
            if delete:
                shutil.rmtree(filesystempath)
                os.mkdir(filesystempath)
            else:
                if os.listdir(filesystempath):
                    logger.critical("Refusing to write to non-empty directory")
                    raise IOError("Directory %s not empty" % filesystempath)
        else:
            os.mkdir(filesystempath)

        with open(os.path.join(filesystempath, '_description'),
                  'w') as fhandle:
            fhandle.write(self._description)
        if self._longdescription:
            with open(os.path.join(filesystempath, '_longdescription'),
                      'w') as fhandle:
                fhandle.write(str(self._longdescription))
        with open(os.path.join(filesystempath, '__repr__'), 'w') as fhandle:
            fhandle.write(self.__repr__())

        for key in self.keys():
            dirname = os.path.join(filesystempath, os.path.dirname(key))
            if dirname:
                if not os.path.exists(dirname):
                    os.makedirs(dirname)

            data = self.get_df(key)
            filename = os.path.join(dirname, os.path.basename(key))
            if isinstance(data, pd.DataFrame):
                logger.info("Dumping %s", key)
                data.to_csv(filename, index=False)
            elif isinstance(data, dict):
                with open(filename, 'w') as fhandle:
                    for paramkey in data.keys():
                        fhandle.write(paramkey + " " +
                                      str(data[paramkey]) + "\n")
            elif (isinstance(data, str) or isinstance(data, float) or
                  isinstance(data, int)):
                with open(filename, 'w') as fhandle:
                    fhandle.write(str(data))
            else:
                logger.warning("Don't know how to dump %s " +
                               "of type %s to disk", key, type(key))

    def load_disk(self, filesystempath):
        """Load data for a virtual realization from disk.

        Existing data in the current object will be wiped,
        this function is intended for initialization

        WARNING: This code is really shaky. We need metafiles written
        by to_json() for robust parsing of files on disk, f.ex. are
        txt files really key-value data (dicts) or csv files?

        Currently, the file format is guessed based on the contents
        of the two first lines:
        * CSV files contains commas, and more than one line
        * key-value files contains two space-separated values, and at least one line
        * scalar files contain only one item and one line

        Args:
            filesystempath: path to a directory that to_disk() has
                written to (or a really careful user)
        """
        logger.info("Loading virtual realization from %s", filesystempath)
        for root, _, filenames in os.walk(filesystempath):
            for filename in filenames:
                if filename == '_description':
                    self._description = ' '.join(open(os.path.join(
                        root, filename)).readlines())
                    logger.info('got name as %s', self._description)
                elif filename == 'STATUS':
                    self.append('STATUS', pd.read_csv(os.path.join(root,
                                                                   filename)))
                    logger.info('got STATUS')
                elif filename == '__repr__':
                    # Not implemented..
                    continue
                else:
                    # GUESS scalar, key-value txt or CSV from the first
                    # two lines. SHAKY!
                    commafields = 0
                    spacefields = 0
                    linecount = 0
                    with open(os.path.join(root, filename)) as realfile:
                        line1 = realfile.next()
                        commafields = len(line1.split(','))
                        spacefields = len(line1.split())
                        try:
                            realfile.next()
                            linecount = 2
                        except StopIteration:
                            linecount = 1
                    print(filename, commafields, spacefields, linecount)
                    if spacefields == 2 and commafields == 1:
                        # key-value txt file!
                        self.append(filename,
                                    pd.read_table(os.path.join(root, filename),
                                                  sep=r'\s+', index_col=0,
                                                  header=None)[1].to_dict())
                        logger.info('Read txt file %s', filename)
                    elif (spacefields == 1 and linecount == 1 and
                          commafields == 1):
                        # scalar file
                        value = pd.read_table(os.path.join(root, filename),
                                              header=None,
                                              engine='python').iloc[0, 0]
                        logger.info('Read scalar file %s', filename)
                        self.append(filename, value)
                    elif (spacefields == 1 and linecount > 1 and
                          commafields > 1):
                        # CSV file!
                        self.append(filename,
                                    pd.read_csv(os.path.join(root, filename)))
                        logger.info('Read csv file %s', filename)

    def to_json(self):
        """
        Dump realization data to json.

        Resulting json string is compatible with the
        accompanying load_json() function
        """
        raise NotImplementedError

    def get_df(self, localpath):
        """Access the internal datastore which contains dataframes, dicts
        or scalars.

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
        data = None
        if localpath in self.keys():
            data = self.data[localpath]
        fullpath = self.shortcut2path(localpath)

        if fullpath in self.keys():
            data = self.data[fullpath]
        else:
            raise ValueError("Could not find {}".format(localpath))

        if isinstance(data, pd.DataFrame):
            return data
        elif isinstance(data, pd.Series):
            return data.to_dict()
        elif isinstance(data, dict):
            return data
        elif (isinstance(data, str) or isinstance(data, int) or
              isinstance(data, float)):
            return data
        else:
            raise ValueError("BUG: Unknown datatype")

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
        """
        basenames = [os.path.basename(x) for x in self.keys()]
        if basenames.count(shortpath) == 1:
            shortcut2path = {os.path.basename(x): x for x in self.keys()}
            return shortcut2path[shortpath]
        noexts = [''.join(x.split('.')[:-1]) for x in self.keys()]
        if noexts.count(shortpath) == 1:
            shortcut2path = {''.join(x.split('.')[:-1]): x
                             for x in self.keys()}
            return shortcut2path[shortpath]
        basenamenoexts = [''.join(os.path.basename(x).split('.')[:-1])
                          for x in self.keys()]
        if basenamenoexts.count(shortpath) == 1:
            shortcut2path = {''.join(os.path.basename(x).split('.')[:-1]): x
                             for x in self.keys()}
            return shortcut2path[shortpath]
        # If we get here, we did not find anything that
        # this shorthand could point to. Return as is, and let the
        # calling function handle further errors.
        return shortpath

    def get_smry(self, column_keys=None, time_index=None):
        """Analog function to get_smry() in ScratchRealization

        Accesses the internalized summary data and performs
        interpolation if needed.

        Returns data for those columns that are known, unknown
        columns will be issued a warning for.

        Args:
            column_keys: str or list of str with column names,
                may contain wildcards (glob-style)
            time_index: str or list of datetimes
        """
        column_keys = self._glob_smry_keys(column_keys)
        if not column_keys:
            raise ValueError("No column keys found")
        if not time_index:
            time_index = 'monthly'
        if isinstance(time_index, 'str'):
            time_index = self._get_smry_dates(time_index)

        # Determine which of the internalized dataframes we should use
        # for interpolation. Or, should we merge some of them for even
        # higher accuracy?

        # If asked for yearly, look for yearly, monthly, daily, and then for raw.
        # If asked for monthly, look for monthly, daily, raw, yearly
        # If asked for weekly, look for weekly, daily, raw, monthly, yearly
        # If asked for list of datetimes, look for raw, daily, weekly, monthly, yearly.
        # We assume that smry dataframes with custom datetime list is never
        # internalized, so we do not need to look for that

        smry_source = self.get_df('unsmry-monthly')

        smry_source.set_index('DATE', inplace=True)

        # Add time_index to smry_soruce.index
        # Sort smry_source.index

        # Establish list of cumulative columns

        # Linear-in-Time-interpolation for cumulative columns
        # bfill for other columns
        # Check if we need ffill at end for non-cum columns
        # Check if we need bfill at start or ffill at end for cum columns
        # Slice out the time-indices that was requested and return

    def smry_cumulative(self, column_keys):
        """Determine whether smry vectors are cumulative

        Returns list of booleans, indicating whether a certain
        column_key in summary dataframes corresponds to a cumulative
        column.

        The current implementation checks for the letter 'T' in the
        column key, but this behaviour is not guaranteed in the
        future, in case the cumulative information gets internalized

        Warning: This code is duplicated in realization.py, even though
        a ScratchRealization has access to the EclSum object which can
        give the true answer

        Args:
            column_keys: str or list of strings with summary vector
                names
        Returns:
            list of booleans, corresponding to each inputted
                summary vector name.
        """
        if isinstance(column_keys, str):
            column_keys = [column_keys]
        if not isinstance(column_keys, list):
            raise TypeError("column_keys must be str or list of str")
        return [(x.endswith('T') and ':' not in x and 'CT' not in x)
                or ('T:' in x and 'CT:' not in x) for x in column_keys]

    @property
    def parameters(self):
        """Convenience getter for parameters.txt"""
        return self.data['parameters.txt']

    @property
    def name(self):
        """Return name of ensemble"""
        return self._description
