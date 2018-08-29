# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import shutil
import pandas as pd

from fmu import config

fmux = config.etc.Interaction()
logger = fmux.basiclogger(__name__)

if not fmux.testsetup():
    raise SystemExit()


class VirtualRealization(object):
    """A computed or archived realization.

    Computed or archived, one cannot assume to have access to the file
    system containing original data.

    Datatables that in a ScratchRealization was available through the
    files dataframe, is now available as dataframes in a dict accessed
    by the localpath in the files dataframe from ScratchRealization-

    """
    def __init__(self, description=None, data={}, longdescription=None):
        self._description = description
        self._longdescription = longdescription
        self.data = data

    def keys(self):
        return self.data.keys()

    def __getitem__(self, localpath):
        return self.get_df(localpath)

    def __delitem__(self, localpath):
        if localpath in self.keys():
            del self.data[localpath]

    def append(self, key, dataframe, overwrite=False):
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
            if isinstance(data, dict):
                with open(filename, 'w') as fhandle:
                    for paramkey in data.keys():
                        fhandle.write(paramkey + " " +
                                      str(data[paramkey]) + "\n")

    def from_disk(self, filesystempath):
        """
        Load data for a virtual realization from disk.

        Existing data in the current object will be wiped,
        this function is intended for initialization
        """
        raise NotImplementedError

    def to_json(self):
        """
        Dump realization data to json.

        Resulting json string is compatible with the
        accompanying from_json() function
        """
        raise NotImplementedError

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

    @property
    def parameters(self):
        return self.data['parameters.txt']

    @property
    def name(self):
        return self._description
