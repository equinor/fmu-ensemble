# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import re
import glob
import json
import shutil
import numpy
import pandas as pd

import ert.ecl
from fmu import config

fmux = config.etc.Interaction()
logger = fmux.basiclogger(__name__)

if not fmux.testsetup():
    raise SystemExit()

class VirtualEnsemble(object):
    """A computed or archived ensemble

    Computed or archived, one cannot assume to have access to the file
    system containing original data.

    Compared to a ScratchEnsemble, a VirtualEnsemble has dataframes stored
    similarly to VirtualRealization, but with REAL added as a column
    in all dataframes.
    """
    def __init__(self, name=None, data={}, longdescription=None):
        self._name = name
        self._longdescription = longdescription
        self.data = data


    def keys(self):
        return self.data.keys()

    def __getitem__(self, localpath):
        return self.get_df(localpath)

    def remove_realizations(self, realindices):
        raise NotImplementedError

    def remove_data(self, localpaths):
        raise NotImplementedError

    def agg(self, aggregation, keylist=[]):
        raise NotImplementedError

    def append(self, key, dataframe, overwrite=False):
        if key in self.data.keys() and not overwrite:
            logger.warning('Ignoring ' + key + ', data already exists')
            return
        self.data[key] = dataframe

    def to_disk(self):
        # Mature analogue function in VirtualRealization before commencing this
        raise NotImplementedError

    def from_disk(self):
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
        

    @property
    def parameters(self):
        return self.data['parameters.txt']

    @property
    def name(self):
        return self._description
