# -*- coding: utf-8 -*-
"""Module containing a VirtualEnsemble class"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pandas as pd

from fmu import config
from fmu.ensemble.virtualrealization import VirtualRealization

fmux = config.etc.Interaction()
logger = fmux.basiclogger(__name__)

if not fmux.testsetup():
    raise SystemExit()


class VirtualEnsemble(object):
    """A computed or archived ensemble

    Computed or archived, one cannot assume to have access to the file
    system containing original data.

    Contrary to a ScratchEnsemble, a VirtualEnsemble stores aggregated
    dataframes. The column REAL signifies the realization index.
    """
    def __init__(self, name=None, data={}, longdescription=None):
        """
        Initialize a virtual ensemble.

        Typical use of this constructor is from ScratchEnsemble.to_virtual()
        Args:
            name: string, can be chosen freely
            data: dict with data to initialize with. Defaults to empty
            longdescription: string, free form multiline description.
        """
        self._name = name
        self._longdescription = longdescription

        # At ensemble level, this dictionary has dataframes only.
        # All dataframes have the column REAL.
        self.data = data

    def keys(self):
        """Return all keys in the internal datastore"""
        return self.data.keys()

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
        for key in self.keys():
            data = self.get_df(key)
            realizationdata = data[data['REAL'] == realindex]
            if not len(realizationdata):
                continue
            if len(realizationdata) == 1:
                # Convert scalar values to dictionaries, avoiding
                # getting length-one-series returned later on access.
                realizationdata = realizationdata.iloc[0].to_dict()
            else:
                realizationdata.reset_index(inplace=True, drop=True)
            del realizationdata['REAL']
            vreal.append(key, realizationdata)
        if len(vreal.keys()):
            return vreal
        else:
            raise ValueError("No data for realization %d" % realindex)

    def remove_realizations(self, realindices):
        """Remove realizations from internal data

        This will remove all rows in all internalized data belonging
        to the set of supplied indices.

        Args:
            realindices: int or list of ints, realization indices to remove
        """
        if not isinstance(realindices, list):
            realindices = [realindices]

        indicesknown = self.parameters['REAL'].unique()
        indicestodelete = list(set(realindices) & set(indicesknown))
        indicesnotknown = list(set(realindices) - set(indicestodelete))
        if indicesnotknown:
            logger.warn("Skipping undefined realization indices %s",
                        str(indicesnotknown))
        # There might be Pandas tricks to avoid this outer loop.
        for realindex in indicestodelete:
            for key in self.data:
                self.data[key] = self.data[key][self.data[key]['REAL']
                                                != realindex]
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

    def agg(self, aggregation, keylist=[]):
        """Aggregate the ensemble data into a VirtualRealization

        All data will be attempted aggregated. String data will typically
        be dropped in the result.

        An educated guess for groupby arguments wil be done for
        dataframes.

        Args:
            aggregation: string, among supported aggregation operators
                mean, p10, p90, min, max, median
        """
        raise NotImplementedError

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

    def from_disk(self, directory):
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

    @property
    def parameters(self):
        """Quick access to parameters"""
        return self.data['parameters.txt']

    @property
    def name(self):
        """The name of the virtual ensemble as set during initialization"""
        return self._name
