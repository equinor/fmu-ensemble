# -*- coding: utf-8 -*-
"""Module for parsing an ensemble for FMU.

This module will be ... (text to come).
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import re
import glob
import pandas as pd

from fmu.config import etc

xfmu = etc.Interaction()
logger = xfmu.functionlogger(__name__)


class Ensemble(object):
    """An ensemble is a collection of Realizations.

    Ensembles are initialized from path(s) pointing to
    filesystem locations containing realizations.

    Ensemble objects can be grouped into EnsembleSet.

    Realizations in an ensembles are uniquely determined
    by their realization index (integer).
    """

    def __init__(self, ensemble_name, paths):
        self._name = ensemble_name  # ensemble name
        self.files = pd.DataFrame()  # list of files representing the ensemble

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
        self.add_realizations(paths)

        logger.debug('Ran __init__')

    def add_realizations(self, paths):
        """Utility function to add realization to the ensemble.

        The ensemble realizations are defined from the content of the
        object dataframe 'files'. As a minimum, a realization must
        have a STATUS file. Additionally, jobs.json, and
        parameters.txt will be parsed.

        A realization is *uniquely* determined by its realization index,
        put into the column 'REAL' in the files dataframe

        This function can be used to reload ensembles.
        Existing realizations will have their data removed
        from the files dataframe.

        Columns added to the files dataframe:
         * REAL realization index.
         * FULLPATH absolute path to the file
         * FILETYPE filename extension (after last dot)
         * LOCALPATH relative filename inside realization directory
         * BASENAME filename only. No path. Includes extension.

        """
        # This df will be appended to self.files at the end:
        files = pd.DataFrame(columns=['REAL', 'FULLPATH', 'FILETYPE',
                                      'LOCALPATH', 'BASENAME'])
        if isinstance(paths, list):
            globbedpaths = [glob.glob(path) for path in paths]
            # Flatten list and uniquify:
            globbedpaths = list(set([item for sublist in globbedpaths
                                     for item in sublist]))
        else:
            globbedpaths = glob.glob(paths)

        realregex = re.compile(r'.*realization-(\d*)')
        for realdir in globbedpaths:
            # Support initialization using relative paths
            absrealdir = os.path.abspath(realdir)
            logger.info("Processing realization directory %s...",
                        absrealdir)
            realidxmatch = re.match(realregex, absrealdir)
            if realidxmatch:
                realidx = int(realidxmatch.group(1))
            else:
                xfmu.warn('Realization %s not valid, skipping' %
                          absrealdir)
                continue
            if os.path.exists(os.path.join(realdir, 'STATUS')):
                files = files.append({'REAL': realidx,
                                      'LOCALPATH': 'STATUS',
                                      'FILETYPE': 'STATUS',
                                      'FULLPATH': os.path.join(absrealdir,
                                                               'STATUS')},
                                     ignore_index=True)
            else:
                logger.warn("Invalid realization, no STATUS file, %s",
                      realdir)
            if os.path.exists(os.path.join(realdir, 'jobs.json')):
                files = files.append({'REAL': realidx,
                                      'LOCALPATH': 'jobs.json',
                                      'FILETYPE': 'json',
                                      'FULLPATH': os.path.join(absrealdir,
                                                               'jobs.json')},
                                     ignore_index=True)

            if os.path.exists(os.path.join(realdir, 'parameters.txt')):
                files = files.append({'REAL': realidx,
                                    'LOCALPATH': 'parameters.txt',
                                      'FILETYPE': 'txt',
                                      'FULLPATH': os.path.join(absrealdir,
                                                               'parameters.txt')},
                                    ignore_index=True)
        logger.info('add_realization() found %d realizations',
                    len(files.REAL.unique()))
        self.files = self.files.append(files, ignore_index=True)

    def __len__(self):
        return len(self.files.REAL.unique())

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

    def dummyfunction(self, basefolder='myensemble',
                      resultfolder='share/results'):
        """Do something with an ensemble.

        Args:
            basefolder (str): Base folder path for an ensemble
            resultfolder: Relative path to results to search for.

        Raises:
            ValueError: If basefolder does not exist

        Example::

            base = '/scratch/ola/dunk'
            myensemble.dummyfunction(basefolder=base)
        """
        pass
