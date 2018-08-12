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
        if type(paths) == str:
            paths = [paths]
       
        # Glob incoming paths to determine 
        # paths for each realization (flatten and uniqify)
        globbedpaths = [glob.glob(path) for path in paths]
        globbedpaths = list(set([item for sublist in globbedpaths
                                 for item in sublist]))
        print(globbedpaths)
        # Search and locate minimal set of files
        # representing the realizations.
        #self.files = self.find_files(paths)
    
        # Store list of integers, realization indices
        #self.reals = self.files['REAL'].unique().sort_values()

        logger.debug('Ran __init__')

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
