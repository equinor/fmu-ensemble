# -*- coding: utf-8 -*-
"""Module for parsing an ensemble for FMU.

This module will be ... (text to come).
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


from fmu.config import etc

xfmu = etc.Interaction()
logger = xfmu.functionlogger(__name__)


class Ensemble(object):
    """Class for Ensemble, more text to come."""

    def __init__(self):
        self._name = None  # ensemble name
        self._basefolder = None
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
