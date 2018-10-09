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

import warnings
import numpy as np
from collections import defaultdict
import pandas as pd
from ecl import EclDataType
from ecl.eclfile import EclKW

from fmu.config import etc
from .realization import ScratchRealization
from .virtualrealization import VirtualRealization

xfmu = etc.Interaction()
logger = xfmu.functionlogger(__name__)


class Observations(object):
    """Represents a set of observations and the ability to
    compare realizations and ensembles to the observations

    The primary data structure is a dictionary holding actual
    observations, this can typically be loaded from a yaml-file

    Key functionality is to be able to compute mismatch pr
    observation and presenting the computed data as a Pandas
    Dataframe. If run on ensembles, every row will be tagged
    by which realization index the data was computed for.

    An observation unit is a concept for the observation and points to
    something we define as a "single" observation. It can be one value
    for one datatype at a specific date, but in the case of Eclipse
    summary vector, it can also be a time-series.

    The type of observations supported must follow the datatypes that
    the realizations and ensemble objects are able to internalize.
    """

    # Discussion points:
    # * Should mismatch calculation happen in this function
    #   with ensembles/realizations input or the other way around?
    # * Should it be possible to represent the observations
    #   themselves in a dataframe, or does the dict suffice?
    #   (each observation unit should be representable as
    #   a dict, and then it is mergeable in Pandas)

    def __init__(self, observations):
        """Initialize an observation object with observations.
        
        Args:
            observations: dict with observation structure or string
                with path to a yaml file.
        """
        self.observations = dict()

        if isinstance(observations, str):
            with open(observations) as yamlfile:
                self.observations = yaml.load(yamlfile)
        elif isinstance(observations, dict):
            self.observations = observations
        else:
            raise ValueError("Unsupported object for observations")
        
        if not self._clean_observations():
            raise ValueError("No usable observations")
        
    def mismatch(ens_or_real):
        """Compute the mismatch from the current observation set
        to the incoming ensemble or realization.

        In the case of an ensemble, it will pass on the task to
        every realization, and aggregate the results

        Returns:
            dataframe with REAL (only if ensemble), OBSKEY, DATE,
                L1, L2.
        """
        # For ensembles, we should in the future be able to loop
        # over realizations in a multiprocessing fashion
        if isinstance(ens_or_real, VirtualEnsemble) or 
            isinstance(ens_or_real, ScratchEnsemble):
            mismatches = dict()
            for realidx, real in ens_or_real.realizations:
                mismatches[realidx] = _realization_mismatch(real)
                mismatches[realidx]['REAL'] = realidx
            return pd.DataFrame(mismatches)
        elif isinstance(ens_or_real, VirtualRealization) or
            isinstance(ens_or_real, ScratchRealization):
            return _realization_mismatch(ens_or_real)
        elif isinstance(ens_or_real, EnsembleSet):
            pass 
        else:
            raise ValueError("Unsupported object for mismatch calculation")

    def _realization_mismatch(realizationobject):
        """Compute the mismatch from the current
        loaded observations to the a realization
        
        Supports both ScratchRealizations and
        VirtualRealizations
       
        The returned dataframe contains the columns:
            * OBSKEY - name of the observation key
            * DATE - only where relevant.
            * OBSINDEX - where an enumeration is relevant
            * MISMATCH - signed difference between value and result
            * L1 - absolute difference
            * L2 - absolute difference squared
            * SIGN - True if positive difference

        Returns:
            dataframe: One row per observation unit with
                mismatch data
        """
        mismatches = pd.DataFrame(columns=['OBSKEY',
            'DATE', 'OBSINDEX', 'MISMATCH', 'L1', 'L2', 'SIGN'])
        return mismatches
  
    def _clean_observations(self):
        """Verify integrity of observations, remove
        observation units that cannot be used.

        Will log warnings about things that are removed. 
        
        Returns number of usable observation units.
        """
        return 1 
