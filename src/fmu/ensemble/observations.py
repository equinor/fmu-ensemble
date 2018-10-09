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
from .virtualensemble import VirtualEnsemble
from .ensemblecombination import EnsembleCombination
from .observation_parser import observations_parser
from .realization import parse_number

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

    A observation unit is a concept for the observation and points to
    something we define as a "single" observation. It can be one value
    for one datatype at a specific date, but in the case of Eclipse
    summary vector, it can also be a time-series.

    The type of observations supported must follow the datatypes that
    the realizations and ensemble objects are able to internalize.
    """

    def __init__(self, **kwargs):
        # If yaml-file, load from that
        # If dict, copy in directory

        # Verify integrity, warn about unsuported observations
        # Drop unsupported observartions.

    def mismatch(ensemble_or_realization):
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

    def _realization_mistmatch(realizationobject):
        """..."""
