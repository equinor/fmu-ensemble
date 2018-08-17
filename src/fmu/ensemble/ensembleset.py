# -*- coding: utf-8 -*-
"""Module for book-keeping and aggregation of ensembles
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import glob
import pandas as pd

from fmu.config import etc
from .realization import ScratchRealization

xfmu = etc.Interaction()
logger = xfmu.functionlogger(__name__)


class EnsembleSet(object):
    """An ensemble set is any collection of ensembles.

    Ensembles might be both ScratchEnsembles or VirtualEnsembles.
    """

    def __init__(self, ensembleset_name, paths=None):
        """Initiate an ensemble set

        For convencience, it is possible to initiate with paths to the
        file system, which will amount to the same as generating
        ScratchEnsembles for each ensemble.
        """
        self._ensembles = {}  # Dictionary indexed by each ensemble's name.

        if isinstance(paths, str):
            paths = [paths]
        if paths:
            # Recognize realization-*/iter-* and use iter-* to set ensemble
            # names for each ScratchEnsemble
            raise NotImplementedError

    def add_ensemble(self, ensembleobject):
        self._ensembles[ensembleobject.name] = ensemble
