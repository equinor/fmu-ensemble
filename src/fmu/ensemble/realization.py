# -*- coding: utf-8 -*-
"""Implementation of realization classes

A realization is a set of results from one subsurface model
realization. A realization can be either defined from 
its output files from the FMU run on the file system,
it can be computed from other realizations, or it can be
an archived realization.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pandas as pd

from fmu import config

fmux = config.etc.Interaction()
logger = fmux.basiclogger(__name__)

if not fmux.testsetup():
    raise SystemExit()

class Realization():
    """Abstract representation of a realization

    Operations that are meaningful for both ScratchRealization
    and VirtualRealization
    """
    
class ScratchRealization(Realization):
    """A representation of results still present on disk

    ScratchRealization's point to the filesystem for their
    contents.
    """
    def __init__(self, path):
        self._origpath = path

        self.files = pd.DataFrame()
    
class VirtualRealization(Realization):
    """A computed or archived realization.

    Computed or archived, one cannot assume to have access to the file
    system containing original data.

    Datatables that in a ScratchRealization was available through the
    files dataframe, is now available as dataframes in a dict accessed
    by the localpath in the files dataframe from ScratchRealization-

    """
