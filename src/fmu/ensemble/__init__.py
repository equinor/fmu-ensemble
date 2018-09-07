# -*- coding: utf-8 -*-

"""Top-level package for fmu.ensemble"""

from ._version import get_versions
__version__ = get_versions()['version']

del get_versions

from .ensemble import ScratchEnsemble  # noqa
from .realization import ScratchRealization  # noqa
from .ensembleset import EnsembleSet  # noqa
from .virtualrealization import VirtualRealization  # noqa
from .virtualensemble import VirtualEnsemble  # noqa
from .ensemblecombination import EnsembleCombination  # noqa
