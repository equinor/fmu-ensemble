# -*- coding: utf-8 -*-

"""Top-level package for fmu.ensemble"""

from ._version import get_versions
__version__ = get_versions()['version']

del get_versions

from .ensemble import Ensemble  # noqa
