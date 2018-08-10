# -*- coding: utf-8 -*-

"""Top-level package for fmu_config"""

from ._version import get_versions
__version__ = get_versions()['version']

del get_versions

from .configparserfmu import ConfigParserFMU  # noqa
