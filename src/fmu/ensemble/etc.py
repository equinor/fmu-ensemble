"""Deprecated module, will disappear in fmu-ensemble 2.0.0"""

import logging
import warnings


class Interaction:
    def __init__(self):
        warnings.warn(
            "fmu.ensemble.etc is deprecated and will be removed.", DeprecationWarning
        )
        pass

    def basiclogger(self, name, level=None):
        return logging.getLogger(name)
