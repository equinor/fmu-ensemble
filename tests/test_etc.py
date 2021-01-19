"""Test a deprecated submodule"""

from fmu import ensemble
from fmu.ensemble import etc

import pytest


def test_deprecated_etc():
    """The following lines was in documentation up to 1.4.0 inclusive

    The should just not crash, the logging configuration does not have
    to work."""
    with pytest.warns(DeprecationWarning):
        fmux = ensemble.etc.Interaction()
    logger = fmux.basiclogger(__name__, level="WARNING")
    logger.info("testing deprecated code")

    with pytest.warns(DeprecationWarning):
        fmux = etc.Interaction()
    logger = fmux.basiclogger(__name__, level="WARNING")
    logger.info("testing deprecated code")
