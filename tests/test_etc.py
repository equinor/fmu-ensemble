"""Test a deprecated submodule"""

import pytest

from fmu import ensemble


def test_deprecated_etc():
    """The following lines was in documentation up to 1.4.0 inclusive.

    v1.4.1 and v1.4.2 miss support for fmu.ensemble.etc.Interaction()
    and then it is reinstated in v1.4.3. To be removed for fmu.ensemble v2.0.0
    """

    with pytest.warns(DeprecationWarning):
        fmux = ensemble.etc.Interaction()
    logger = fmux.basiclogger(__name__, level="WARNING")
    logger.info("testing deprecated code")

    # pylint: disable=import-outside-toplevel
    # Test different import:
    from fmu.ensemble import etc

    with pytest.warns(DeprecationWarning):
        fmux = etc.Interaction()
    logger = fmux.basiclogger(__name__, level="WARNING")
    logger.info("testing deprecated code")
