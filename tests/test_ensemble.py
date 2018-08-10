# -*- coding: utf-8 -*-
"""Testing fmu-ensemble."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from fmu import config
from fmu import ensemble

fmux = config.etc.Interaction()
logger = fmux.basiclogger(__name__)

if not fmux.testsetup():
    raise SystemExit()


def test_ensemble_import():
    """Test basic behaviour, module import"""

    myensemble = ensemble.Ensemble()
    assert isinstance(myensemble, ensemble.Ensemble)
