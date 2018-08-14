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


def test_single_realization():
    real = ensemble.ScratchRealization('data/testensemble-reek001/' +
                                       'realization-0/iter-0')
    assert len(real.files) == 3
    assert isinstance(real.parameters['RMS_SEED'], int)
    assert real.parameters['RMS_SEED'] == 422851785
    assert isinstance(real.parameters['MULTFLT_F1'], float)
