# -*- coding: utf-8 -*-
"""Testing fmu-ensemble."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pandas as pd
import ert.ecl

from fmu import config
from fmu import ensemble

fmux = config.etc.Interaction()
logger = fmux.basiclogger(__name__)

if not fmux.testsetup():
    raise SystemExit()


def test_single_realization():

    testdir = os.path.dirname(os.path.abspath(__file__))
    realdir = os.path.join(testdir, 'data/testensemble-reek001',
                           'realization-0/iter-0')
    real = ensemble.ScratchRealization(realdir)
    assert len(real.files) == 3
    assert isinstance(real.parameters['RMS_SEED'], int)
    assert real.parameters['RMS_SEED'] == 422851785
    assert isinstance(real.parameters['MULTFLT_F1'], float)
    assert isinstance(real.get_parameters(convert_numeric=False)['RMS_SEED'],
                      str)

    # Eclipse summary files:
    assert isinstance(real.get_eclsum(), ert.ecl.EclSum)
    assert real.get_smryvalues('FOPT')['FOPT'].max() > 6000000

    # STATUS file
    assert isinstance(real.get_status(), pd.DataFrame)
    assert len(real.get_status())
    assert 'ECLIPSE' in real.get_status().loc[49, 'FORWARD_MODEL']
    assert int(real.get_status().loc[49, 'DURATION']) == 141
