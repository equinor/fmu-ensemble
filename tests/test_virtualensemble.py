# -*- coding: utf-8 -*-
"""Testing fmu-ensemble."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import datetime
import pytest
import shutil
import pandas as pd
import ert.ecl

from fmu import config
from fmu import ensemble

fmux = config.etc.Interaction()
logger = fmux.basiclogger(__name__)

if not fmux.testsetup():
    raise SystemExit()


def test_virtualensemble():

    if '__file__' in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath('.')

    reekensemble = ensemble.ScratchEnsemble('reektest',
                                            testdir +
                                            '/data/testensemble-reek001/' +
                                            'realization-*/iter-0')
    reekensemble.from_smry(time_index='yearly', column_keys=['F*'])
    vens = reekensemble.to_virtual()

    # Check that we have data for 5 realizations
    assert len(vens['unsmry-yearly']['REAL'].unique()) == 5
    assert len(vens['parameters.txt']) == 5
    
