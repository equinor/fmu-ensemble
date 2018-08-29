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


def test_virtual_realization():

    if '__file__' in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath('.')

    realdir = os.path.join(testdir, 'data/testensemble-reek001',
                           'realization-0/iter-0')
    real = ensemble.ScratchRealization(realdir)

    # Check deepcopy(), first prove a bad situation
    vreal = real.to_virtual(deepcopy=False)
    assert 'parameters.txt' in real.keys()
    del vreal['parameters.txt']
    # This is a bad situation:
    assert 'parameters.txt' not in real.keys()

    # Now confirm that we can fix the bad
    # situation with the default to_virtual()
    real = ensemble.ScratchRealization(realdir)
    vreal = real.to_virtual()
    del vreal['parameters.txt']
    assert 'parameters.txt' in real.keys()

    real = ensemble.ScratchRealization(realdir)
    vreal = real.to_virtual()
    assert real.keys() == vreal.keys()
    
def test_virtual_todisk():
    if '__file__' in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath('.')

    realdir = os.path.join(testdir, 'data/testensemble-reek001',
                           'realization-0/iter-0')
    real = ensemble.ScratchRealization(realdir)
    real.from_smry(time_index='yearly', column_keys=['F*'])

    vreal = real.to_virtual()

    vreal.to_disk('virtreal', delete=True)
    assert os.path.exists('virtreal/parameters.txt')
    assert os.path.exists('virtreal/STATUS')
    assert os.path.exists('virtreal/share/results/tables/unsmry-yearly.csv')
