# -*- coding: utf-8 -*-
"""Testing fmu-ensemble."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os

from fmu import config
from fmu import ensemble

from fmu.ensemble import RealizationCombination
from fmu.ensemble import ScratchEnsemble

fmux = config.etc.Interaction()
logger = fmux.basiclogger(__name__)

if not fmux.testsetup():
    raise SystemExit()


def test_realizationcombination_basic():
    if '__file__' in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath('.')

    real0dir = os.path.join(testdir, 'data/testensemble-reek001',
                           'realization-0/iter-0')
    real0 = ensemble.ScratchRealization(real0dir)
    real0.from_smry(time_index='yearly', column_keys=['F*'])
    real1dir = os.path.join(testdir, 'data/testensemble-reek001',
                           'realization-1/iter-0')
    real1 = ensemble.ScratchRealization(real1dir)
    real1.from_smry(time_index='yearly', column_keys=['F*'])


    assert 'FWPR' in ((real0 - real1)['unsmry-yearly']).columns
    assert 'FWL' in ((real0 - real1)['parameters'])


def test_manual_aggregation():
    if '__file__' in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath('.')

    reekensemble = ScratchEnsemble('reektest',
                                   testdir +
                                   '/data/testensemble-reek001/' +
                                   'realization-*/iter-0')
    reekensemble.from_smry(time_index='yearly', column_keys=['F*'])
    reekensemble.from_csv('share/results/volumes/simulator_volume_fipnum.csv')

    mean = reekensemble.agg('mean')

    manualmean = 1/5 * (reekensemble[0] + reekensemble[1] + reekensemble[2]
                        + reekensemble[3] + reekensemble[4])

    assert mean['parameters']['RMS_SEED'] == \
        manualmean['parameters']['RMS_SEED']
