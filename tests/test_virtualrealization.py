# -*- coding: utf-8 -*-
"""Testing fmu-ensemble."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pytest
import pandas as pd
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

    # Test appending a random dictionary
    vreal.append('betteroutput', {'NPV': 200000000, 'BREAKEVEN': 8.4})
    assert vreal['betteroutput']['NPV'] > 0
    # Appending to a key that exists should not help
    vreal.append('betteroutput', {'NPV': -300, 'BREAKEVEN': 300})
    assert vreal['betteroutput']['NPV'] > 0
    # Unless we overwrite explicitly:
    vreal.append('betteroutput', {'NPV': -300, 'BREAKEVEN': 300},
                 overwrite=True)
    assert vreal['betteroutput']['NPV'] < 0

    with pytest.raises(ValueError):
        vreal.get_df('bogusdataname')


def test_virtual_todisk():
    if '__file__' in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath('.')

    realdir = os.path.join(testdir, 'data/testensemble-reek001',
                           'realization-0/iter-0')
    real = ensemble.ScratchRealization(realdir)
    real.load_smry(time_index='yearly', column_keys=['F*'])
    real.load_scalar('npv.txt')

    vreal = real.to_virtual()
    assert 'npv.txt' in vreal.keys()

    with pytest.raises(IOError):
        vreal.to_disk('.')

    vreal.to_disk('virtreal1', delete=True)
    assert os.path.exists('virtreal1/parameters.txt')
    assert os.path.exists('virtreal1/STATUS')
    assert os.path.exists('virtreal1/share/results/tables/unsmry-yearly.csv')
    assert os.path.exists('virtreal1/npv.txt')


def test_virtual_fromdisk():
    if '__file__' in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath('.')

    realdir = os.path.join(testdir, 'data/testensemble-reek001',
                           'realization-0/iter-0')
    real = ensemble.ScratchRealization(realdir)
    real.load_smry(time_index='yearly', column_keys=['F*'])
    real.load_scalar('npv.txt')
    real.to_virtual().to_disk('virtreal2', delete=True)
    #
    vreal = ensemble.VirtualRealization('foo')
    vreal.load_disk('virtreal2')

    for key in vreal.keys():
        if isinstance(real.get_df(key), pd.DataFrame) or \
           isinstance(real.get_df(key), dict):
            assert len(real.get_df(key)) == len(vreal.get_df(key))
        else:  # Scalars:
            assert real.get_df(key) == vreal.get_df(key)
    assert real.get_df('parameters')['FWL'] == \
        vreal.get_df('parameters')['FWL']
    assert real.get_df('unsmry-yearly').iloc[-1]['FGIP'] == \
        vreal.get_df('unsmry-yearly').iloc[-1]['FGIP']
    assert real.get_df('npv.txt') == 3444
