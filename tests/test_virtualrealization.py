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


def test_virtual_todisk(tmp='TMP'):
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

    if not os.path.exists(tmp):
        os.mkdir(tmp)
    print(os.path.join(tmp, 'virtreal1'))
    vreal.to_disk(os.path.join(tmp, 'virtreal1'), delete=True)
    assert os.path.exists(os.path.join(tmp, 'virtreal1/parameters.txt'))
    assert os.path.exists(os.path.join(tmp, 'virtreal1/STATUS'))
    assert os.path.exists(os.path.join(tmp,
                                       'virtreal1/share/results/' +
                                       'tables/unsmry-yearly.csv'))
    assert os.path.exists(os.path.join(tmp, 'virtreal1/npv.txt'))


def test_virtual_fromdisk(tmp='TMP'):
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
    if not os.path.exists(tmp):
        os.mkdir(tmp)
    real.to_virtual().to_disk(os.path.join(tmp, 'virtreal2'), delete=True)
    #
    vreal = ensemble.VirtualRealization('foo')
    vreal.load_disk(os.path.join(tmp, 'virtreal2'))

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


def test_get_smry():
    if '__file__' in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath('.')

    realdir = os.path.join(testdir, 'data/testensemble-reek001',
                           'realization-0/iter-0')
    real = ensemble.ScratchRealization(realdir)
    real.load_smry(time_index='yearly', column_keys=['F*'])
    vreal = real.to_virtual()
    monthly = vreal.get_smry(column_keys=['FOPT', 'FOPR', 'FGPR', 'FWCT'],
                             time_index='monthly')
    assert 'FOPT' in monthly.columns
    assert len(monthly) > 20
    assert 'FOPR' in monthly.columns
    assert len(monthly) == len(monthly.dropna())

    assert len(vreal.get_smry(column_keys='FOPT',
                              time_index='yearly').columns) == 1


def test_get_smry_cumulative():
    """Test the cumulative boolean function"""

    vreal = ensemble.VirtualRealization()

    assert isinstance(vreal._smry_cumulative([]), list)
    with pytest.raises(TypeError):
        vreal._smry_cumulative({})
    with pytest.raises(TypeError):
        vreal._smry_cumulative()
    assert vreal._smry_cumulative(['FOPT'])[0]
    assert not vreal._smry_cumulative(['FOPR'])[0]

    assert not vreal._smry_cumulative(['FWCT'])[0]
    assert vreal._smry_cumulative(['WOPT:A-1'])[0]
    assert not vreal._smry_cumulative(['WOPR:A-1T'])[0]


def test_get_smry_dates():
    """Test date grid functionality from a virtual realization.

    Already internalized summary data is needed for this"""

    # First test with no data:
    empty_vreal = ensemble.VirtualRealization()
    with pytest.raises(ValueError):
        empty_vreal._get_smry_dates()

    if '__file__' in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath('.')

    realdir = os.path.join(testdir, 'data/testensemble-reek001',
                           'realization-0/iter-0')
    real = ensemble.ScratchRealization(realdir)
    real.load_smry(time_index='yearly', column_keys=['F*', 'W*'])
    vreal = real.to_virtual()

    assert len(vreal._get_smry_dates(freq='monthly')) == 49
    assert len(vreal._get_smry_dates(freq='daily')) == 1462
    assert len(vreal._get_smry_dates(freq='yearly')) == 5

    with pytest.raises(ValueError):
        assert vreal._get_smry_dates(freq='foobar')


def test_glob_smry_keys():
    """Test the globbing function for virtual realization"""
    empty_vreal = ensemble.VirtualRealization()
    with pytest.raises(ValueError):
        empty_vreal._glob_smry_keys('FOP*')

    if '__file__' in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath('.')

    realdir = os.path.join(testdir, 'data/testensemble-reek001',
                           'realization-0/iter-0')
    real = ensemble.ScratchRealization(realdir)
    real.load_smry(time_index='yearly', column_keys=['F*', 'W*'])
    vreal = real.to_virtual()

    assert len(vreal._glob_smry_keys('FOP*')) == 9
    assert len(vreal._glob_smry_keys(['FOP*'])) == 9

    assert len(vreal._glob_smry_keys('WOPT:*')) == 8
    assert all([x.startswith('WOPT:')
                for x in vreal._glob_smry_keys('WOPT:*')])

    assert not vreal._glob_smry_keys('FOOBAR')
