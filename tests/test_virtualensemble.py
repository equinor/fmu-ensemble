# -*- coding: utf-8 -*-
"""Testing fmu-ensemble."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pandas as pd
import pytest

from fmu import config
from fmu.ensemble import ScratchEnsemble

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

    reekensemble = ScratchEnsemble('reektest',
                                   testdir +
                                   '/data/testensemble-reek001/' +
                                   'realization-*/iter-0')
    reekensemble.from_smry(time_index='yearly', column_keys=['F*'])
    reekensemble.from_scalar('npv.txt')
    reekensemble.from_txt('outputs.txt')
    vens = reekensemble.to_virtual()

    # Check that we have data for 5 realizations
    assert len(vens['unsmry-yearly']['REAL'].unique()) == 5
    assert len(vens['parameters.txt']) == 5

    assert 'REAL' in vens['STATUS'].columns

    # Check shorthand functionality:
    assert vens.shortcut2path('unsmry-yearly') == \
        'share/results/tables/unsmry-yearly.csv'
    assert vens.shortcut2path('unsmry-yearly.csv') == \
        'share/results/tables/unsmry-yearly.csv'

    assert 'npv.txt' in vens.keys()
    assert len(vens['npv.txt']) == 5  # includes the 'error!' string in real4
    assert 'outputs.txt' in vens.keys()
    assert len(vens['outputs.txt']) == 4

    # Check that get_smry() works
    fopt = vens.get_smry(column_keys=['FOPT'], time_index='yearly')
    assert 'FOPT' in fopt.columns
    assert 'DATE' in fopt.columns
    assert 'REAL' in fopt.columns
    assert 'FGPT' not in fopt.columns
    assert len(fopt) == 25

    # Eclipse summary vector statistics for a given ensemble
    df_stats = vens.get_smry_stats(column_keys=['FOPR', 'FGPR'],
                                   time_index='yearly')
    assert isinstance(df_stats, dict)
    assert len(df_stats.keys()) == 2
    assert isinstance(df_stats['FOPR'], pd.DataFrame)
    assert len(df_stats['FOPR'].index) == 5

    # Check webviz requirements for dataframe
    assert 'min' in df_stats['FOPR'].columns
    assert 'max' in df_stats['FOPR'].columns
    assert 'name' in df_stats['FOPR'].columns
    assert df_stats['FOPR']['name'].unique() == 'FOPR'
    assert 'index' in df_stats['FOPR'].columns  # This is DATE (!)
    assert 'mean' in df_stats['FOPR'].columns
    assert 'p10' in df_stats['FOPR'].columns
    assert 'p90' in df_stats['FOPR'].columns
    assert df_stats['FOPR']['min'].iloc[-2] < \
        df_stats['FOPR']['max'].iloc[-2]

    # Test virtrealization retrieval:
    vreal = vens.get_realization(2)
    assert vreal.keys() == vens.keys()

    # Test realization removal:
    vens.remove_realizations(3)
    assert len(vens.parameters['REAL'].unique()) == 4
    vens.remove_realizations(3)  # This will give warning
    assert len(vens.parameters['REAL'].unique()) == 4
    assert len(vens['unsmry-yearly']['REAL'].unique()) == 4

    # Test data removal:
    vens.remove_data('parameters.txt')
    assert 'parameters.txt' not in vens.keys()
    vens.remove_data('bogus')  # This should only give warning

    # Test data addition. It should(?) work also for earlier nonexisting
    vens.append('betterdata', pd.DataFrame({'REAL': [0, 1, 2, 3, 4, 5, 6, 80],
                                            'NPV': [1000, 2000, 1500,
                                                    2300, 6000, 3000,
                                                    800, 9]}))
    assert 'betterdata' in vens.keys()
    assert 'REAL' in vens['betterdata'].columns
    assert 'NPV' in vens['betterdata'].columns

    assert vens.get_realization(3)['betterdata']['NPV'] == 2300
    assert vens.get_realization(0)['betterdata']['NPV'] == 1000
    assert vens.get_realization(1)['betterdata']['NPV'] == 2000
    assert vens.get_realization(2)['betterdata']['NPV'] == 1500
    assert vens.get_realization(80)['betterdata']['NPV'] == 9

    with pytest.raises(ValueError):
        vens.get_realization(9999)

    assert vens.shortcut2path('betterdata') == 'betterdata'
    assert vens.agg('min')['betterdata']['NPV'] == 9
    assert vens.agg('max')['betterdata']['NPV'] == 6000
    assert vens.agg('min')['betterdata']['NPV'] < \
        vens.agg('p93')['betterdata']['NPV']
    assert vens.agg('p55')['betterdata']['NPV'] < \
        vens.agg('p05')['betterdata']['NPV']
    assert vens.agg('p54')['betterdata']['NPV'] < \
        vens.agg('max')['betterdata']['NPV']

    assert 'REAL' not in vens.agg('min')['STATUS'].columns

    # Betterdata should be returned as a dictionary
    assert isinstance(vens.agg('min')['betterdata'], dict)
