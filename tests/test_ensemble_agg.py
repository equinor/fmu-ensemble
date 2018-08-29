# -*- coding: utf-8 -*-
"""Testing fmu-ensemble."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import numpy
import pandas as pd

from fmu import config
from fmu import ensemble

fmux = config.etc.Interaction()
logger = fmux.basiclogger(__name__)

if not fmux.testsetup():
    raise SystemExit()

def test_ensemble_aggregations():
    if '__file__' in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath('.')

    reekensemble = ensemble.ScratchEnsemble('reektest',
                                            testdir +
                                            '/data/testensemble-reek001/' +
                                            'realization-*/iter-0')
    reekensemble.from_smry(time_index='monthly', column_keys=['F*'])
    reekensemble.from_smry(time_index='yearly', column_keys=['F*'])
    reekensemble.from_csv('share/results/volumes/simulator_volume_fipnum.csv')

    stats = {
        'mean': reekensemble.agg('mean'),
        'median' : reekensemble.agg('median'),
        'min' : reekensemble.agg('min'),
        'max' : reekensemble.agg('max'),
        'p90' : reekensemble.agg('p10'),
        'p10' : reekensemble.agg('p90')
    }

    stats['min'].to_disk('virtreal_min', delete=True)
    stats['max'].to_disk('virtreal_max', delete=True)
    stats['mean'].to_disk('virtreal_mean', delete=True)

    assert stats['min']['parameters.txt']['RMS_SEED'] < \
        stats['max']['parameters.txt']['RMS_SEED']

    assert stats['min']['parameters.txt']['RMS_SEED'] <= \
        stats['p10']['parameters.txt']['RMS_SEED']
    assert stats['p10']['parameters.txt']['RMS_SEED'] <= \
        stats['median']['parameters.txt']['RMS_SEED']
    assert stats['median']['parameters.txt']['RMS_SEED'] <= \
        stats['p90']['parameters.txt']['RMS_SEED']
    assert stats['p90']['parameters.txt']['RMS_SEED'] <= \
        stats['max']['parameters.txt']['RMS_SEED']

    assert stats['min']['parameters.txt']['RMS_SEED'] <= \
        stats['mean']['parameters.txt']['RMS_SEED']
    assert stats['min']['parameters.txt']['RMS_SEED'] <= \
        stats['max']['parameters.txt']['RMS_SEED']

    assert stats['min']['unsmry-monthly']['FOPT'].iloc[-1] < \
        stats['max']['unsmry-monthly']['FOPT'].iloc[-1]

    # .loc[2] corresponds to FIPNUM=3
    assert stats['min']['simulator_volume_fipnum'].iloc[2]['STOIIP_OIL'] < \
        stats['mean']['simulator_volume_fipnum'].iloc[2]['STOIIP_OIL']
    assert stats['mean']['simulator_volume_fipnum'].loc[2]['STOIIP_OIL'] < \
        stats['max']['simulator_volume_fipnum'].loc[2]['STOIIP_OIL']

    # Aggregation of STATUS also works. Note that min and max
    # works for string columns, so the available data will vary
    # depending on aggregation method
    assert stats['p90']['STATUS'].iloc[49]['DURATION'] < \
        stats['max']['STATUS'].iloc[49]['DURATION']
    # job 49 is the Eclipse forward model

