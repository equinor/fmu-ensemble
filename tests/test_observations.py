# -*- coding: utf-8 -*-
"""Testing fmu-ensemble."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import numpy
import pandas as pd

import pytest

from fmu import config
from fmu.ensemble import ScratchEnsemble, ScratchRealization, Observations

fmux = config.etc.Interaction()
logger = fmux.basiclogger(__name__)

if not fmux.testsetup():
    raise SystemExit()


def test_observation_import():
    if '__file__' in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath('.')

    obs = Observations(testdir +
                       '/data/testensemble-reek001/' +
                       '/share/observations/' +
                       'observations.yml')
    assert len(obs.keys()) == 10 # adjust this..



def test_real_mismatch():
    if '__file__' in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath('.')

    real = ScratchRealization(testdir + '/data/testensemble-reek001/' +
                              'realization-0/iter-0/')

    real.load_smry()
    obs = Observations({'txt': [ {'localpath': 'parameters.txt',
                                  'key': 'FWL',
                                  'value': 1702}]})
    realmis = obs.mismatch(real)

    # Check layout of returned data
    assert isinstance(realmis, pd.DataFrame)
    assert len(realmis) == 1
    assert 'REAL' not in realmis.columns  # should only be there for ensembles.
    assert 'OBSKEY' in realmis.columns
    assert 'DATE' not in realmis.columns  # date is not relevant
    assert 'MIS' in realmis.columns
    assert 'L1' in realmis.columns
    assert 'L2' in realmis.columns

    # Check actually computed values, there should only be one row with data:
    assert realmis.loc[0, 'OBSKEY'] == 'txt/parameters.txt/FWL'
    assert realmis.loc[0, 'MIS'] == -2
    assert realmis.loc[0, 'L1'] == 2
    assert realmis.loc[0, 'L2'] == 2

    # Another observation set:
    obs2 = Observations({'txt': [ {'localpath': 'parameters.txt',
                                   'key': 'RMS_SEED',
                                   'value': 600000000},
                                  {'localpath': 'outputs.txt',
                                   'key': 'top_structure',
                                   'value': 3200}
                                  ]
                         'scalar': [ {'key': 'npv.txt',
                                      'value': 3400}]})
    realmis2 = obs2.mismatch(real)
    assert len(realmis2) == 3
    assert realmis2['OBSKEY'].values == ['txt/parameters.txt',
                                         'txt/outputs.txt',
                                         'scalar/npv.txt']
    # assert much more!

    # Test use of allocated values:
    obs3 = Observations({'smry': [ {'key': 'FOPT',
                                    'observations': 'FOPTH'} ]})
    fopt_mis = obs3.mismatch(real)
    assert fopt_mis.loc[0, 'OBSKEY'] == 'smry/FOPT'
    assert fopt_mis.loc[0, 'L1'] > 0
    assert fopt_mis.loc[0, 'L1'] != fopt_mis.loc[0, 'L2']

    # Test dumping to yaml:
    yamlobsstr = obs2.to_yaml()
    assert isinstance(yamlobsstr, str)
    # * Write yamlobsstr to tmp file
    # * Reload observation object from that file
    # * Check that the observation objects are the same


def test_ens_mismatch():
    if '__file__' in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath('.')
        
    
        
    theobs = obsobject.get_observations()  # returns a dict
    theobs.pop('FGPT')
    alternative = Observations(theobs)  # We can also give in a 'dict'
    # to reinitialize

    # For dumping to disk/cloud.
    yaml = alternative.to_yaml()  # (returns multiline string)
    # json = alternative.to_json()  # should not be supported,
    # will create confusion

    alternative_mismatches = alternative.mismatch(ensemble)

    # Obtain a list of integers
    ranked_realizations = alternative.rank(ensemble, "FOPT")
    best_realization = ranked_realizations[0]
