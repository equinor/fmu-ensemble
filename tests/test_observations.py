# -*- coding: utf-8 -*-
"""Testing fmu-ensemble."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pandas as pd

import pytest

from fmu import config
from fmu.ensemble import ScratchEnsemble, ScratchRealization, Observations

fmux = config.etc.Interaction()
logger = fmux.basiclogger(__name__)

if not fmux.testsetup():
    raise SystemExit()


def test_observation_import():
    """Test import of observations from yaml"""
    if '__file__' in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath('.')

    obs = Observations(testdir +
                       '/data/testensemble-reek001/' +
                       '/share/observations/' +
                       'observations.yml')
    assert len(obs.keys()) == 2  # adjust this..
    assert len(obs['smry']) == 7
    assert len(obs['rft']) == 2

    assert isinstance(obs['smry'], list)
    assert isinstance(obs['rft'], list)


def test_real_mismatch():
    """Test calculation of mismatch from the observation set to a
    realization"""
    if '__file__' in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath('.')

    real = ScratchRealization(testdir + '/data/testensemble-reek001/' +
                              'realization-0/iter-0/')

    real.load_smry()
    real.load_txt('outputs.txt')
    real.load_scalar('npv.txt')

    obs = Observations({'txt': [{'localpath': 'parameters.txt',
                                 'key': 'FWL',
                                 'value': 1702}]})
    realmis = obs.mismatch(real)

    # Check layout of returned data
    assert isinstance(realmis, pd.DataFrame)
    assert len(realmis) == 1
    assert 'REAL' not in realmis.columns  # should only be there for ensembles.
    assert 'OBSTYPE' in realmis.columns
    assert 'OBSKEY' in realmis.columns
    assert 'DATE' not in realmis.columns  # date is not relevant
    assert 'MISMATCH' in realmis.columns
    assert 'L1' in realmis.columns
    assert 'L2' in realmis.columns

    # Check actually computed values, there should only be one row with data:
    assert realmis.loc[0, 'OBSTYPE'] == 'txt'
    assert realmis.loc[0, 'OBSKEY'] == 'parameters.txt/FWL'
    assert realmis.loc[0, 'MISMATCH'] == -2
    assert realmis.loc[0, 'SIGN'] == -1
    assert realmis.loc[0, 'L1'] == 2
    assert realmis.loc[0, 'L2'] == 4

    # Another observation set:
    obs2 = Observations({'txt': [{'localpath': 'parameters.txt',
                                  'key': 'RMS_SEED',
                                  'value': 600000000},
                                 {'localpath': 'outputs.txt',
                                  'key': 'top_structure',
                                  'value': 3200}
                                ],
                         'scalar': [{'key': 'npv.txt',
                                     'value': 3400}]})
    realmis2 = obs2.mismatch(real)
    assert len(realmis2) == 3
    assert 'parameters.txt/RMS_SEED' in realmis2['OBSKEY'].values
    assert 'outputs.txt/top_structure' in realmis2['OBSKEY'].values
    assert 'npv.txt' in realmis2['OBSKEY'].values

    # assert much more!

    # Test use of allocated values:
    obs3 = Observations({'smryh': [{'key': 'FOPT',
                                    'histvec': 'FOPTH'}]})
    fopt_mis = obs3.mismatch(real)
    assert fopt_mis.loc[0, 'OBSTYPE'] == 'smryh'
    assert fopt_mis.loc[0, 'OBSKEY'] == 'FOPT'
    assert fopt_mis.loc[0, 'L1'] > 0
    assert fopt_mis.loc[0, 'L1'] != fopt_mis.loc[0, 'L2']

    # Test dumping to yaml:
    # Not implemented.
    # yamlobsstr = obs2.to_yaml()
    # assert isinstance(yamlobsstr, str)
    # * Write yamlobsstr to tmp file
    # * Reload observation object from that file
    # * Check that the observation objects are the same


def test_smry():
    """Test the support for smry observations, these are
    observations relating to summary data, but where
    the observed values are specified in yaml, not through
    *H summary variables"""

    if '__file__' in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath('.')

    obs = Observations(testdir +
                       '/data/testensemble-reek001/' +
                       '/share/observations/' +
                       'observations.yml')
    real = ScratchRealization(testdir + '/data/testensemble-reek001/' +
                              'realization-0/iter-0/')

    # Compute the mismatch from this particular observation set to the
    # loaded realization.
    mismatch = obs.mismatch(real)

    assert len(mismatch) == 21  # later: implement counting in the obs object
    assert mismatch.L1.sum() > 0
    assert mismatch.L2.sum() > 0

    # This should work, but either the observation object
    # must do the smry interpolation in dataframes, or
    # the virtual realization should implement get_smry()
    # vreal = real.to_virtual()
    # vmismatch = obs.mismatch(vreal)
    # print(vmismatch)


def test_errormessages():
    """Test that we give ~sensible error messages when the
    observation input is unparseable"""

    # Emtpy
    with pytest.raises(TypeError):
        Observations()

    # Non-existing filename:
    with pytest.raises(IOError):
        Observations("foobar")

    # Integer input does not make sense..
    with pytest.raises(ValueError):
        Observations(3)

    # Unsupported observation category
    with pytest.raises(ValueError):
        Observations(dict(foobar='foo'))
        # (there will be logged a warning also)

    # Check that the dict is a dict of lists:
    with pytest.raises(ValueError):
        Observations(dict(smry='not_a_list'))
        # (warning will also be issued)


def test_ens_mismatch():
    """Test calculation of mismatch to ensemble data"""
    if '__file__' in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath('.')
    ens = ScratchEnsemble('test', testdir + '/data/testensemble-reek001/' +
                          'realization-*/iter-0/')

    obs = Observations({'smryh': [{'key': 'FOPT',
                                   'histvec': 'FOPTH'}]})

    mismatch = obs.mismatch(ens)

    assert 'L1' in mismatch.columns
    assert 'L2' in mismatch.columns
    assert 'MISMATCH' in mismatch.columns
    assert 'OBSKEY' in mismatch.columns
    assert 'OBSTYPE' in mismatch.columns
    assert 'REAL' in mismatch.columns
    assert len(mismatch) == len(ens) * 1  # number of observation units.

    fopt_rank = mismatch.sort_values('L2', ascending=True)['REAL'].values
    assert fopt_rank[0] == 2  # closest realization
    assert fopt_rank[-1] == 1  # worst realization
