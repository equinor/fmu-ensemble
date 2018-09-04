# -*- coding: utf-8 -*-
"""Testing fmu-ensemble."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os

from fmu import config
from fmu import ensemble

fmux = config.etc.Interaction()
logger = fmux.basiclogger(__name__)

if not fmux.testsetup():
    raise SystemExit()


def test_ensemblecombination():
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

    # Difference with itself should give zero..
    diff = ensemble.EnsembleCombination(ref=reekensemble, sub=reekensemble)
    assert diff['parameters']['KRW1'].sum() == 0
    assert diff['unsmry-yearly']['FOPT'].sum() == 0

    foptsum = reekensemble.get_df('unsmry-yearly')['FOPT'].sum()
    half = 0.5 * reekensemble
    assert half['unsmry-yearly']['FOPT'].sum() == 0.5 * foptsum

    # This is only true since we only juggle one ensemble here:
    assert len(half.get_smry_dates(freq='monthly')) == len(
        reekensemble.get_smry_dates(freq='monthly'))

    # Test something long:
    zero = reekensemble + 4*reekensemble - reekensemble*2 -\
           (-1) * reekensemble - reekensemble * 4
    assert zero['parameters']['KRW1'].sum() == 0

    assert len(diff.get_smry(column_keys=['FOPR', 'FGPR',
                                          'FWCT']).columns) == 5

    # Virtualization of ensemble combinations
    # (equals comutation of everything)
    vzero = zero.to_virtual()
    assert len(vzero.keys()) == len(zero.keys())
