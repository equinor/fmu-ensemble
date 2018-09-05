# -*- coding: utf-8 -*-
"""Testing fmu-ensemble."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os

from fmu import config
from fmu import ensemble

from fmu.ensemble import RealizationCombination

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

    
    print((real0 - real1)['unsmry-yearly'])
    ##    
    ### Difference with itself should give zero..
    ##diff = realization.RealizationCombination(ref=reekrealization, sub=reekrealization)
    ##assert diff['parameters']['KRW1'].sum() == 0
    ##assert diff['unsmry-yearly']['FOPT'].sum() == 0
   ##
    ##foptsum = reekrealization.get_df('unsmry-yearly')['FOPT'].sum()
    ##half = 0.5 * reekrealization
    ##assert half['unsmry-yearly']['FOPT'].sum() == 0.5 * foptsum
   ##
    ### This is only true since we only juggle one realization here:
    ##assert len(half.get_smry_dates(freq='monthly')) == len(
    ##    reekrealization.get_smry_dates(freq='monthly'))
   ##
    ### Test something long:
    ##zero = reekrealization + 4*reekrealization - reekrealization*2 -\
    ##    (-1) * reekrealization - reekrealization * 4
    ##assert zero['parameters']['KRW1'].sum() == 0
   ##
    ##assert len(diff.get_smry(column_keys=['FOPR', 'FGPR',
    ##                                      'FWCT']).columns) == 5
   ##
    ### Virtualization of realization combinations
    ### (equals comutation of everything)
    ##vzero = zero.to_virtual()
    ##assert len(vzero.keys()) == len(zero.keys())


