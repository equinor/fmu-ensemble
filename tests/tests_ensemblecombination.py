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

    diff = ensemble.EnsembleCombination(ref=reekensemble, subs=reekensemble)

    # example of several operations
    diff = diff + reekensemble - reekensemble

    assert isinstance(diff, ensemble.EnsembleCombination)

    assert len(diff.from_smry(column_keys=['FOPR', 'FGPR',
                              'FWCT']).columns) == 5
