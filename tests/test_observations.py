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
from fmu.ensemble import ScratchEnsemble, ScratchRealization

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

    reekensemble = ScratchEnsemble('reektest',
                                   testdir +
                                   '/data/testensemble-reek001/' +
                                   'realization-*/iter-0')

    obs = reekensemble.load_observations(testdir +
                                         '/data/testensemble-reek001/' +
                                         '/share/observations/' +
                                         'observations.yml')

    assert len(obs.keys()) == 2
    df_mismatch = reekensemble.ensemble_mismatch()

    assert len(df_mismatch.columns) == 7

## Alternative code with observations as its own object
     obsobject = ensemble.Observations(filename)

     mismatches = obsobject.mismatch(ensemble)
     # Returns a 
     
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
