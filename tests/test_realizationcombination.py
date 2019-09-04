# -*- coding: utf-8 -*-
"""Testing fmu-ensemble."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os

from fmu.ensemble import etc
from fmu import ensemble

from fmu.ensemble import ScratchEnsemble

fmux = etc.Interaction()
logger = fmux.basiclogger(__name__, level="WARNING")

if not fmux.testsetup():
    raise SystemExit()


def test_realizationcombination_basic():
    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    real0dir = os.path.join(
        testdir, "data/testensemble-reek001", "realization-0/iter-0"
    )
    real0 = ensemble.ScratchRealization(real0dir)
    real0.load_smry(time_index="yearly", column_keys=["F*"])
    real1dir = os.path.join(
        testdir, "data/testensemble-reek001", "realization-1/iter-0"
    )
    real1 = ensemble.ScratchRealization(real1dir)
    real1.load_smry(time_index="yearly", column_keys=["F*"])

    realdiff = real0 - real1
    assert "FWPR" in realdiff["unsmry--yearly"]
    assert "FWL" in realdiff["parameters"]


def test_manual_aggregation():
    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    reekensemble = ScratchEnsemble(
        "reektest", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0"
    )
    reekensemble.load_smry(time_index="yearly", column_keys=["F*"])
    reekensemble.load_csv("share/results/volumes/simulator_volume_fipnum.csv")

    # Aggregate an ensemble into a virtual "mean" realization
    mean = reekensemble.agg("mean")

    # Combine the ensemble members directly into a mean computation.
    # Also returns a virtual realization.
    manualmean = (
        1
        / 5
        * (
            reekensemble[0]
            + reekensemble[1]
            + reekensemble[2]
            + reekensemble[3]
            + reekensemble[4]
        )
    )

    # Commutativity proof:
    assert mean["parameters"]["RMS_SEED"] == manualmean["parameters"]["RMS_SEED"]
