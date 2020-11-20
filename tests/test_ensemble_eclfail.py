"""Testing fmu-ensemble, how it works on ensembles with realizations where Eclipse
has stopped preamturely

In the test data directory, there is a file 2_R001_REEK-1.UNSMRY-failed2000
which is the result of the same DATA file but with an END keyword injected
in the Schedule section, to simulate a premature failure. FMU-ensemble does
not try to distuingish between early exits from failure or deliberate (like this).
"""

import os
import logging
import shutil
import datetime

import numpy as np
import pandas as pd

from fmu.ensemble import ScratchEnsemble

logger = logging.getLogger(__name__)


def test_ens_premature_ecl(tmpdir):
    """Check an ensemble where Eclipse has failed early in realization 1"""
    if "__file__" in globals():
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    origensemble = ScratchEnsemble(
        "origreek", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0"
    )
    raw_orig_smry = origensemble.load_smry()
    # Copy the ensemble to /tmp so we can modify the UNSMRY file in real 2:
    tmpdir.chdir()

    shutil.copytree(testdir + "/data/testensemble-reek001", "ens_fail_real_reek001")
    unsmry_filename = (
        "ens_fail_real_reek001/realization-1/"
        + "iter-0/eclipse/model/2_R001_REEK-1.UNSMRY"
    )
    shutil.copy(unsmry_filename + "-failed2000", unsmry_filename)

    failensemble = ScratchEnsemble(
        "failedreek", "ens_fail_real_reek001/realization-*/iter-0"
    )
    raw_fail_smry = failensemble.load_smry()

    # This is usually superfluous when raw datetimes are obtained.
    raw_orig_smry["DATE"] = pd.to_datetime(raw_orig_smry["DATE"])
    raw_fail_smry["DATE"] = pd.to_datetime(raw_fail_smry["DATE"])

    # Homogeneous max-date in orig smry:
    assert len(raw_orig_smry.groupby("REAL").max()["DATE"].unique()) == 1
    # Different values for raw_fail:
    assert len(raw_fail_smry.groupby("REAL").max()["DATE"].unique()) == 2
    # END statement in schedule file on 2000-08-01 yields this:
    assert (
        str(raw_fail_smry.groupby("REAL").max()["DATE"].loc[1]) == "2000-08-01 00:00:00"
    )

    # Filter away all those that did not make it to the end. In normal scenarios,
    # this would be accomplished by .filter('OK'), but not in this test context.
    max_date = str(failensemble.get_smry()["DATE"].max())
    filtered_fail_ensemble = failensemble.filter(
        "unsmry--raw", column="DATE", columncontains=max_date, inplace=False
    )
    assert len(filtered_fail_ensemble) == 4
    assert (
        len(filtered_fail_ensemble.get_smry().groupby("REAL").max()["DATE"].unique())
        == 1
    )
    # Check also get_smry():
    assert len(failensemble.get_smry().groupby("REAL").max()["DATE"].unique()) == 2

    # With time_index set to something, then all realization will get
    # interpolated onto the same date range
    assert (
        len(
            failensemble.get_smry(time_index="monthly")
            .groupby("REAL")
            .max()["DATE"]
            .unique()
        )
        == 1
    )
    # This is in fact *different* from what you would get from load_smry (issue #97)
    assert (
        len(
            failensemble.load_smry(time_index="monthly")
            .groupby("REAL")
            .max()["DATE"]
            .unique()
        )
        == 2
    )
    # (this behaviour might change, get_smry() is allowed in
    # the future to mimic load_smry())

    # Check that FOPT is very much lower in real 1 in failed ensemble:
    assert (
        failensemble.get_smry(column_keys="FOPT", time_index="monthly")
        .groupby("REAL")
        .max()["FOPT"]
        .loc[1]
        < 1500000
    )
    assert (
        origensemble.get_smry(column_keys="FOPT", time_index="monthly")
        .groupby("REAL")
        .max()["FOPT"]
        .loc[1]
        > 6000000
    )

    # Also for yearly
    assert (
        failensemble.get_smry(column_keys="FOPT", time_index="yearly")
        .groupby("REAL")
        .max()["FOPT"]
        .loc[1]
        < 1500000
    )
    assert (
        origensemble.get_smry(column_keys="FOPT", time_index="yearly")
        .groupby("REAL")
        .max()["FOPT"]
        .loc[1]
        > 6000000
    )

    fail_foprs = failensemble.get_smry(column_keys="FOPR", time_index="monthly")

    # The FOPR rate vector should be all zero after the stop
    assert (
        fail_foprs[
            (fail_foprs["REAL"] == 1) & (fail_foprs["DATE"] > datetime.date(2000, 8, 1))
        ]["FOPR"]
        .abs()
        .sum()
        == 0
    )
    assert (
        fail_foprs[
            (fail_foprs["REAL"] == 0) & (fail_foprs["DATE"] > datetime.date(2000, 8, 1))
        ]["FOPR"]
        .abs()
        .sum()
        > 0
    )

    # This frame treats the "failed" realization as correct,
    # and it will affect the stats:
    fail_stats = failensemble.get_smry_stats(time_index="monthly")
    # Here, real 1 is removed
    filtered_stats = filtered_fail_ensemble.get_smry_stats(time_index="monthly")
    # Original stats
    orig_stats = origensemble.get_smry_stats(time_index="monthly")

    # The 30 last rows are the rows from 2000-09-01 to 2003-02-01:
    assert fail_stats.loc["minimum"]["FOPR"].iloc[-30:].abs().sum() == 0
    assert fail_stats.loc["minimum"]["FOPT"].iloc[-30:].unique()[0] == 1431247.125
    # Oh no, in filtered stats, the last date 2003-02-01 is
    # not included, probably a minor bug!
    # But that means that the indexing of the last 30 is a little bit rogue.
    # (this test should work even that bug is fixed)
    assert filtered_stats.loc["minimum"]["FOPR"].iloc[-29:].abs().sum() > 0
    assert len(filtered_stats.loc["minimum"]["FOPT"].iloc[-29:].unique()) == 29

    # Mean FOPR and FOPT should be affected by the zero-padded rates:
    assert (
        fail_stats.loc["mean"].iloc[-10]["FOPR"]
        < filtered_stats.loc["mean"].iloc[-10]["FOPR"]
    )
    assert (
        fail_stats.loc["mean"].iloc[-10]["FOPR"]
        < orig_stats.loc["mean"].iloc[-10]["FOPR"]
    )
    assert (
        fail_stats.loc["mean"].iloc[-10]["FOPT"]
        < filtered_stats.loc["mean"].iloc[-10]["FOPT"]
    )
    assert (
        fail_stats.loc["mean"].iloc[-10]["FOPT"]
        < orig_stats.loc["mean"].iloc[-10]["FOPT"]
    )

    # Delta profiles:
    delta_fail = origensemble - failensemble
    # Delta profiles are given for all realizations
    delta_fail_smry = delta_fail.get_smry()
    assert len(delta_fail_smry["REAL"].unique()) == 5
    # and they all end at the same ultimate date:
    assert len(delta_fail_smry.groupby("REAL").max()["DATE"].unique()) == 1
    # BUT, there is only NaNs for values after 2000-08-01:
    assert np.isnan(
        delta_fail_smry[
            (delta_fail_smry["REAL"] == 1) & (delta_fail_smry["DATE"] > "2000-08-01")
        ]["FOPT"].unique()[0]
    )

    # Delta profiles after filtering:
    delta_filtered = origensemble - filtered_fail_ensemble
    assert len(origensemble) == 5
    assert len(filtered_fail_ensemble) == 4
    # assert len(delta_filtered) == 4  # Only four realizations (requires #83 resolved)
    # to_virtual() and time_index can be removed when #83 is finished.
    delta_filtered_smry = delta_filtered.to_virtual().get_smry(time_index="monthly")
    # Should contain only four realizations, as one has been filtered away
    assert len(delta_filtered_smry["REAL"].unique()) == 4
    # Ultimate date is the same in all four:
    assert len(delta_filtered_smry.groupby("REAL").max()["DATE"].unique()) == 1
