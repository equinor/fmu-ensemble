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

    # With time_index set to something, each realization is still time-interpolated
    # individually and we still have two different max-dates:
    assert (
        len(
            failensemble.get_smry(time_index="monthly")
            .groupby("REAL")
            .max()["DATE"]
            .unique()
        )
        == 2
    )
    # load_smry and get_smry behave the same
    # (they were different in fmu-ensemble 1.x)
    assert (
        len(
            failensemble.load_smry(time_index="monthly")
            .groupby("REAL")
            .max()["DATE"]
            .unique()
        )
        == 2
    )

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
    orig_smry = origensemble.get_smry(time_index="monthly").set_index(["REAL", "DATE"])

    # fmu-ensemble 1.x extrapolated the failed realization with zero rates to the
    # common end-date for the ensemble, giving zero as the minimum realization.
    # fmu-ensemble 2.x have NaNs for rates after the failure date, and do not
    # enter the statistics

    # Thus the minimum rates at the latest dates (post failure in real 0) is nonzero:
    assert fail_stats.loc["minimum"]["FOPR"].iloc[-30:].abs().sum() > 0

    # The final date is present in the statistics frames
    assert "2003-02-01" in fail_stats.loc["minimum"].index.astype(str).values

    # Oh no, in filtered stats, the last date 2003-02-01 is
    # not included, probably a minor bug!
    assert "2003-02-01" not in filtered_stats.loc["minimum"].index.astype(str).values
    # But that means that the indexing of the last 30 is a little bit rogue.
    # (this test should work even that bug is fixed)
    assert filtered_stats.loc["minimum"]["FOPR"].iloc[-29:].abs().sum() > 0
    assert len(filtered_stats.loc["minimum"]["FOPT"].iloc[-29:].unique()) == 29

    # Mean FOPR and FOPT should be affected by the zero-padded rates.
    # In fail_stats, realization 1 is truncated, and in filtered_stats
    # realization 1 does not exist.

    # Some manually computed means from orig summary:
    fopr_mean_all = (
        orig_smry.loc[0, datetime.datetime(2002, 1, 1)]["FOPR"]
        # Pandas allows index lookup using both strings and datetimes (not date),
        # because we have done a set_index() on the frame.
        + orig_smry.loc[1, "2002-01-01"]["FOPR"]
        + orig_smry.loc[2, "2002-01-01"]["FOPR"]
        + orig_smry.loc[3, "2002-01-01"]["FOPR"]
        + orig_smry.loc[4, "2002-01-01"]["FOPR"]
    ) / 5
    fopr_mean_not1 = (
        orig_smry.loc[0, "2002-01-01"]["FOPR"]
        + orig_smry.loc[2, "2002-01-01"]["FOPR"]
        + orig_smry.loc[3, "2002-01-01"]["FOPR"]
        + orig_smry.loc[4, "2002-01-01"]["FOPR"]
    ) / 4  # == 5627.0299072265625

    # The last alternative was how fmu.ensemble v1.x worked:
    fopr_mean_zero1 = (  # noqa
        orig_smry.loc[0, "2002-01-01"]["FOPR"]
        + 0
        + orig_smry.loc[2, "2002-01-01"]["FOPR"]
        + orig_smry.loc[3, "2002-01-01"]["FOPR"]
        + orig_smry.loc[4, "2002-01-01"]["FOPR"]
    ) / 5  # == 4501.62392578125

    # Pandas 1.2.3 at least provides different time objects between the two frames:
    # failensemble.get_smry_stats(time_index="monthly").loc["mean"].index.values
    # filtered_fail_ensemble.get_smry_stats(time_index="monthly").loc["mean"].index.values
    # with datetime.date() in the first and datetime64[ns] in the latter.
    # We don't want to expose this test code to that detail, so convert to strings:
    fail_stats_mean = fail_stats.loc["mean"]
    fail_stats_mean.index = fail_stats_mean.index.astype(str)
    assert fail_stats_mean.loc["2002-01-01"]["FOPR"] == fopr_mean_not1
    filtered_stats_mean = filtered_stats.loc["mean"]
    filtered_stats_mean.index = filtered_stats_mean.index.astype(str)
    assert filtered_stats_mean.loc["2002-01-01"]["FOPR"] == fopr_mean_not1
    orig_stats_mean = orig_stats.loc["mean"]
    orig_stats_mean.index = orig_stats_mean.index.astype(str)
    assert orig_stats_mean.loc["2002-01-01"]["FOPR"] == fopr_mean_all
    # FOPT is handled identical to FOPR, as there is no extrapolation
    # by default of summary vectors in fmu.ensemble v2.x (in v1.x rates and totals
    # were extrapolated individually)

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
