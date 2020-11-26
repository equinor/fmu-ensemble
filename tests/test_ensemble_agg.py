"""Testing aggregation of ensembles."""

import os
import logging

import pandas as pd
import pytest

from fmu.ensemble import ScratchEnsemble

logger = logging.getLogger(__name__)


def test_ensemble_aggregations(tmpdir):
    """Test aggregations of ensembles, that
    is taking means, medians, p10 and so on, producing
    virtual realizations"""
    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    reekensemble = ScratchEnsemble(
        "reektest", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0"
    )
    reekensemble.load_smry(time_index="monthly", column_keys=["F*"])
    reekensemble.load_smry(time_index="yearly", column_keys=["F*"])
    reekensemble.load_csv("share/results/volumes/simulator_volume_fipnum.csv")
    reekensemble.load_scalar("npv.txt", convert_numeric=True)

    stats = {
        "mean": reekensemble.agg("mean"),
        "median": reekensemble.agg("median"),
        "min": reekensemble.agg("min"),
        "max": reekensemble.agg("max"),
        "p10": reekensemble.agg("p10"),  # low estimate
        "p90": reekensemble.agg("p90"),  # high estimate
    }

    tmpdir.chdir()
    stats["min"].to_disk("virtreal_min", delete=True)
    stats["max"].to_disk("virtreal_max", delete=True)
    stats["mean"].to_disk("virtreal_mean", delete=True)

    assert (
        stats["min"]["parameters.txt"]["RMS_SEED"]
        < stats["max"]["parameters.txt"]["RMS_SEED"]
    )

    assert (
        stats["min"]["parameters.txt"]["RMS_SEED"]
        <= stats["p10"]["parameters.txt"]["RMS_SEED"]
    )
    assert (
        stats["p10"]["parameters.txt"]["RMS_SEED"]
        <= stats["median"]["parameters.txt"]["RMS_SEED"]
    )
    assert (
        stats["median"]["parameters.txt"]["RMS_SEED"]
        <= stats["p90"]["parameters.txt"]["RMS_SEED"]
    )
    assert (
        stats["p90"]["parameters.txt"]["RMS_SEED"]
        <= stats["max"]["parameters.txt"]["RMS_SEED"]
    )

    assert (
        stats["min"]["parameters.txt"]["RMS_SEED"]
        <= stats["mean"]["parameters.txt"]["RMS_SEED"]
    )
    assert (
        stats["min"]["parameters.txt"]["RMS_SEED"]
        <= stats["max"]["parameters.txt"]["RMS_SEED"]
    )

    assert (
        stats["min"]["unsmry--monthly"]["FOPT"].iloc[-1]
        < stats["max"]["unsmry--monthly"]["FOPT"].iloc[-1]
    )

    # .loc[2] corresponds to FIPNUM=3
    assert (
        stats["min"]["simulator_volume_fipnum"].iloc[2]["STOIIP_OIL"]
        < stats["mean"]["simulator_volume_fipnum"].iloc[2]["STOIIP_OIL"]
    )
    assert (
        stats["mean"]["simulator_volume_fipnum"].loc[2]["STOIIP_OIL"]
        < stats["max"]["simulator_volume_fipnum"].loc[2]["STOIIP_OIL"]
    )

    # Aggregation of STATUS also works. Note that min and max
    # works for string columns, so the available data will vary
    # depending on aggregation method
    assert (
        stats["p10"]["STATUS"].iloc[49]["DURATION"]
        < stats["max"]["STATUS"].iloc[49]["DURATION"]
    )
    # job 49 is the Eclipse forward model

    assert "npv.txt" in stats["mean"].keys()
    assert stats["mean"]["npv.txt"] == 3382.5

    # Test agg(excludekeys=..)
    assert "STATUS" not in reekensemble.agg("mean", excludekeys="STATUS").keys()
    assert "STATUS" not in reekensemble.agg("mean", keylist=["parameters.txt"]).keys()

    assert (
        reekensemble.agg("p01")["parameters"]["RMS_SEED"]
        < reekensemble.agg("p99")["parameters"]["RMS_SEED"]
    )

    with pytest.raises(ValueError):
        reekensemble.agg("foobar")

    # Check that include/exclude functionality in agg() works:
    assert (
        "parameters.txt"
        not in reekensemble.agg("mean", excludekeys="parameters.txt").keys()
    )
    assert (
        "parameters.txt"
        not in reekensemble.agg("mean", excludekeys=["parameters.txt"]).keys()
    )
    assert "parameters.txt" not in reekensemble.agg("mean", keylist="STATUS").keys()
    assert "parameters.txt" not in reekensemble.agg("mean", keylist=["STATUS"]).keys()

    # Shorthand notion works for keys to include, but they
    # should get returned with fully qualified paths.
    assert (
        "share/results/tables/unsmry--yearly.csv"
        in reekensemble.agg("mean", keylist="unsmry--yearly").keys()
    )
    assert (
        "share/results/tables/unsmry--yearly.csv"
        in reekensemble.agg("mean", keylist=["unsmry--yearly"]).keys()
    )
    assert isinstance(
        reekensemble.agg("mean", keylist="unsmry--yearly").get_df("unsmry--yearly"),
        pd.DataFrame,
    )
