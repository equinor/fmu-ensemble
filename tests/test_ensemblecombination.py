"""Testing linear combinations of ensemble."""

import os
import logging
import pandas as pd

import pytest

from fmu import ensemble

logger = logging.getLogger(__name__)


def test_ensemblecombination_basic():
    """Basic tests for linear combinations of ensemble objects"""
    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    reekensemble = ensemble.ScratchEnsemble(
        "reektest", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0"
    )
    reekensemble.load_smry(time_index="yearly", column_keys=["F*"])
    reekensemble.load_scalar("npv.txt", convert_numeric=True)

    # Difference with itself should give zero..
    diff = ensemble.EnsembleCombination(ref=reekensemble, sub=reekensemble)
    assert len(diff) == 5
    # Implicit virtualization and aggregation
    assert diff.agg("mean")["parameters"]["KRW1"] == 0

    assert diff.parameters["KRW1"].sum() == 0
    assert diff["parameters"]["KRW1"].sum() == 0
    assert diff["unsmry--yearly"]["FOPT"].sum() == 0
    assert diff["npv.txt"]["npv.txt"].sum() == 0

    assert (
        diff.get_volumetric_rates(column_keys="FOPT", time_index="yearly")["FOPR"].sum()
        == 0
    )

    foptsum = reekensemble.get_df("unsmry--yearly")["FOPT"].sum()
    half = 0.5 * reekensemble
    assert half["unsmry--yearly"]["FOPT"].sum() == 0.5 * foptsum
    assert half["npv.txt"].sort_values("REAL")["npv.txt"].iloc[0] == 1722
    assert (
        half.get_smry(column_keys="FOPT", time_index="last")["FOPT"].sum()
        == half.get_volumetric_rates(column_keys="FOPT", time_index="yearly")[
            "FOPR"
        ].sum()
    )

    smrymeta = diff.get_smry_meta(["FO*"])
    assert "FOPT" in smrymeta

    # This is only true since we only juggle one ensemble here:
    assert len(half.get_smry_dates(freq="monthly")) == len(
        reekensemble.get_smry_dates(freq="monthly")
    )

    # Test comb of virtualized ensembles.
    vhalf = 0.5 * reekensemble.to_virtual()
    assert vhalf["unsmry--yearly"]["FOPT"].sum() == 0.5 * foptsum

    vhalf_filtered = (0.5 * reekensemble).to_virtual(keyfilter="parameters")

    # This means that unsmry--yearly is not found:
    assert vhalf_filtered.shortcut2path("unsmry--yearly") == "unsmry--yearly"

    # Ask to include summary data:
    vhalf_filtered2 = (0.5 * reekensemble).to_virtual(keyfilter="unsmry")
    assert not vhalf_filtered2.get_df("unsmry--yearly").empty
    with pytest.raises((KeyError, ValueError)):
        # pylint: disable=pointless-statement
        vhalf_filtered2.parameters

    # Get summary data with parameters:
    smry_params = vhalf.get_df("unsmry--yearly", merge="parameters.txt")
    assert "SORG1" in smry_params
    assert "FWCT" in smry_params
    # Data is merged before ensemble computations:
    assert smry_params["T_1OG"].min() == reekensemble.parameters["T_1OG"].min() * 0.5
    print(smry_params)

    # Test something long:
    # zero = (
    #    reekensemble
    #    + 4 * reekensemble
    #    - reekensemble * 2
    #    - (-1) * reekensemble
    #    - reekensemble * 4
    # )
    # The above takes too long time to compute until #10 is solved.
    # We can test something cheaper:
    zero = reekensemble + reekensemble - 2 * reekensemble
    assert zero["parameters"]["KRW1"].sum() == 0
    smrymeta = zero.get_smry_meta(["FO*"])
    assert "FOPT" in smrymeta

    vzero = (
        reekensemble.to_virtual()
        + reekensemble.to_virtual()
        - 2 * reekensemble.to_virtual()
    )
    assert vzero["parameters"]["KRW1"].sum() == 0

    assert len(diff.get_smry(column_keys=["FOPR", "FGPR", "FWCT"]).columns) == 5

    # eclipse summary vector statistics for a given ensemble
    df_stats = diff.get_smry_stats(column_keys=["FOPR", "FGPR"], time_index="monthly")
    assert isinstance(df_stats, pd.DataFrame)
    assert len(df_stats.columns) == 2
    assert isinstance(df_stats["FOPR"]["mean"], pd.Series)
    assert len(df_stats["FOPR"]["mean"].index) == 38

    # check if wild cards also work for get_smry_stats
    df_stats = reekensemble.get_smry_stats(
        column_keys=["FOP*", "FGP*"], time_index="monthly"
    )
    assert len(df_stats.columns) == len(reekensemble.get_smrykeys(["FOP*", "FGP*"]))

    # Check webviz requirements for dataframe
    stats = df_stats.index.levels[0]
    assert "minimum" in stats
    assert "maximum" in stats
    assert "p10" in stats
    assert "p90" in stats
    assert "mean" in stats
    assert df_stats["FOPR"]["minimum"].iloc[-2] < df_stats["FOPR"]["maximum"].iloc[-2]

    # Virtualization of ensemble combinations
    # (equals comutation of everything)
    vzero = zero.to_virtual()
    assert len(vzero.keys()) == len(zero.keys())


def test_ensemblecomb_observations():
    """Test a combination of EnsembleCombinations and mismatch calculations"""
    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    reekensemble = ensemble.ScratchEnsemble(
        "reektest", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0"
    )
    reekensemble.load_smry(time_index="yearly", column_keys=["F*"])

    delta_ens = reekensemble - 0.5 * reekensemble

    # Now find the realization in delta_ens that is closest to
    # the mean delta-FOPT profile:
    mean = delta_ens.agg("mean")  # a virtual realization
    obs = ensemble.Observations({})
    obs.load_smry(mean, "FOPT", time_index="yearly")
    mis = obs.mismatch(delta_ens)
    # Realization 4 is best representing the mean delta FOPT:
    assert mis.groupby("REAL").sum()["L2"].sort_values().index.values[0] == 4
    # (this is the same as the representative realization for mean as
    # found in test_observations.py::test_virtual_observations)


def test_ensemblecombination_sparse():
    """Test ensemble combinations where the ensembles are not so similiar,
    something missing in some ensembles etc.
    """
    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    ref = ensemble.ScratchEnsemble(
        "reektest", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0"
    )
    ref.load_smry(time_index="yearly", column_keys=["F*"])

    # Initialize over again to get two different objects
    ior = ensemble.ScratchEnsemble(
        "reektest", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0"
    )
    ior.load_smry(time_index="yearly", column_keys=["F*"])
    ior.remove_realizations(3)

    assert len(ref) == 5
    assert len(ior) == 4
    assert len(ior - ref) == 4

    assert 3 not in (ior - ref)["parameters"].REAL.unique()
    assert 3 not in (ior - ref)["unsmry--yearly"].REAL.unique()

    # Delete a specific date in the ior ensemble
    dframe = ior[4].data["share/results/tables/unsmry--yearly.csv"]
    print(dframe.DATE.dtype)
    dframe.drop(2, inplace=True)  # index 2 is for date 2002-01-1
    # Inject into ensemble again:
    ior[4].data["share/results/tables/unsmry--yearly.csv"] = dframe
    assert "2002-01-01" not in list((ior - ref)["unsmry--yearly.csv"].DATE.unique())

    # Convert ref case to virtual:
    vref = ref.to_virtual()
    # Do the same checks over again:
    assert 3 not in (ior - vref)["parameters"].REAL.unique()
    assert 3 not in (ior - vref)["unsmry--yearly"].REAL.unique()
    assert "2002-01-01" not in list((ior - vref)["unsmry--yearly.csv"].DATE.unique())
    assert len((ior - vref)["unsmry--yearly"]) == 19

    unsmry = vref.data["share/results/tables/unsmry--yearly.csv"]
    del unsmry["FWIR"]
    vref.data["share/results/tables/unsmry--yearly.csv"] = unsmry

    assert "FWIR" in ior.get_df("unsmry--yearly").columns
    assert "FWIR" not in vref.get_df("unsmry--yearly").columns
    assert "FWIR" not in (ior - vref)["unsmry--yearly"].columns
