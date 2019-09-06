# -*- coding: utf-8 -*-
"""Testing fmu-ensemble."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os

import pandas as pd

from fmu.ensemble import etc
from fmu import ensemble

fmux = etc.Interaction()
logger = fmux.basiclogger(__name__, level="WARNING")

if not fmux.testsetup():
    raise SystemExit()


def test_ensemblecombination_basic():
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
    assert diff["parameters"]["KRW1"].sum() == 0
    assert diff["unsmry--yearly"]["FOPT"].sum() == 0
    assert diff["npv.txt"]["npv.txt"].sum() == 0

    foptsum = reekensemble.get_df("unsmry--yearly")["FOPT"].sum()
    half = 0.5 * reekensemble
    assert half["unsmry--yearly"]["FOPT"].sum() == 0.5 * foptsum
    assert half["npv.txt"].sort_values("REAL")["npv.txt"].iloc[0] == 1722

    # This is only true since we only juggle one ensemble here:
    assert len(half.get_smry_dates(freq="monthly")) == len(
        reekensemble.get_smry_dates(freq="monthly")
    )

    # Test something long:
    zero = (
        reekensemble
        + 4 * reekensemble
        - reekensemble * 2
        - (-1) * reekensemble
        - reekensemble * 4
    )
    assert zero["parameters"]["KRW1"].sum() == 0

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
