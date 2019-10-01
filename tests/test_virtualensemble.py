# -*- coding: utf-8 -*-
"""Testing fmu-ensemble."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import numpy as np
import pandas as pd
import pytest

from fmu.ensemble import etc
from fmu.ensemble import ScratchEnsemble, VirtualEnsemble

fmux = etc.Interaction()
logger = fmux.basiclogger(__name__, level="WARNING")

if not fmux.testsetup():
    raise SystemExit()


def test_virtualensemble():
    """Test the properties of a virtualized ScratchEnsemble"""
    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    reekensemble = ScratchEnsemble(
        "reektest", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0"
    )
    reekensemble.load_smry(time_index="yearly", column_keys=["F*"])
    reekensemble.load_scalar("npv.txt")
    reekensemble.load_txt("outputs.txt")
    vens = reekensemble.to_virtual()

    # Check that we have data for 5 realizations
    assert len(vens["unsmry--yearly"]["REAL"].unique()) == 5
    assert len(vens["parameters.txt"]) == 5

    # This is the dataframe of discovered files in the ScratchRealization
    assert isinstance(vens["__files"], pd.DataFrame)
    assert not vens["__files"].empty

    assert "REAL" in vens["STATUS"].columns

    # Check shorthand functionality:
    assert (
        vens.shortcut2path("unsmry--yearly")
        == "share/results/tables/unsmry--yearly.csv"
    )
    assert (
        vens.shortcut2path("unsmry--yearly.csv")
        == "share/results/tables/unsmry--yearly.csv"
    )

    assert "npv.txt" in vens.keys()
    assert len(vens["npv.txt"]) == 5  # includes the 'error!' string in real4
    assert "outputs.txt" in vens.keys()
    assert len(vens["outputs.txt"]) == 4

    # Check that get_smry() works
    # (here is with no interpolation necessary)
    fopt = vens.get_smry(column_keys=["FOPT"], time_index="yearly")
    assert "FOPT" in fopt.columns
    assert "DATE" in fopt.columns
    assert "REAL" in fopt.columns
    assert "FGPT" not in fopt.columns
    assert len(fopt) == 25

    # Check that we can default get_smry()
    alldefaults = vens.get_smry()
    # This should glob to all columns, and monthly time frequency
    assert len(alldefaults) == 245
    assert len(alldefaults.columns) == 51

    # Eclipse summary vector statistics for a given ensemble
    df_stats = vens.get_smry_stats(column_keys=["FOPR", "FGPR"], time_index="yearly")
    assert isinstance(df_stats, pd.DataFrame)
    assert len(df_stats.columns) == 2
    assert isinstance(df_stats["FOPR"]["mean"], pd.Series)
    assert len(df_stats["FOPR"]["mean"]) == 5

    # Check webviz requirements for dataframe
    stats = df_stats.index.levels[0]
    assert "minimum" in stats
    assert "maximum" in stats
    assert "p10" in stats
    assert "p90" in stats
    assert "mean" in stats
    assert df_stats["FOPR"]["minimum"].iloc[-2] < df_stats["FOPR"]["maximum"].iloc[-2]

    # Test virtrealization retrieval:
    vreal = vens.get_realization(2)
    assert vreal.keys() == vens.keys()

    # Test realization removal:
    vens.remove_realizations(3)
    assert len(vens.parameters["REAL"].unique()) == 4
    assert len(vens) == 4
    vens.remove_realizations(3)  # This will give warning
    assert len(vens.parameters["REAL"].unique()) == 4
    assert len(vens["unsmry--yearly"]["REAL"].unique()) == 4
    assert len(vens) == 4

    # Test data removal:
    vens.remove_data("parameters.txt")
    assert "parameters.txt" not in vens.keys()
    vens.remove_data("bogus")  # This should only give warning

    # Test data addition. It should(?) work also for earlier nonexisting
    vens.append(
        "betterdata",
        pd.DataFrame(
            {
                "REAL": [0, 1, 2, 3, 4, 5, 6, 80],
                "NPV": [1000, 2000, 1500, 2300, 6000, 3000, 800, 9],
            }
        ),
    )
    assert "betterdata" in vens.keys()
    assert "REAL" in vens["betterdata"].columns
    assert "NPV" in vens["betterdata"].columns

    assert vens.get_realization(3).get_df("betterdata")["NPV"] == 2300
    assert vens.get_realization(0).get_df("betterdata")["NPV"] == 1000
    assert vens.get_realization(1).get_df("betterdata")["NPV"] == 2000
    assert vens.get_realization(2).get_df("betterdata")["NPV"] == 1500
    assert vens.get_realization(80).get_df("betterdata")["NPV"] == 9

    with pytest.raises(ValueError):
        vens.get_realization(9999)

    assert vens.shortcut2path("betterdata") == "betterdata"
    assert vens.agg("min").get_df("betterdata")["NPV"] == 9
    assert vens.agg("max").get_df("betterdata")["NPV"] == 6000
    assert (
        vens.agg("min").get_df("betterdata")["NPV"]
        < vens.agg("p07").get_df("betterdata")["NPV"]
    )
    assert (
        vens.agg("p05").get_df("betterdata")["NPV"]
        < vens.agg("p55").get_df("betterdata")["NPV"]
    )
    assert (
        vens.agg("p46").get_df("betterdata")["NPV"]
        < vens.agg("max").get_df("betterdata")["NPV"]
    )

    assert "REAL" not in vens.agg("min")["STATUS"].columns

    # Betterdata should be returned as a dictionary
    assert isinstance(vens.agg("min").get_df("betterdata"), dict)


def test_todisk():
    """Test that we can write VirtualEnsembles to the filesystem in a
    retrievable manner"""
    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")
    reekensemble = ScratchEnsemble(
        "reektest", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0"
    )

    reekensemble.load_smry(time_index="monthly", column_keys="*")
    reekensemble.load_smry(time_index="daily", column_keys="*")
    reekensemble.load_smry(time_index="yearly", column_keys="F*")
    reekensemble.load_scalar("npv.txt")
    reekensemble.load_txt("outputs.txt")
    vens = reekensemble.to_virtual()

    vens.to_disk("vens_dumped", delete=True)
    assert len(vens) == len(reekensemble)

    fromdisk = VirtualEnsemble(fromdisk="vens_dumped")

    # Same number of realizations:
    assert len(fromdisk) == len(vens)

    # Should have all the same keys,
    # but change of order is fine
    assert set(vens.keys()) == set(fromdisk.keys())

    for frame in vens.keys():
        if frame == "STATUS":
            continue
        # Columns that only contains NaN will not have their
        # type preserved, this is too much to ask for, especially
        # with CSV files. So we drop columns with NaN
        virtframe = vens.get_df(frame).dropna("columns")
        diskframe = fromdisk.get_df(frame).dropna("columns")

        assert (virtframe.columns == diskframe.columns).all()

        # It would be nice to be able to use pd.Dataframe.equals,
        # but it is too strict, as columns with mixed type number/strings
        # will easily be wrong.

        for column in virtframe.columns:
            if virtframe[column].dtype == object or diskframe[column].dtype == object:
                # Ensure we only compare strings when working with object dtype
                assert (
                    virtframe[column].astype(str).equals(diskframe[column].astype(str))
                )
            else:
                assert virtframe[column].equals(diskframe[column])

    fromdisk.to_disk("vens_double_dumped", delete=True)
    # Here we could check filesystem equivalence if we want.

    vens.to_disk("vens_dumped_csv", delete=True, dumpparquet=False)
    fromcsvdisk = VirtualEnsemble(fromdisk="vens_dumped_csv")
    lazyfromdisk = VirtualEnsemble(fromdisk="vens_dumped_csv", lazy_load=True)
    assert set(vens.keys()) == set(fromcsvdisk.keys())
    assert set(vens.keys()) == set(lazyfromdisk.keys())
    assert 'OK' in lazyfromdisk.lazy_frames.keys()
    assert 'OK' not in lazyfromdisk.data.keys()
    assert len(fromcsvdisk.get_df("OK")) == len(lazyfromdisk.get_df("OK"))
    assert 'OK' not in lazyfromdisk.lazy_frames.keys()
    assert 'OK' in lazyfromdisk.data.keys()
    assert len(fromcsvdisk.parameters) == len(lazyfromdisk.parameters)
    assert len(fromcsvdisk.get_df("unsmry--yearly")) == len(
        lazyfromdisk.get_df("unsmry--yearly")
    )

    vens.to_disk("vens_dumped_parquet", delete=True, dumpcsv=False)
    fromparquetdisk = VirtualEnsemble()
    fromparquetdisk.from_disk("vens_dumped_parquet")
    assert set(vens.keys()) == set(fromparquetdisk.keys())

    fromparquetdisk2 = VirtualEnsemble()
    fromparquetdisk2.from_disk("vens_dumped_parquet", fmt="csv")
    # Here we will miss a lot of CSV files, because we only wrote parquet:
    assert len(vens.keys()) > len(fromparquetdisk2.keys())

    fromcsvdisk2 = VirtualEnsemble()
    fromcsvdisk2.from_disk("vens_dumped_csv", fmt="parquet")
    # But even if we only try to load parquet files, when CSV
    # files are found without corresponding parquet, the CSV file
    # will be read.
    assert set(vens.keys()) == set(fromcsvdisk2.keys())

    # Test manual intervention:
    fooframe = pd.DataFrame(data=np.random.randn(3, 3), columns=["FOO", "BAR", "COM"])
    fooframe.to_csv(os.path.join("vens_dumped", "share/results/tables/randomdata.csv"))
    manualens = VirtualEnsemble(fromdisk="vens_dumped")
    assert "share/results/tables/randomdata.csv" not in manualens.keys()

    # Now with correct column header,
    # but floating point data for realizations..
    fooframe = pd.DataFrame(data=np.random.randn(3, 3), columns=["REAL", "BAR", "COM"])
    fooframe.to_csv(os.path.join("vens_dumped", "share/results/tables/randomdata.csv"))
    manualens = VirtualEnsemble(fromdisk="vens_dumped")
    assert "share/results/tables/randomdata.csv" not in manualens.keys()

    # Now with correct column header, and with integer data for REAL..
    fooframe = pd.DataFrame(
        data=np.random.randint(low=0, high=100, size=(3, 3)),
        columns=["REAL", "BAR", "COM"],
    )
    fooframe.to_csv(os.path.join("vens_dumped", "share/results/tables/randomdata.csv"))
    manualens = VirtualEnsemble(fromdisk="vens_dumped")
    assert "share/results/tables/randomdata.csv" in manualens.keys()


def test_todisk_includefile():
    """Test that we can write VirtualEnsembles to the filesystem in a
    retrievable manner with discovered files included"""
    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")
    reekensemble = ScratchEnsemble(
        "reektest", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0"
    )

    reekensemble.load_smry(time_index="monthly", column_keys="*")
    reekensemble.load_smry(time_index="daily", column_keys="*")
    reekensemble.load_smry(time_index="yearly", column_keys="F*")
    reekensemble.load_scalar("npv.txt")
    reekensemble.load_txt("outputs.txt")
    vens = reekensemble.to_virtual()

    vens.to_disk("vens_dumped_files", delete=True, includefiles=True, symlinks=True)
    for real in [0, 1, 2, 4, 4]:
        runpath = os.path.join(
            "vens_dumped_files", "__discoveredfiles", "realization-" + str(real)
        )
        assert os.path.exists(runpath)
        assert os.path.exists(
            os.path.join(runpath, "eclipse/model/2_R001_REEK-" + str(real) + ".UNSMRY")
        )


def test_get_smry_interpolation():
    """Test the summary resampling code for virtual ensembles"""

    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    reekensemble = ScratchEnsemble(
        "reektest", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0"
    )
    reekensemble.load_smry(time_index="yearly", column_keys=["F*"])
    reekensemble.load_scalar("npv.txt")
    vens_yearly = reekensemble.to_virtual()
    reekensemble.load_smry(time_index="monthly", column_keys=["F*"])
    # Create a vens that contains both monthly and yearly:
    vens_monthly = reekensemble.to_virtual()
    reekensemble.load_smry(time_index="daily", column_keys=["F*"])
    _ = reekensemble.to_virtual()  # monthly, yearly *and* daily

    # Resample yearly to monthly:
    monthly = vens_yearly.get_smry(column_keys="FOPT", time_index="monthly")
    assert "FOPT" in monthly.columns
    assert "REAL" in monthly.columns
    assert "DATE" in monthly.columns
    assert len(monthly["REAL"].unique()) == 5

    # 12 months pr. year, including final 1. jan, four years, 5 realizations:
    assert len(monthly) == (12 * 4 + 1) * 5

    for realidx in monthly["REAL"].unique():
        int_m = monthly.set_index("REAL").loc[realidx].set_index("DATE")
        true_m = (
            reekensemble.get_smry(column_keys="FOPT", time_index="monthly")
            .set_index("REAL")
            .loc[realidx]
            .set_index("DATE")
        )
        difference = int_m["FOPT"] - true_m["FOPT"]

        # The interpolation error should be zero at each 1st of January
        # but most likely nonzero elsewhere (at least for these realization)
        assert difference.loc["2001-01-01"] < 0.0001
        assert abs(difference.loc["2001-06-01"]) > 0
        assert difference.loc["2002-01-01"] < 0.0001
        assert abs(difference.loc["2002-06-01"]) > 0
        assert difference.loc["2003-01-01"] < 0.0001

    daily = vens_yearly.get_smry(column_keys=["FOPT", "FOPR"], time_index="daily")
    assert "FOPT" in daily.columns
    assert "REAL" in daily.columns
    assert "DATE" in daily.columns
    assert len(daily["REAL"].unique()) == 5
    assert len(daily) == (365 * 4 + 2) * 5  # 2003-01-01 and 2003-01-02 at end

    # Linear interpolation will give almost unique values everywhere:
    assert len(daily["FOPT"].unique()) > (365 * 4) * 5
    # While bfill for rates cannot be more unique than the yearly input
    assert len(daily["FOPR"].unique()) < 4 * 5  # Must be less than the numbers


def test_volumetric_rates():
    """Test the summary resampling code for virtual ensembles

    We only need to test the aggregation here.
    """

    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    reekensemble = ScratchEnsemble(
        "reektest", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0"
    )
    reekensemble.load_smry(time_index="yearly", column_keys=["F*"])
    reekensemble.load_scalar("npv.txt")
    vens = reekensemble.to_virtual()

    vol_rates = vens.get_volumetric_rates(column_keys="FOPT", time_index="yearly")
    assert isinstance(vol_rates, pd.DataFrame)
    assert "REAL" in vol_rates
    assert "DATE" in vol_rates
    assert "FOPR" in vol_rates
    assert len(vol_rates) == 25
