"""Testing fmu-ensemble, virtual realizations"""
# pylint: disable=protected-access,duplicate-code

import os
import logging
import datetime

import numpy as np
import pandas as pd

import pytest

from fmu.ensemble.virtualrealization import smry_cumulative
from fmu import ensemble

logger = logging.getLogger(__name__)


def test_virtual_realization():
    """Test making av virtual realization from
    a fresh ScratchRealization, and veryfing that the
    internalized data was conserved"""

    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    realdir = os.path.join(testdir, "data/testensemble-reek001", "realization-0/iter-0")
    real = ensemble.ScratchRealization(realdir)

    # Check deepcopy(), first prove a bad situation
    vreal = real.to_virtual(deepcopy=False)
    assert "parameters.txt" in real.keys()
    del vreal["parameters.txt"]
    # This is a bad situation:
    assert "parameters.txt" not in real.keys()

    # Now confirm that we can fix the bad
    # situation with the default to_virtual()
    real = ensemble.ScratchRealization(realdir)
    vreal = real.to_virtual()
    del vreal["parameters.txt"]
    assert "parameters.txt" in real.keys()

    real = ensemble.ScratchRealization(realdir)
    vreal = real.to_virtual()
    assert real.keys() == vreal.keys()

    # Test appending a random dictionary betteroutput
    vreal.append("betteroutput", {"NPV": 200000000, "BREAKEVEN": 8.4})
    assert vreal.get_df("betteroutput")["NPV"] > 0
    # Appending to a key that exists should not help
    vreal.append("betteroutput", {"NPV": -300, "BREAKEVEN": 300})
    assert vreal.get_df("betteroutput")["NPV"] > 0
    # Unless we overwrite explicitly:
    vreal.append("betteroutput", {"NPV": -300, "BREAKEVEN": 300}, overwrite=True)
    assert vreal.get_df("betteroutput")["NPV"] < 0

    with pytest.raises((KeyError, ValueError)):
        vreal.get_df("bogusdataname")


def test_virtual_todisk(tmpdir):
    """Test writing a virtual realization to disk (as a directory with files)"""
    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    realdir = os.path.join(testdir, "data/testensemble-reek001", "realization-0/iter-0")
    real = ensemble.ScratchRealization(realdir)
    real.load_smry(time_index="yearly", column_keys=["F*"])
    real.load_scalar("npv.txt")

    vreal = real.to_virtual()
    assert "npv.txt" in vreal.keys()

    tmpdir.chdir()

    with pytest.raises(IOError):
        vreal.to_disk("/")

    print("virtreal1")
    vreal.to_disk("virtreal1", delete=True)
    assert os.path.exists("virtreal1/parameters.txt")
    assert os.path.exists("virtreal1/STATUS")
    assert os.path.exists("virtreal1/share/results/tables/unsmry--yearly.csv")
    assert os.path.exists("virtreal1/npv.txt")


def test_virtual_fromdisk(tmpdir):
    """Test retrieval of a virtualrealization that
    has been dumped to disk"""
    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    # First make a ScratchRealization to virtualize and dump:
    realdir = os.path.join(testdir, "data/testensemble-reek001", "realization-0/iter-0")
    real = ensemble.ScratchRealization(realdir)
    # Internalize some data that we can test for afterwards
    real.load_smry(time_index="yearly", column_keys=["F*"])
    real.load_scalar("npv.txt")

    tmpdir.chdir()

    # Virtualize and dump to disk:
    real.to_virtual().to_disk("virtreal2", delete=True)

    # Reload the virtualized realization back from disk:
    vreal = ensemble.VirtualRealization("foo")
    vreal.load_disk("virtreal2")

    for key in vreal.keys():
        if key != "__smry_metadata":
            if isinstance(real.get_df(key), (pd.DataFrame, dict)):
                assert len(real.get_df(key)) == len(vreal.get_df(key))
            else:  # Scalars:
                assert real.get_df(key) == vreal.get_df(key)
    assert real.get_df("parameters")["FWL"] == vreal.get_df("parameters")["FWL"]
    assert (
        real.get_df("unsmry--yearly").iloc[-1]["FGIP"]
        == vreal.get_df("unsmry--yearly").iloc[-1]["FGIP"]
    )
    assert real.get_df("npv.txt") == 3444


def test_get_smry():
    """Check that we can to get_smry() on virtual realizations"""
    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    realdir = os.path.join(testdir, "data/testensemble-reek001", "realization-0/iter-0")
    real = ensemble.ScratchRealization(realdir)
    real.load_smry(time_index="yearly", column_keys=["F*"])
    vreal = real.to_virtual()
    monthly = vreal.get_smry(
        column_keys=["FOPT", "FOPR", "FGPR", "FWCT"], time_index="monthly"
    )
    assert "FOPT" in monthly.columns
    assert len(monthly) > 20
    assert "FOPR" in monthly.columns
    assert len(monthly) == len(monthly.dropna())

    vfopt = vreal.get_smry(column_keys="FOPT", time_index="yearly")
    fopt = real.get_smry(column_keys="FOPT", time_index="yearly")
    assert all(vfopt == fopt)
    # But note that the dtype of the index in each dataframe differs
    # vfopt.index.dtype == datetime, while fopt.index.dtype == object
    assert len(fopt.columns) == 1  # DATE is index (unlabeled)

    dvfopt = vreal.get_smry(column_keys="FOPT", time_index="daily")
    assert all(dvfopt.diff() >= 0)
    # Linear interpolation should give many unique values:
    assert len(dvfopt["FOPT"].unique()) == 1462
    # Length is here 1462 while daily smry for the scratchrealization
    # would give 1098 (one year less) - this is correct here
    # since we only have yearly dates to interpolate from.

    dvfopr = vreal.get_smry(column_keys="FOPR", time_index="daily")
    # FOPR is bfill'ed and should not have many unique values:
    assert len(dvfopr["FOPR"].unique()) == 4

    # Try with custom datetimes
    long_time_ago = [datetime.date(1978, 5, 6), datetime.date(1988, 5, 6)]
    assert all(
        vreal.get_smry(column_keys=["FOPR", "FOPT"], time_index=long_time_ago) == 0
    )
    before_and_after = [datetime.date(1900, 1, 1), datetime.date(2100, 1, 1)]

    assert all(
        vreal.get_smry(
            column_keys=["FOPR", "FOPT"], time_index=before_and_after
        ).sort_index(axis=1)
        == real.get_smry(
            column_keys=["FOPR", "FOPT"], time_index=before_and_after
        ).sort_index(axis=1)
    )

    # If you supply repeating timeindices, you get duplicates out
    # (only duplicates between existing and supplied timesteps are
    # removed)
    repeating = [datetime.date(2002, 2, 1), datetime.date(2002, 2, 1)]
    assert len(vreal.get_smry(column_keys="FOPR", time_index=repeating)) == len(
        repeating
    )

    # Test that time_index='first' and ='last' is supported on a virtual realization
    real = ensemble.ScratchRealization(realdir)
    monthly_smry = real.load_smry(time_index="monthly", column_keys=["FOIP"])
    vreal = real.to_virtual()
    pd.testing.assert_series_equal(
        vreal.get_smry(time_index="first")["FOIP"].reset_index(drop=True),
        monthly_smry[monthly_smry["DATE"] == min(monthly_smry["DATE"])][
            "FOIP"
        ].reset_index(drop=True),
    )
    pd.testing.assert_series_equal(
        vreal.get_smry(time_index="last")["FOIP"].reset_index(drop=True),
        monthly_smry[monthly_smry["DATE"] == max(monthly_smry["DATE"])][
            "FOIP"
        ].reset_index(drop=True),
    )


def test_get_smry2():
    """More tests for get_smry, with more choices in
    what is internalized"""
    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    realdir = os.path.join(testdir, "data/testensemble-reek001", "realization-0/iter-0")
    real = ensemble.ScratchRealization(realdir)
    real.load_smry(time_index="yearly", column_keys=["F*"])
    real.load_smry(time_index="monthly", column_keys=["F*"])
    daily = real.load_smry(time_index="daily", column_keys=["F*"])
    real.load_smry(time_index="raw", column_keys=["F*"])
    real.load_smry(time_index=None, column_keys=["F*"])
    vreal = real.to_virtual()

    assert len(vreal.get_smry(column_keys="FOPR", time_index="daily")["FOPR"]) == len(
        daily
    )

    assert len(vreal.get_smry(column_keys="FOPT", time_index="daily")["FOPT"]) == len(
        daily
    )

    # Check that time_index=None and time_index='raw' are equal
    pd.testing.assert_series_equal(
        vreal.get_smry(time_index="raw")["FOPT"].reset_index(drop=True),
        vreal.get_smry(time_index=None)["FOPT"].reset_index(drop=True),
    )

    daily_dt = vreal.get_smry_dates("daily")
    # If we now ask for daily, we probably pick from 'raw' as it is
    # internalized.
    daily2 = vreal.get_smry(column_keys=["FOPR", "FOPT"], time_index=daily_dt)
    assert len(daily2["FOPR"].unique()) == len(daily["FOPR"].unique())
    assert len(daily2["FOPT"].unique()) == len(daily["FOPT"].unique())

    # Check defaults handling:
    monthly_length = len(vreal.get_smry(column_keys="FOPR", time_index="monthly"))
    assert len(vreal.get_smry(column_keys="FOPR")) == monthly_length

    alldefaults = vreal.get_smry()
    assert len(alldefaults) == monthly_length
    assert len(alldefaults.columns) == 49


def test_get_smry_cumulative():
    """Test the cumulative boolean function"""

    assert isinstance(smry_cumulative([]), list)
    with pytest.raises(TypeError):
        smry_cumulative({})
    with pytest.raises(TypeError):
        # pylint: disable=no-value-for-parameter
        smry_cumulative()
    assert smry_cumulative(["FOPT"])[0]
    assert not smry_cumulative(["FOPR"])[0]

    assert not smry_cumulative(["FWCT"])[0]
    assert smry_cumulative(["WOPT:A-1"])[0]
    assert not smry_cumulative(["WOPR:A-1T"])[0]


def test_get_smry_dates():
    """Test date grid functionality from a virtual realization.

    Already internalized summary data is needed for this"""

    # First test with no data:
    empty_vreal = ensemble.VirtualRealization()
    with pytest.raises(ValueError):
        empty_vreal.get_smry_dates()

    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    realdir = os.path.join(testdir, "data/testensemble-reek001", "realization-0/iter-0")
    real = ensemble.ScratchRealization(realdir)
    real.load_smry(time_index="yearly", column_keys=["F*", "W*"])
    vreal = real.to_virtual()

    assert len(vreal.get_smry_dates(freq="monthly")) == 49
    assert len(vreal.get_smry_dates(freq="daily")) == 1462
    assert len(vreal.get_smry_dates(freq="yearly")) == 5

    with pytest.raises(ValueError):
        assert vreal.get_smry_dates(freq="foobar")


def test_volumetric_rates():
    """Test computation of volumetric rates from cumulative vectors

    This function is primarily tested in test_realization.py. Here
    we only check that the wrapper in VirtualRealization is actually
    working
    """
    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    realdir = os.path.join(testdir, "data/testensemble-reek001", "realization-0/iter-0")
    real = ensemble.ScratchRealization(realdir)
    fopt = real.load_smry(column_keys="FOPT", time_index="monthly")
    vreal = real.to_virtual()
    fopr = vreal.get_volumetric_rates(column_keys="FOPT", time_index="monthly")
    assert fopt["FOPT"].iloc[-1] == pytest.approx(fopr["FOPR"].sum())
    fopr = vreal.get_volumetric_rates(
        column_keys="FOPT", time_index="yearly", time_unit="months"
    )
    assert all(np.isfinite(fopr["FOPR"]))


def test_glob_smry_keys():
    """Test the globbing function for virtual realization"""
    empty_vreal = ensemble.VirtualRealization()
    with pytest.raises(ValueError):
        empty_vreal._glob_smry_keys("FOP*")

    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    realdir = os.path.join(testdir, "data/testensemble-reek001", "realization-0/iter-0")
    real = ensemble.ScratchRealization(realdir)
    real.load_smry(time_index="yearly", column_keys=["F*", "W*"])
    vreal = real.to_virtual()

    assert len(vreal._glob_smry_keys("FOP*")) == 9
    assert len(vreal._glob_smry_keys("FOP?")) == 3
    assert len(vreal._glob_smry_keys(["FOP*"])) == 9

    assert len(vreal._glob_smry_keys("WOPT:*")) == 8
    assert all([x.startswith("WOPT:") for x in vreal._glob_smry_keys("WOPT:*")])

    assert not vreal._glob_smry_keys("FOOBAR")


def test_get_smry_meta():
    """Test that summary meta information is preserved through
    virtualization
    """
    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    realdir = os.path.join(testdir, "data/testensemble-reek001", "realization-0/iter-0")
    real = ensemble.ScratchRealization(realdir)
    real.load_smry(column_keys="*", time_index="yearly")
    vreal = real.to_virtual()

    meta = vreal.get_smry_meta()
    assert "FOPT" in meta
    assert "WOPR:OP_1" in meta

    assert meta["FOPT"]["wgname"] is None


def test_get_df_merge():
    """Test the merge support in get_df. Could be tricky for virtualrealizations
    since not everything is dataframes"""
    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    realdir = os.path.join(testdir, "data/testensemble-reek001", "realization-0/iter-0")
    real = ensemble.ScratchRealization(realdir)
    unsmry = real.load_smry(column_keys="*", time_index="yearly")
    real.load_csv("share/results/volumes/simulator_volume_fipnum.csv")
    real.load_txt("outputs.txt")
    real.load_scalar("npv.txt")
    vreal = real.to_virtual()

    assert len(vreal.get_df("unsmry--yearly", merge="parameters").columns) == len(
        unsmry.columns
    ) + len(real.parameters)

    smryoutput = vreal.get_df("unsmry--yearly", merge="outputs")
    assert "top_structure" in smryoutput.columns

    paramoutput = vreal.get_df("parameters", merge="outputs")
    assert "SORG1" in paramoutput
    assert "top_structure" in paramoutput

    output_scalar = vreal.get_df("outputs", merge="npv.txt")
    assert "npv.txt" in output_scalar
    assert "top_structure" in output_scalar

    output_scalar = vreal.get_df("npv.txt", merge="outputs")
    assert "npv.txt" in output_scalar
    assert "top_structure" in output_scalar

    # Try merging dataframes:
    real.load_csv("share/results/volumes/simulator_volume_fipnum.csv")

    # Inject a mocked dataframe to the realization:
    real.data["fipnum2zone"] = pd.DataFrame(
        columns=["FIPNUM", "ZONE"],
        data=[
            [1, "UpperReek"],
            [2, "MidReek"],
            [3, "LowerReek"],
            [4, "UpperReek"],
            [5, "MidReek"],
            [6, "LowerReek"],
        ],
    )
    volframe = real.get_df("simulator_volume_fipnum", merge="fipnum2zone")

    assert "ZONE" in volframe
    assert "FIPNUM" in volframe
    assert "STOIIP_OIL" in volframe
    assert len(volframe["ZONE"].unique()) == 3
