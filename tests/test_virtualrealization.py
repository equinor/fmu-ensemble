# -*- coding: utf-8 -*-
"""Testing fmu-ensemble."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import datetime

import numpy as np
import pandas as pd

import pytest

from fmu.ensemble import etc
from fmu import ensemble

fmux = etc.Interaction()
logger = fmux.basiclogger(__name__, level="WARNING")

if not fmux.testsetup():
    raise SystemExit()


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

    with pytest.raises(ValueError):
        vreal.get_df("bogusdataname")


def test_virtual_todisk(tmp="TMP"):
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

    with pytest.raises(IOError):
        vreal.to_disk(".")

    if not os.path.exists(tmp):
        os.mkdir(tmp)
    print(os.path.join(tmp, "virtreal1"))
    vreal.to_disk(os.path.join(tmp, "virtreal1"), delete=True)
    assert os.path.exists(os.path.join(tmp, "virtreal1/parameters.txt"))
    assert os.path.exists(os.path.join(tmp, "virtreal1/STATUS"))
    assert os.path.exists(
        os.path.join(tmp, "virtreal1/share/results/" + "tables/unsmry--yearly.csv")
    )
    assert os.path.exists(os.path.join(tmp, "virtreal1/npv.txt"))


def test_virtual_fromdisk(tmp="TMP"):
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
    if not os.path.exists(tmp):
        os.mkdir(tmp)
    # Virtualize and dump to disk:
    real.to_virtual().to_disk(os.path.join(tmp, "virtreal2"), delete=True)

    # Reload the virtualized realization back from disk:
    vreal = ensemble.VirtualRealization("foo")
    vreal.load_disk(os.path.join(tmp, "virtreal2"))

    for key in vreal.keys():
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
    vreal = real.to_virtual()

    assert len(vreal.get_smry(column_keys="FOPR", time_index="daily")["FOPR"]) == len(
        daily
    )

    assert len(vreal.get_smry(column_keys="FOPT", time_index="daily")["FOPT"]) == len(
        daily
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

    vreal = ensemble.VirtualRealization()

    assert isinstance(vreal._smry_cumulative([]), list)
    with pytest.raises(TypeError):
        vreal._smry_cumulative({})
    with pytest.raises(TypeError):
        vreal._smry_cumulative()
    assert vreal._smry_cumulative(["FOPT"])[0]
    assert not vreal._smry_cumulative(["FOPR"])[0]

    assert not vreal._smry_cumulative(["FWCT"])[0]
    assert vreal._smry_cumulative(["WOPT:A-1"])[0]
    assert not vreal._smry_cumulative(["WOPR:A-1T"])[0]


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
    assert len(vreal._glob_smry_keys(["FOP*"])) == 9

    assert len(vreal._glob_smry_keys("WOPT:*")) == 8
    assert all([x.startswith("WOPT:") for x in vreal._glob_smry_keys("WOPT:*")])

    assert not vreal._glob_smry_keys("FOOBAR")
