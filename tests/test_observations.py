# -*- coding: utf-8 -*-
"""Testing observations in fmu-ensemble."""

import os
import glob
import logging
import datetime
import yaml
import pandas as pd
import numpy as np
import dateutil
import pytest

from fmu.ensemble import Observations, ScratchRealization, ScratchEnsemble, EnsembleSet

logger = logging.getLogger(__name__)


def test_observation_import(tmpdir):
    """Test import of observations from yaml"""
    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    obs = Observations(
        testdir
        + "/data/testensemble-reek001/"
        + "/share/observations/"
        + "observations.yml"
    )
    assert len(obs.keys()) == 2  # adjust this..
    assert len(obs["smry"]) == 7
    assert len(obs["rft"]) == 2

    assert isinstance(obs["smry"], list)
    assert isinstance(obs["rft"], list)

    # Dump back to disk
    tmpdir.chdir()
    exportedfile = "share/observations/observations_copy.yml"
    obs.to_disk(exportedfile)
    assert os.path.exists(exportedfile)


def test_real_mismatch():
    """Test calculation of mismatch from the observation set to a
    realization"""
    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    real = ScratchRealization(
        testdir + "/data/testensemble-reek001/" + "realization-0/iter-0/"
    )

    real.load_smry()
    real.load_txt("outputs.txt")
    real.load_scalar("npv.txt")

    obs = Observations(
        {"txt": [{"localpath": "parameters.txt", "key": "FWL", "value": 1702}]}
    )
    realmis = obs.mismatch(real)

    # Check layout of returned data
    assert isinstance(realmis, pd.DataFrame)
    assert len(realmis) == 1
    assert "REAL" not in realmis.columns  # should only be there for ensembles.
    assert "OBSTYPE" in realmis.columns
    assert "OBSKEY" in realmis.columns
    assert "DATE" not in realmis.columns  # date is not relevant
    assert "MISMATCH" in realmis.columns
    assert "L1" in realmis.columns
    assert "L2" in realmis.columns

    # Check actually computed values, there should only be one row with data:
    assert realmis.loc[0, "OBSTYPE"] == "txt"
    assert realmis.loc[0, "OBSKEY"] == "parameters.txt/FWL"
    assert realmis.loc[0, "MISMATCH"] == -2
    assert realmis.loc[0, "SIGN"] == -1
    assert realmis.loc[0, "L1"] == 2
    assert realmis.loc[0, "L2"] == 4

    # Another observation set:
    obs2 = Observations(
        {
            "txt": [
                {"localpath": "parameters.txt", "key": "RMS_SEED", "value": 600000000},
                {"localpath": "outputs.txt", "key": "top_structure", "value": 3200},
            ],
            "scalar": [{"key": "npv.txt", "value": 3400}],
        }
    )
    realmis2 = obs2.mismatch(real)
    assert len(realmis2) == 3
    assert "parameters.txt/RMS_SEED" in realmis2["OBSKEY"].values
    assert "outputs.txt/top_structure" in realmis2["OBSKEY"].values
    assert "npv.txt" in realmis2["OBSKEY"].values

    # assert much more!

    # Test that we can write the observations to yaml
    # and verify that the exported yaml can be reimported
    # and yield the same result
    obs2r = Observations(yaml.full_load(obs2.to_yaml()))
    realmis2r = obs2r.mismatch(real)
    assert np.all(
        realmis2["MISMATCH"].values.sort() == realmis2r["MISMATCH"].values.sort()
    )

    # Test use of allocated values:
    obs3 = Observations({"smryh": [{"key": "FOPT", "histvec": "FOPTH"}]})
    fopt_mis = obs3.mismatch(real)
    assert fopt_mis.loc[0, "OBSTYPE"] == "smryh"
    assert fopt_mis.loc[0, "OBSKEY"] == "FOPT"
    assert fopt_mis.loc[0, "L1"] > 0
    assert fopt_mis.loc[0, "L1"] != fopt_mis.loc[0, "L2"]

    # Test mismatch where some data is missing:
    obs4 = Observations({"smryh": [{"key": "FOOBAR", "histvec": "FOOBARH"}]})
    mis_mis = obs4.mismatch(real)
    assert mis_mis.empty

    # This test fails, the consistency check is not implemented.
    # obs_bogus = Observations({'smryh': [{'keddy': 'FOOBAR',
    #                               'histdddvec': 'FOOBARH'}]})
    # mis_mis = obs_bogus.mismatch(real)
    # assert mis_mis.empty

    obs_bogus_scalar = Observations(
        {"scalar": [{"key": "nonexistingnpv.txt", "value": 3400}]}
    )
    # (a warning should be logged)
    assert obs_bogus_scalar.mismatch(real).empty

    obs_bogus_param = Observations(
        {
            "txt": [
                {
                    "localpath": "bogusparameters.txt",
                    "key": "RMS_SEED",
                    "value": 600000000,
                }
            ]
        }
    )
    # (a warning should be logged)
    assert obs_bogus_param.mismatch(real).empty

    obs_bogus_param = Observations(
        {
            "txt": [
                {
                    "localpath": "parameters.txt",
                    "key": "RMS_SEEEEEEED",
                    "value": 600000000,
                }
            ]
        }
    )
    # (a warning should be logged)
    assert obs_bogus_param.mismatch(real).empty

    # Non-existing summary key:
    obs_bogus_smry = Observations(
        {
            "smry": [
                {
                    "key": "WBP4:OP_XXXXX",
                    "observations": [
                        {"date": datetime.date(2001, 1, 1), "error": 4, "value": 251}
                    ],
                }
            ]
        }
    )
    assert obs_bogus_smry.mismatch(real).empty

    # Test dumping to yaml:
    # Not implemented.
    # yamlobsstr = obs2.to_yaml()
    # assert isinstance(yamlobsstr, str)
    # * Write yamlobsstr to tmp file
    # * Reload observation object from that file
    # * Check that the observation objects are the same


def test_smry():
    """Test the support for smry observations, these are
    observations relating to summary data, but where
    the observed values are specified in yaml, not through
    *H summary variables"""

    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    obs = Observations(
        testdir
        + "/data/testensemble-reek001/"
        + "/share/observations/"
        + "observations.yml"
    )
    real = ScratchRealization(
        testdir + "/data/testensemble-reek001/" + "realization-0/iter-0/"
    )

    # Compute the mismatch from this particular observation set to the
    # loaded realization.
    mismatch = obs.mismatch(real)

    assert len(mismatch) == 21  # later: implement counting in the obs object
    assert mismatch.L1.sum() > 0
    assert mismatch.L2.sum() > 0

    # This should work, but either the observation object
    # must do the smry interpolation in dataframes, or
    # the virtual realization should implement get_smry()
    # vreal = real.to_virtual()
    # vmismatch = obs.mismatch(vreal)
    # print(vmismatch)


def test_errormessages():
    """Test that we give ~sensible error messages when the
    observation input is unparseable"""

    # Emtpy
    with pytest.raises(TypeError):
        # pylint: disable=E1120
        Observations()

    # Non-existing filename:
    with pytest.raises(IOError):
        Observations("foobar")

    # Integer input does not make sense..
    with pytest.raises(ValueError):
        Observations(3)

    # Unsupported observation category, this foobar will be wiped
    emptyobs = Observations(dict(foobar="foo"))
    assert emptyobs.empty
    # (there will be logged a warning)

    # Empty observation set should be ok, but it must be a dict
    empty2 = Observations(dict())
    assert empty2.empty
    with pytest.raises(ValueError):
        Observations([])

    # Check that the dict is a dict of lists:
    assert Observations(dict(smry="not_a_list")).empty
    # (warning will be printed)

    # This should give a warning because 'observation' is missing
    wrongobs = Observations(
        {"smry": [{"key": "WBP4:OP_1", "comment": "Pressure observations well OP_1"}]}
    )
    assert wrongobs.empty


def test_smryh():
    """Test that smryh mismatch calculation will respect time index"""
    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    ens = ScratchEnsemble(
        "test", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0/"
    )

    obs_yearly = Observations(
        {"smryh": [{"key": "FOPT", "histvec": "FOPTH", "time_index": "yearly"}]}
    )
    obs_raw = Observations(
        {"smryh": [{"key": "FOPT", "histvec": "FOPTH", "time_index": "raw"}]}
    )
    obs_monthly = Observations(
        {"smryh": [{"key": "FOPT", "histvec": "FOPTH", "time_index": "monthly"}]}
    )
    obs_daily = Observations(
        {"smryh": [{"key": "FOPT", "histvec": "FOPTH", "time_index": "daily"}]}
    )
    obs_last = Observations(
        {"smryh": [{"key": "FOPT", "histvec": "FOPTH", "time_index": "last"}]}
    )
    obs_isodatestr = Observations(
        {"smryh": [{"key": "FOPT", "histvec": "FOPTH", "time_index": "2003-02-01"}]}
    )
    obs_future = Observations(
        {"smryh": [{"key": "FOPT", "histvec": "FOPTH", "time_index": "3003-02-01"}]}
    )
    obs_past = Observations(
        {"smryh": [{"key": "FOPT", "histvec": "FOPTH", "time_index": "1003-02-01"}]}
    )

    assert obs_isodatestr
    obs_isodate = Observations(
        {
            "smryh": [
                {
                    "key": "FOPT",
                    "histvec": "FOPTH",
                    "time_index": dateutil.parser.isoparse("2003-02-01"),
                }
            ]
        }
    )
    assert obs_isodate

    obs_error = Observations(
        {"smryh": [{"key": "FOPT", "histvec": "FOPTH", "time_index": "ølasjkdf"}]}
    )
    assert not obs_error
    obs_error2 = Observations(
        {"smryh": [{"key": "FOPT", "histvec": "FOPTH", "time_index": 4.43}]}
    )
    assert not obs_error2

    mismatchyearly = obs_yearly.mismatch(ens)
    mismatchmonthly = obs_monthly.mismatch(ens)
    mismatchdaily = obs_daily.mismatch(ens)
    mismatchlast = obs_last.mismatch(ens)
    mismatchraw = obs_raw.mismatch(ens)
    assert mismatchraw["TIME_INDEX"].unique() == ["raw"]

    mismatchdate = obs_isodate.mismatch(ens)
    assert "2003-02-01" in mismatchdate["TIME_INDEX"].unique()[0]

    mismatchdatestr = obs_isodatestr.mismatch(ens)
    # There might be a clock time included
    assert "2003-02-01" in mismatchdatestr["TIME_INDEX"].unique()[0]
    assert all(mismatchdate["L1"] == mismatchdatestr["L1"])

    mismatchfuture = obs_future.mismatch(ens)
    assert all(mismatchfuture["L1"] == mismatchlast["L1"])

    mismatchpast = obs_past.mismatch(ens)
    assert np.isclose(sum(mismatchpast["L2"]), 0.0)

    # When only one datapoint is included, these should be identical:
    assert (mismatchlast["L1"] == mismatchlast["L2"]).all()
    assert (mismatchlast["L1"] == mismatchlast["MISMATCH"].abs()).all()

    # Check that we have indeed calculated things differently between the time indices:
    assert mismatchyearly["L2"].sum != mismatchmonthly["L2"].sum()
    assert mismatchdaily["L2"].sum != mismatchraw["L2"].sum()


def test_ens_mismatch():
    """Test calculation of mismatch to ensemble data"""
    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")
    ens = ScratchEnsemble(
        "test", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0/"
    )

    obs = Observations({"smryh": [{"key": "FOPT", "histvec": "FOPTH"}]})

    mismatch = obs.mismatch(ens)

    assert "L1" in mismatch.columns
    assert "L2" in mismatch.columns
    assert "MISMATCH" in mismatch.columns
    assert "OBSKEY" in mismatch.columns
    assert "OBSTYPE" in mismatch.columns
    assert "REAL" in mismatch.columns
    assert len(mismatch) == len(ens) * 1  # number of observation units.

    fopt_rank = mismatch.sort_values("L2", ascending=True)["REAL"].values
    assert fopt_rank[0] == 2  # closest realization
    assert fopt_rank[-1] == 1  # worst realization

    # Try again with reference to non-existing vectors:
    obs = Observations({"smryh": [{"key": "FOPTFLUFF", "histvec": "FOPTFLUFFH"}]})
    mismatch = obs.mismatch(ens)
    assert mismatch.empty


def test_vens_mismatch():
    """Test calculation of mismatch to virtualized ensemble data"""
    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")
    ens = ScratchEnsemble(
        "test", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0/"
    )
    ens.load_smry(column_keys=["FOPT*"], time_index="monthly")

    vens = ens.to_virtual()

    # We don't need time_index now, because monthly is all we have.
    obs = Observations({"smryh": [{"key": "FOPT", "histvec": "FOPTH"}]})

    mismatch = obs.mismatch(vens)
    mismatch_raw = obs.mismatch(ens)
    assert isinstance(mismatch, pd.DataFrame)
    assert not mismatch.empty
    assert "L1" in mismatch.columns
    assert "L2" in mismatch.columns
    assert "MISMATCH" in mismatch.columns

    assert mismatch["MISMATCH"].sum() != mismatch_raw["MISMATCH"].sum()

    obs_monthly = Observations(
        {"smryh": [{"key": "FOPT", "histvec": "FOPTH", "time_index": "monthly"}]}
    )
    assert (
        (
            mismatch.sort_values("REAL")
            .reset_index(drop=True)
            .drop("TIME_INDEX", axis=1)
            == obs_monthly.mismatch(ens)
            .sort_values("REAL")
            .reset_index(drop=True)
            .drop("TIME_INDEX", axis=1)
        )
        .all()
        .all()
    )

    # We should be able to do yearly smryh comparisons from virtualized
    # monthly profiles:
    obs_yearly = Observations(
        {"smryh": [{"key": "FOPT", "histvec": "FOPTH", "time_index": "yearly"}]}
    )
    mismatch_yearly = obs_yearly.mismatch(vens)
    assert mismatch_yearly["MISMATCH"].sum() != mismatch["MISMATCH"].sum()

    # When load_smry() is forgotten before virtualization:
    vens = ScratchEnsemble(
        "test", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0/"
    ).to_virtual()
    with pytest.raises(ValueError):
        obs.mismatch(vens)

    # Removal of one realization in the virtualized ensemble:
    ens = ScratchEnsemble(
        "test", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0/"
    )
    ens.load_smry(column_keys=["FOPT*"], time_index="monthly")
    vens = ens.to_virtual()
    vens.remove_realizations(2)
    mismatch_subset = obs.mismatch(vens)
    assert 2 not in mismatch_subset["REAL"].unique()
    assert 0 in mismatch_subset["REAL"].unique()


def test_ens_failedreals():
    """Ensure we can calculate mismatch where some realizations
    do not have UNSMRY data"""
    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")
    ens = ScratchEnsemble(
        "test",
        testdir + "/data/testensemble-reek001/" + "realization-*/iter-0/",
        autodiscovery=False,
    )
    obs = Observations({"smryh": [{"key": "FOPT", "histvec": "FOPTH"}]})
    mismatch = obs.mismatch(ens)

    # There are no UNSMRY found, so the mismatch should be empty:
    assert mismatch.empty

    ens.find_files("eclipse/model/*UNSMRY")
    assert not obs.mismatch(ens).empty

    # Reinitialize
    ens = ScratchEnsemble(
        "test",
        testdir + "/data/testensemble-reek001/" + "realization-*/iter-0/",
        autodiscovery=False,
    )

    # Redirect UNSMRY pointer in realizaion 3 so it isn't found
    ens.find_files("eclipse/model/*UNSMRY")
    real3files = ens[3].files
    real3files.loc[real3files["FILETYPE"] == "UNSMRY", "FULLPATH"] = "FOO"

    # Check that we only have EclSum for 2 and not for 3:
    assert ens[2].get_eclsum()
    assert not ens[3].get_eclsum()

    missingsmry = obs.mismatch(ens)
    # Realization 3 should NOT be present now
    assert 3 not in list(missingsmry["REAL"])
    assert not obs.mismatch(ens).empty


def test_ensset_mismatch():
    """Test mismatch calculation on an EnsembleSet"""
    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    ensdir = os.path.join(testdir, "data/testensemble-reek001/")

    # Copy iter-0 to iter-1, creating an identical ensemble
    # we can load for testing.
    for realizationdir in glob.glob(ensdir + "/realization-*"):
        if os.path.exists(realizationdir + "/iter-1"):
            os.remove(realizationdir + "/iter-1")
        os.symlink(realizationdir + "/iter-0", realizationdir + "/iter-1")

    iter0 = ScratchEnsemble("iter-0", ensdir + "/realization-*/iter-0")
    iter1 = ScratchEnsemble("iter-1", ensdir + "/realization-*/iter-1")

    ensset = EnsembleSet("reek001", [iter0, iter1])

    obs = Observations({"smryh": [{"key": "FOPT", "histvec": "FOPTH"}]})

    mismatch = obs.mismatch(ensset)
    assert "ENSEMBLE" in mismatch.columns
    assert "REAL" in mismatch.columns
    assert len(mismatch) == 10
    assert (
        mismatch[mismatch.ENSEMBLE == "iter-0"].L1.sum()
        == mismatch[mismatch.ENSEMBLE == "iter-1"].L1.sum()
    )

    # This is quite hard to input in dict-format. Better via YAML..
    obs_pr = Observations(
        {
            "smry": [
                {
                    "key": "WBP4:OP_1",
                    "comment": "Pressure observations well OP_1",
                    "observations": [
                        {"value": 250, "error": 1, "date": datetime.date(2001, 1, 1)}
                    ],
                }
            ]
        }
    )

    mis_pr = obs_pr.mismatch(ensset)
    assert len(mis_pr) == 10

    # We should also be able to input dates as strings, and they
    # should be attempted parsed to datetime.date:
    obs_pr = Observations(
        {
            "smry": [
                {
                    "key": "WBP4:OP_1",
                    "observations": [{"value": 250, "error": 1, "date": "2001-01-01"}],
                }
            ]
        }
    )
    mis_pr2 = obs_pr.mismatch(ensset)
    assert len(mis_pr2) == 10

    # We are strict and DO NOT ALLOW non-ISO dates like this:
    with pytest.raises(ValueError):
        obs_pr = Observations(
            {
                "smry": [
                    {
                        "key": "WBP4:OP_1",
                        "observations": [
                            {"value": 250, "error": 1, "date": "01-01-2001"}
                        ],
                    }
                ]
            }
        )

    # Erroneous date will raise Exception
    # (but a valid date will give an extrapolated value)
    with pytest.raises(ValueError):
        obs_pr = Observations(
            {
                "smry": [
                    {
                        "key": "WBP4:OP_1",
                        "observations": [
                            {"value": 250, "error": 1, "date": "3011-45-443"}
                        ],
                    }
                ]
            }
        )
    obs_extrap = Observations(
        {
            "smry": [
                {
                    "key": "WBP4:OP_1",
                    "observations": [{"value": 250, "error": 1, "date": "1977-01-01"}],
                }
            ]
        }
    )
    assert len(obs_extrap.mismatch(ensset)) == 10  # (5 reals, 2 ensembles)


def test_virtual_observations():
    """Construct an virtual(?) observation object from a specific summary vector
    and use it to rank realizations for similarity.
    """

    # We need an ensemble to work with:
    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")
    ens = ScratchEnsemble(
        "test", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0/"
    )
    ens.load_smry(
        column_keys=["FOPT", "FGPT", "FWPT", "FWCT", "FGOR"], time_index="yearly"
    )

    # And we need some VirtualRealizations
    virtreals = {
        "p90realization": ens.agg("p90"),
        "meanrealization": ens.agg("mean"),
        "p10realization": ens.agg("p10"),
    }

    summaryvector = "FOPT"
    representative_realizations = {}
    for virtrealname, virtreal in virtreals.items():
        # Create empty observation object
        obs = Observations({})
        obs.load_smry(virtreal, summaryvector, time_index="yearly")

        # Calculate how far each realization is from this observation set
        # (only one row pr. realization, as FOPTH is only one observation unit)
        mis = obs.mismatch(ens)

        closest_realization = (
            mis.groupby("REAL").sum()["L2"].sort_values().index.values[0]
        )
        representative_realizations[virtrealname] = closest_realization

    assert representative_realizations["meanrealization"] == 4
    assert representative_realizations["p90realization"] == 2
    assert representative_realizations["p10realization"] == 1

    # Test again with the ensemble virtualized:
    vens = ens.to_virtual()

    # And we need some VirtualRealizations
    vvirtreals = {
        "p90realization": vens.agg("p90"),
        "meanrealization": vens.agg("mean"),
        "p10realization": vens.agg("p10"),
    }

    summaryvector = "FOPT"
    vrepresentative_realizations = {}
    for virtrealname, virtreal in vvirtreals.items():
        # Create empty observation object
        obs = Observations({})
        obs.load_smry(virtreal, summaryvector, time_index="yearly")

        mis = obs.mismatch(ens)

        closest_realization = (
            mis.groupby("REAL").sum()["L2"].sort_values().index.values[0]
        )
        vrepresentative_realizations[virtrealname] = closest_realization

    assert vrepresentative_realizations["meanrealization"] == 4
    assert vrepresentative_realizations["p90realization"] == 2
    assert vrepresentative_realizations["p10realization"] == 1
