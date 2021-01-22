"""Testing fmu-ensemble."""
# pylint: disable=protected-access

import os
import logging

import yaml
import numpy
import pandas as pd

import pytest

from .test_ensembleset import symlink_iter

from fmu.ensemble import ScratchEnsemble, ScratchRealization


try:
    SKIP_FMU_TOOLS = False
    from fmu.tools import volumetrics
except ImportError:
    SKIP_FMU_TOOLS = True

logger = logging.getLogger(__name__)


def test_reek001(tmpdir):
    """Test import of a stripped 5 realization ensemble"""

    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    reekensemble = ScratchEnsemble(
        "reektest", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0"
    )
    assert isinstance(reekensemble, ScratchEnsemble)
    assert reekensemble.name == "reektest"
    assert len(reekensemble) == 5

    assert isinstance(reekensemble[0], ScratchRealization)

    assert len(reekensemble.files[reekensemble.files.LOCALPATH == "jobs.json"]) == 5
    assert (
        len(reekensemble.files[reekensemble.files.LOCALPATH == "parameters.txt"]) == 5
    )
    assert len(reekensemble.files[reekensemble.files.LOCALPATH == "STATUS"]) == 5

    statusdf = reekensemble.get_df("STATUS")
    assert len(statusdf) == 250  # 5 realizations, 50 jobs in each
    assert "REAL" in statusdf.columns
    assert "FORWARD_MODEL" in statusdf.columns
    statusdf = statusdf.set_index(["REAL", "FORWARD_MODEL"]).sort_index()
    assert "DURATION" in statusdf.columns  # calculated
    assert "argList" in statusdf.columns  # from jobs.json

    # Sample check the duration for RMS in realization 4:
    assert int(statusdf.loc[4, "RMS_BATCH"]["DURATION"].values[0]) == 195

    # STATUS in real4 is modified to simulate that Eclipse never finished:
    assert numpy.isnan(statusdf.loc[4, "ECLIPSE100_2014.2"]["DURATION"].values[0])

    tmpdir.chdir()
    statusdf.to_csv("status.csv", index=False)

    # Parameters.txt
    paramsdf = reekensemble.load_txt("parameters.txt")
    assert len(paramsdf) == 5  # 5 realizations
    paramsdf = reekensemble.parameters  # also test as property
    paramsdf = reekensemble.get_df("parameters.txt")
    assert len(paramsdf) == 5
    assert len(paramsdf.columns) == 26  # 25 parameters, + REAL column
    paramsdf.to_csv("params.csv", index=False)

    # Check that the ensemble object has not tainted the realization dataframe:
    assert "REAL" not in reekensemble.realizations[0].get_df("parameters.txt")

    # The column FOO in parameters is only present in some, and
    # is present with NaN in real0:
    assert "FOO" in reekensemble.parameters.columns
    assert len(reekensemble.parameters["FOO"].dropna()) == 1
    # (NaN ine one real, and non-existing in the others is the same thing)

    # Test loading of another txt file:
    reekensemble.load_txt("outputs.txt")
    assert "NPV" in reekensemble.load_txt("outputs.txt").columns
    # Check implicit discovery
    assert "outputs.txt" in reekensemble.files["LOCALPATH"].values
    assert all([os.path.isabs(x) for x in reekensemble.files["FULLPATH"]])

    # File discovery:
    csvvolfiles = reekensemble.find_files(
        "share/results/volumes/*csv", metadata={"GRID": "simgrid"}
    )
    assert isinstance(csvvolfiles, pd.DataFrame)
    assert "REAL" in csvvolfiles
    assert "FULLPATH" in csvvolfiles
    assert "LOCALPATH" in csvvolfiles
    assert "BASENAME" in csvvolfiles
    # Check the explicit metadata:
    assert "GRID" in csvvolfiles
    assert csvvolfiles["GRID"].unique() == ["simgrid"]

    reekensemble.files.to_csv("files.csv", index=False)

    # Check that rediscovery does not mess things up:

    filecount = len(reekensemble.files)
    newfiles = reekensemble.find_files("share/results/volumes/*csv")
    # Also note that we skipped metadata here in rediscovery:

    assert len(reekensemble.files) == filecount
    assert len(newfiles) == len(csvvolfiles)

    # The last invocation of find_files() should not return the metadata
    assert len(newfiles.columns) + 1 == len(csvvolfiles.columns)

    # FULLPATH should always contain absolute paths
    assert all([os.path.isabs(x) for x in reekensemble.files["FULLPATH"]])

    # The metadata in the rediscovered files should have been removed
    assert reekensemble.files[reekensemble.files["GRID"] == "simgrid"].empty

    # CSV files
    csvpath = "share/results/volumes/simulator_volume_fipnum.csv"
    vol_df = reekensemble.load_csv(csvpath)

    # Check that we have not tainted the realization dataframes:
    assert "REAL" not in reekensemble.realizations[0].get_df(csvpath)

    assert "REAL" in vol_df
    assert len(vol_df["REAL"].unique()) == 3  # missing in 2 reals
    vol_df.to_csv("simulatorvolumes.csv", index=False)

    # Test retrival of cached data
    vol_df2 = reekensemble.get_df(csvpath)

    assert "REAL" in vol_df2
    assert len(vol_df2["REAL"].unique()) == 3  # missing in 2 reals

    # Realization deletion:
    reekensemble.remove_realizations([1, 3])
    assert len(reekensemble) == 3

    # Readd the same realizations
    reekensemble.add_realizations(
        [
            testdir + "/data/testensemble-reek001/" + "realization-1/iter-0",
            testdir + "/data/testensemble-reek001/" + "realization-3/iter-0",
        ]
    )
    assert len(reekensemble) == 5
    print(reekensemble.files)
    assert len(reekensemble.files) == 24

    # File discovery must be repeated for the newly added realizations
    reekensemble.find_files(
        "share/results/volumes/" + "simulator_volume_fipnum.csv",
        metadata={"GRID": "simgrid"},
    )
    assert len(reekensemble.files) == 25
    # Test addition of already added realization:
    reekensemble.add_realizations(
        testdir + "/data/testensemble-reek001/" + "realization-1/iter-0"
    )
    assert len(reekensemble) == 5
    assert len(reekensemble.files) == 24  # discovered files are lost!

    keycount = len(reekensemble.keys())
    reekensemble.remove_data("parameters.txt")
    assert len(reekensemble.keys()) == keycount - 1


def test_noparameters():
    """Test what happens when parameters.txt is missing"""

    testdir = os.path.dirname(os.path.abspath(__file__))
    reekensemble = ScratchEnsemble(
        "reektest", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0"
    )
    # Parameters.txt exist on disk, so it is loaded:
    assert not reekensemble.parameters.empty
    # Remove it each realization:
    reekensemble.remove_data("parameters.txt")
    assert reekensemble.parameters.empty

    # However, when parameters.txt is excplicitly asked for,
    # an exception should be raised:
    with pytest.raises(KeyError):
        reekensemble.get_df("parameters.txt")

    reekensemble.load_smry(time_index="yearly", column_keys="FOPT")
    assert not reekensemble.get_df("unsmry--yearly").empty
    with pytest.raises(KeyError):
        reekensemble.get_df("unsmry--yearly", merge="parameters.txt")


def test_emptyens():
    """Check that we can initialize an empty ensemble"""
    ens = ScratchEnsemble("emptyens")
    assert not ens

    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    emptydf = ens.get_smry()
    assert isinstance(emptydf, pd.DataFrame)
    assert emptydf.empty

    emptydatelist = ens.get_smry_dates()
    assert isinstance(emptydatelist, list)
    assert not emptydatelist

    emptykeys = ens.get_smrykeys()
    assert isinstance(emptykeys, list)
    assert not emptykeys

    emptyrates = ens.get_volumetric_rates()
    assert isinstance(emptyrates, pd.DataFrame)
    assert emptyrates.empty

    emptystats = ens.get_smry_stats()
    assert isinstance(emptystats, pd.DataFrame)
    assert emptystats.empty

    emptywells = ens.get_wellnames()
    assert isinstance(emptywells, list)
    assert not emptywells

    emptygroups = ens.get_groupnames()
    assert isinstance(emptygroups, list)
    assert not emptygroups

    emptymeta = ens.get_smry_meta()
    assert isinstance(emptymeta, dict)
    assert not emptymeta

    emptymeta = ens.get_smry_meta("*")
    assert isinstance(emptymeta, dict)
    assert not emptymeta

    emptymeta = ens.get_smry_meta("FOPT")
    assert isinstance(emptymeta, dict)
    assert not emptymeta

    emptymeta = ens.get_smry_meta(["FOPT"])
    assert isinstance(emptymeta, dict)
    assert not emptymeta

    # Add a realization manually:
    ens.add_realizations(
        testdir + "/data/testensemble-reek001/" + "realization-0/iter-0"
    )
    assert len(ens) == 1


def test_reek001_scalars():
    """Test import of scalar values from files

    Files with scalar values can contain numerics or strings,
    or be empty."""

    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    reekensemble = ScratchEnsemble(
        "reektest", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0"
    )

    assert "OK" in reekensemble.keys()
    assert isinstance(reekensemble.get_df("OK"), pd.DataFrame)
    assert len(reekensemble.get_df("OK")) == 5

    # One of the npv.txt files contains the string "error!"
    reekensemble.load_scalar("npv.txt")
    npv = reekensemble.get_df("npv.txt")
    assert isinstance(npv, pd.DataFrame)
    assert "REAL" in npv
    assert "npv.txt" in npv  # filename is the column name
    assert len(npv) == 5
    assert npv.dtypes["REAL"] == int
    assert npv.dtypes["npv.txt"] == object
    # This is undesirable, can cause trouble with aggregation
    # Try again:
    reekensemble.load_scalar("npv.txt", force_reread=True, convert_numeric=True)
    npv = reekensemble.get_df("npv.txt")
    assert npv.dtypes["npv.txt"] == int or npv.dtypes["npv.txt"] == float
    assert len(npv) == 4  # the error should now be removed

    reekensemble.load_scalar("emptyscalarfile")  # missing in real-4
    assert len(reekensemble.get_df("emptyscalarfile")) == 4
    assert "emptyscalarfile" in reekensemble.keys()
    # Use when filter is merged.
    # assert len(reekensemble.filter('emptyscalarfile', inplace=True)) == 4

    # If we try to read the empty files as numerical values, we should get
    # nothing back:
    with pytest.raises((KeyError, ValueError)):
        reekensemble.load_scalar(
            "emptyscalarfile", force_reread=True, convert_numeric=True
        )

    with pytest.raises((KeyError, ValueError)):
        reekensemble.load_scalar("nonexistingfile")


def test_noautodiscovery():
    """Test that we have full control over auto-discovery of UNSMRY files"""

    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    reekensemble = ScratchEnsemble(
        "reektest", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0"
    )
    # Default ensemble construction will include auto-discovery, check
    # that we got that:
    assert not reekensemble.get_smry(column_keys="FOPT").empty
    assert "UNSMRY" in reekensemble.files["FILETYPE"].values

    # Now try again, with no autodiscovery
    reekensemble = ScratchEnsemble(
        "reektest",
        testdir + "/data/testensemble-reek001/" + "realization-*/iter-0",
        autodiscovery=False,
    )
    assert reekensemble.get_smry(column_keys="FOPT").empty
    reekensemble.find_files("eclipse/model/*UNSMRY")
    assert not reekensemble.get_smry(column_keys="FOPT").empty

    # Some very basic data is discovered even though we have autodiscovery=False
    assert "parameters.txt" in reekensemble.keys()
    assert "STATUS" in reekensemble.keys()

    # If these are unwanted, we can delete explicitly:
    reekensemble.remove_data("parameters.txt")
    reekensemble.remove_data(["STATUS"])
    assert "parameters.txt" not in reekensemble.keys()
    assert "STATUS" not in reekensemble.keys()


def test_ensemble_ecl():
    """Eclipse specific functionality"""

    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    reekensemble = ScratchEnsemble(
        "reektest", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0"
    )

    # Eclipse summary keys:
    assert len(reekensemble.get_smrykeys("FOPT")) == 1
    assert len(reekensemble.get_smrykeys("F*")) == 49
    assert len(reekensemble.get_smrykeys(["F*", "W*"])) == 49 + 280
    assert not reekensemble.get_smrykeys("BOGUS")

    # reading ensemble dataframe
    monthly = reekensemble.load_smry(time_index="monthly")

    monthly = reekensemble.load_smry(column_keys=["F*"], time_index="monthly")
    assert monthly.columns[0] == "REAL"  # Enforce order of columns.
    assert monthly.columns[1] == "DATE"
    assert len(monthly) == 190
    # Check that the result was cached in memory, not necessarily on disk..
    assert isinstance(reekensemble.get_df("unsmry--monthly.csv"), pd.DataFrame)

    assert len(reekensemble.keys()) == 4

    # When asking the ensemble for FOPR, we also get REAL as a column
    # in return. Note that the internal stored version will be
    # overwritten by each load_smry()
    assert len(reekensemble.load_smry(column_keys=["FOPR"]).columns) == 3
    assert len(reekensemble.load_smry(column_keys=["FOP*"]).columns) == 11
    assert len(reekensemble.load_smry(column_keys=["FGPR", "FOP*"]).columns) == 12

    # Check that there is now a cached version with raw dates:
    assert isinstance(reekensemble.get_df("unsmry--raw.csv"), pd.DataFrame)
    # The columns are not similar, this is allowed!'

    # If you get 3205 here, it means that you are using the union of
    # raw dates from all realizations, which is not correct
    assert len(reekensemble.load_smry(column_keys=["FGPR", "FOP*"]).index) == 1700

    # Date list handling:
    assert len(reekensemble.get_smry_dates(freq="report")) == 641
    assert len(reekensemble.get_smry_dates(freq="raw")) == 641
    assert len(reekensemble.get_smry_dates(freq="yearly")) == 5
    assert len(reekensemble.get_smry_dates(freq="monthly")) == 38
    assert len(reekensemble.get_smry_dates(freq="daily")) == 1098
    assert len(reekensemble.get_smry_dates(freq="D")) == 1098
    assert len(reekensemble.get_smry_dates(freq="2D")) == 1098 / 2
    assert len(reekensemble.get_smry_dates(freq="weekly")) == 159
    assert len(reekensemble.get_smry_dates(freq="W-MON")) == 159
    assert len(reekensemble.get_smry_dates(freq="2W-MON")) == 80
    assert len(reekensemble.get_smry_dates(freq="W-TUE")) == 159
    assert len(reekensemble.get_smry_dates(freq="first")) == 1
    assert len(reekensemble.get_smry_dates(freq="last")) == 1
    assert reekensemble.get_smry_dates(freq="first") == reekensemble.get_smry_dates(
        freq="first", start_date="1900-01-01", end_date="2050-02-01"
    )
    assert reekensemble.get_smry_dates(freq="last") == reekensemble.get_smry_dates(
        freq="last", start_date="1900-01-01", end_date="2050-02-01"
    )

    assert str(reekensemble.get_smry_dates(freq="report")[-1]) == "2003-01-02 00:00:00"
    assert str(reekensemble.get_smry_dates(freq="raw")[-1]) == "2003-01-02 00:00:00"
    assert str(reekensemble.get_smry_dates(freq="yearly")[-1]) == "2004-01-01"
    assert str(reekensemble.get_smry_dates(freq="monthly")[-1]) == "2003-02-01"
    assert str(reekensemble.get_smry_dates(freq="daily")[-1]) == "2003-01-02"
    assert str(reekensemble.get_smry_dates(freq="first")[-1]) == "2000-01-01"
    assert str(reekensemble.get_smry_dates(freq="last")[-1]) == "2003-01-02"

    assert (
        str(reekensemble.get_smry_dates(freq="daily", end_date="2002-03-03")[-1])
        == "2002-03-03"
    )
    assert (
        str(reekensemble.get_smry_dates(freq="daily", start_date="2002-03-03")[0])
        == "2002-03-03"
    )

    # Start and end outside of orig data and on the "wrong side"
    dates = reekensemble.get_smry_dates(end_date="1999-03-03")
    assert len(dates) == 1
    assert str(dates[0]) == "1999-03-03"

    dates = reekensemble.get_smry_dates(start_date="2099-03-03")
    assert len(dates) == 1
    assert str(dates[0]) == "2099-03-03"

    # Time interpolated dataframes with summary data:
    yearly = reekensemble.get_smry_dates(freq="yearly")
    assert len(reekensemble.load_smry(column_keys=["FOPT"], time_index=yearly)) == 25
    # NB: This is cached in unsmry-custom.csv, not unsmry--yearly!
    # This usage is discouraged. Use 'yearly' in such cases.

    # Check that we can shortcut get_smry_dates:
    assert len(reekensemble.load_smry(column_keys=["FOPT"], time_index="yearly")) == 25

    assert len(reekensemble.load_smry(column_keys=["FOPR"], time_index="first")) == 5
    assert isinstance(reekensemble.get_df("unsmry--first.csv"), pd.DataFrame)

    assert len(reekensemble.load_smry(column_keys=["FOPR"], time_index="last")) == 5
    assert isinstance(reekensemble.get_df("unsmry--last.csv"), pd.DataFrame)

    # Check that time_index=None and time_index="raw" behaves like default
    raw = reekensemble.load_smry(column_keys=["F*PT"], time_index="raw")
    print(raw)
    assert reekensemble.load_smry(column_keys=["F*PT"]).iloc[3, 2] == raw.iloc[3, 2]
    assert (
        reekensemble.load_smry(column_keys=["F*PT"], time_index=None).iloc[3, 3]
        == raw.iloc[3, 3]
    )

    # Give ISO-dates directly:
    assert (
        len(reekensemble.get_smry(column_keys=["FOPR"], time_index="2001-01-02")) == 5
    )

    # Summary metadata:
    meta = reekensemble.get_smry_meta()
    assert len(meta) == len(reekensemble.get_smrykeys())
    assert "FOPT" in meta
    assert not meta["FOPT"]["is_rate"]
    assert meta["FOPT"]["is_total"]

    meta = reekensemble.get_smry_meta("FOPT")
    assert meta["FOPT"]["is_total"]

    meta = reekensemble.get_smry_meta("*")
    assert meta["FOPT"]["is_total"]

    meta = reekensemble.get_smry_meta(["*"])
    assert meta["FOPT"]["is_total"]

    meta = reekensemble.get_smry_meta(["FOPT", "BOGUS"])
    assert meta["FOPT"]["is_total"]
    assert "BOGUS" not in meta

    # Eclipse well names list
    assert len(reekensemble.get_wellnames("OP*")) == 5
    assert len(reekensemble.get_wellnames(None)) == 8
    assert len(reekensemble.get_wellnames()) == 8
    assert not reekensemble.get_wellnames("")
    assert len(reekensemble.get_wellnames(["OP*", "WI*"])) == 8

    # eclipse well groups list
    assert len(reekensemble.get_groupnames()) == 3

    # delta between two ensembles
    diff = reekensemble - reekensemble
    assert len(diff.get_smry(column_keys=["FOPR", "FGPR", "FWCT"]).columns) == 5

    # eclipse summary vector statistics for a given ensemble
    df_stats = reekensemble.get_smry_stats(
        column_keys=["FOPR", "FGPR"], time_index="monthly"
    )
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

    # Check user supplied quantiles
    df_stats = reekensemble.get_smry_stats(
        column_keys=["FOPT"], time_index="yearly", quantiles=[0, 15, 50, 85, 100]
    )
    statistics = df_stats.index.levels[0]
    assert "p0" in statistics
    assert "p15" in statistics
    assert "p50" in statistics
    assert "p85" in statistics
    assert "p100" in statistics

    # For oil industry, p15 on FOPT should yield a larger value than p85.
    # But the quantiles we get out follows the rest of the world
    # so we check for the opposite.
    assert df_stats["FOPT"]["p85"][-1] > df_stats["FOPT"]["p15"][-1]

    with pytest.raises(ValueError):
        reekensemble.get_smry_stats(
            column_keys=["FOPT"], time_index="yearly", quantiles=["foobar"]
        )

    noquantiles = reekensemble.get_smry_stats(
        column_keys=["FOPT"], time_index="yearly", quantiles=[]
    )
    assert len(noquantiles.index.levels[0]) == 3


def test_nonstandard_dirs(tmpdir):
    """Test that we can initialize ensembles from some
    non-standard directories."""

    tmpdir.chdir()

    ensdir = "foo-ens-bar/"

    os.makedirs(ensdir)
    os.makedirs(ensdir + "/bar_001/iter_003")
    os.makedirs(ensdir + "/bar_002/iter_003")
    os.makedirs(ensdir + "/bar_003/iter_003")
    enspaths = ensdir + "/bar_*/iter_003"

    ens = ScratchEnsemble("foo", enspaths)
    # The logger should also print CRITICAL statements here.
    assert not ens

    # But if we specify a realidxregex, it should work
    ens = ScratchEnsemble("foo", enspaths, realidxregexp=r"bar_(\d+)")
    assert len(ens) == 3

    # Supplying wrong regexpes:
    ens = ScratchEnsemble("foo", enspaths, realidxregexp="bar_xx")
    assert not ens


def test_volumetric_rates():
    """Test computation of cumulative compatible rates"""

    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    ens = ScratchEnsemble(
        "reektest", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0"
    )
    cum_df = ens.get_smry(column_keys=["F*T", "W*T*"], time_index="yearly")
    vol_rate_df = ens.get_volumetric_rates(
        column_keys=["F*T", "W*T*"], time_index="yearly"
    )
    assert "DATE" in vol_rate_df
    assert "FWCR" not in vol_rate_df
    assert "FOPR" in vol_rate_df
    assert "FWPR" in vol_rate_df

    # Test each realization individually
    for realidx in vol_rate_df["REAL"].unique():
        vol_rate_real = vol_rate_df.set_index("REAL").loc[realidx]
        cum_real = cum_df.set_index("REAL").loc[realidx]
        assert len(vol_rate_real) == 5
        assert vol_rate_real["FOPR"].sum() == cum_real["FOPT"].iloc[-1]


def test_filter():
    """Test filtering of realizations in ensembles

    Realizations not fulfilling tested conditions are
    dropped from the ensemble"""

    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    dirs = testdir + "/data/testensemble-reek001/" + "realization-*/iter-0"
    reekensemble = ScratchEnsemble("reektest", dirs)

    # This should just require a STATUS file to be there
    # for every realization
    assert len(reekensemble.filter("STATUS")) == 5

    # Test string equivalence on numeric data:
    reekensemble.filter(
        "parameters.txt", key="RMS_SEED", value="723121249", inplace=True
    )
    assert len(reekensemble) == 2

    # (False positive from pylint on this line)
    assert reekensemble.agg("mean")["parameters"]["RMS_SEED"] == 723121249

    # Test numeric equivalence
    reekensemble = ScratchEnsemble("reektest", dirs)
    reekensemble.filter("parameters.txt", key="RMS_SEED", value=723121249, inplace=True)
    assert len(reekensemble) == 2
    assert reekensemble.agg("mean")["parameters"]["RMS_SEED"] == 723121249

    reekensemble = ScratchEnsemble("reektest", dirs)
    filtered = reekensemble.filter("parameters.txt", key="FOO", inplace=False)
    assert len(filtered) == 2
    # (NaN in one of the parameters.txt is True in this context)

    filtered = reekensemble.filter(
        "parameters.txt", key="MULTFLT_F1", value=0.001, inplace=False
    )
    assert len(filtered) == 4
    assert (
        len(reekensemble.filter("parameters.txt", key="FWL", value=1700, inplace=False))
        == 3
    )
    assert (
        len(
            reekensemble.filter(
                "parameters.txt", key="FWL", value="1700", inplace=False
            )
        )
        == 3
    )

    # This one is tricky, the empty string should correspond to
    # missing data - NOT IMPLEMENTED
    # assert len(reekensemble.filter('parameters.txt', key='FOO',
    #                               value='', inplace=False) == 4)

    # while no value means that the key must be present
    assert len(reekensemble.filter("parameters.txt", key="FOO", inplace=False)) == 2

    # 'key' is not accepted for things that are tables.
    with pytest.raises(ValueError):
        reekensemble.filter("STATUS", key="ECLIPSE")
    with pytest.raises(ValueError):
        reekensemble.filter("STATUS", value="ECLIPSE")

    # Check column presence
    assert len(reekensemble.filter("STATUS", column="FORWARD_MODEL")) == 5
    assert (
        len(reekensemble.filter("STATUS", column="FORWARD_MODEL", inplace=False)) == 5
    )
    assert not reekensemble.filter("STATUS", column="FOOBAR", inplace=False)
    with pytest.raises(ValueError):
        reekensemble.filter("STATUS", wrongarg="FOOBAR", inplace=False)
    assert (
        len(
            reekensemble.filter(
                "STATUS", column="FORWARD_MODEL", columncontains="ECLIPSE100_2014.2"
            )
        )
        == 5
    )
    assert not reekensemble.filter(
        "STATUS",
        column="FORWARD_MODEL",
        columncontains="ECLIPSE100_2010.2",
        inplace=False,
    )
    reekensemble.load_smry()
    assert len(reekensemble.filter("unsmry--raw")) == 5
    assert len(reekensemble.filter("unsmry--raw", column="FOPT")) == 5
    assert not reekensemble.filter("unsmry--raw", column="FOOBAR", inplace=False)
    assert len(reekensemble.filter("unsmry--raw", column="FOPT", columncontains=0)) == 5
    assert not reekensemble.filter(
        "unsmry--raw", column="FOPT", columncontains=-1000, inplace=False
    )
    assert (
        len(
            reekensemble.filter(
                "unsmry--raw", column="FOPT", columncontains=6025523.0, inplace=False
            )
        )
        == 1
    )
    assert (
        len(
            reekensemble.filter(
                "unsmry--raw", column="FOPT", columncontains=6025523, inplace=False
            )
        )
        == 1
    )

    # We do not support strings here (not yet)
    # assert len(reekensemble.filter('unsmry--raw', column='FOPT',
    #                                columncontains='6025523.0',
    #                                inplace=False)) == 1

    assert (
        len(
            reekensemble.filter(
                "unsmry--raw", column="DATE", columncontains="2002-11-25", inplace=False
            )
        )
        == 5
    )
    assert (
        len(
            reekensemble.filter(
                "unsmry--raw",
                column="DATE",
                columncontains="2002-11-25 00:00:00",
                inplace=False,
            )
        )
        == 5
    )
    assert not reekensemble.filter(
        "unsmry--raw",
        column="DATE",
        columncontains="2002-11-25 00:00:01",
        inplace=False,
    )
    assert (
        len(
            reekensemble.filter(
                "unsmry--raw",
                column="DATE",
                columncontains="2000-01-07 02:26:15",
                inplace=False,
            )
        )
        == 3
    )
    assert not reekensemble.filter(
        "unsmry--raw", column="DATE", columncontains="2000-01-07", inplace=False
    )
    # Last one is zero because it implies 00:00:00, it does not round!


def test_ertrunpathfile():
    """Initialize an ensemble from an ERT runpath file"""

    cwd = os.getcwd()

    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    # The example runpathfile contains relative paths, which is not realistic
    # for real runpathfiles coming from ERT. But relative paths are more easily
    # handled in git and with pytest, so we have to try some magic
    # to get it to work:
    if "tests" not in os.getcwd():
        if os.path.exists("tests"):
            os.chdir("tests")
        else:
            pytest.skip("Did not find test data")
    if not os.path.exists("data"):
        pytest.skip("Did not find test data")

    # The ertrunpathfile used here assumes we are in the 'tests' directory
    ens = ScratchEnsemble(
        "ensfromrunpath", runpathfile=testdir + "/data/ert-runpath-file"
    )
    assert len(ens) == 5

    assert all([os.path.isabs(x) for x in ens.files["FULLPATH"]])
    # Check that the UNSMRY files has been discovered, they should always be
    # because ECLBASE is given in the runpathfile
    assert sum(["UNSMRY" in x for x in ens.files["BASENAME"].unique()]) == 5

    os.chdir(cwd)


def test_nonexisting():
    """Test what happens when we try to initialize from a
    filesystem path that does not exist"""

    empty = ScratchEnsemble("nothing", "/foo/bar/com/not_existing")
    assert not empty

    # This ensemble does not exist, but we should ensure no crash
    # when we encounter Permission Denied on /scratch/johan_sverdrup
    nopermission = ScratchEnsemble(
        "noaccess", "/scratch/johan_sverdrup/js_phase5/" + "foo/realization-*/iter-0"
    )
    assert not nopermission


def test_eclsumcaching():
    """Test caching of eclsum"""

    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    dirs = testdir + "/data/testensemble-reek001/" + "realization-*/iter-0"
    ens = ScratchEnsemble("reektest", dirs)

    # The problem here is if you load in a lot of UNSMRY files
    # and the Python process keeps them in memory. Not sure
    # how to check in code that an object has been garbage collected
    # but for garbage collection to work, at least the realization
    # _eclsum variable must be None.

    ens.load_smry()
    # Default is to do caching, so these will not be None:
    assert all([x._eclsum for (idx, x) in ens.realizations.items()])

    # If we redo this operation, the same objects should all
    # be None afterwards:
    ens.load_smry(cache_eclsum=None)
    assert not any([x._eclsum for (idx, x) in ens.realizations.items()])

    ens.get_smry()
    assert all([x._eclsum for (idx, x) in ens.realizations.items()])

    ens.get_smry(cache_eclsum=False)
    assert not any([x._eclsum for (idx, x) in ens.realizations.items()])

    ens.get_smry_stats()
    assert all([x._eclsum for (idx, x) in ens.realizations.items()])

    ens.get_smry_stats(cache_eclsum=False)
    assert not any([x._eclsum for (idx, x) in ens.realizations.items()])

    ens.get_smry_dates()
    assert all([x._eclsum for (idx, x) in ens.realizations.items()])

    # Clear the cached objects because the statement above has cached it..
    for _, realization in ens.realizations.items():
        realization._eclsum = None

    ens.get_smry_dates(cache_eclsum=False)
    assert not any([x._eclsum for (idx, x) in ens.realizations.items()])


def test_filedescriptors():
    """Test how filedescriptors are used.

    The lazy_load option to EclSum affects this, if it is set to True
    file descriptors are not closed (and True is the default).
    In order to be able to open thousands of smry files, we need
    to always close the file descriptors when possible, and therefore
    lazy_load should be set to False in realization.py"""

    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    fd_dir = "/proc/" + str(os.getpid()) + "/fd"
    if not os.path.exists(fd_dir):
        print("Counting file descriptors on non-Linux not supported")
        return
    fd_count1 = len(os.listdir(fd_dir))
    reekensemble = ScratchEnsemble(
        "reektest", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0"
    )

    # fd_count2 = len(os.listdir(fd_dir))
    reekensemble.load_smry()
    # fd_count3 = len(os.listdir(fd_dir))
    del reekensemble
    fd_count4 = len(os.listdir(fd_dir))

    # As long as lazy_load = False, we should have 5,5,5,5 from this
    # If lazy_load is True (default), then we get 15, 15, 25, 20
    # (that last number pattern reveals a (now fixed) bug in EclSum)
    # print(fd_count1, fd_count2, fd_count3, fd_count4)

    assert fd_count1 == fd_count4


def test_read_eclgrid():
    """Test reading Eclipse grids of a full ensemble"""
    testdir = os.path.dirname(os.path.abspath(__file__))
    reekensemble = ScratchEnsemble(
        "reektest", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0"
    )
    grid_df = reekensemble.get_eclgrid(["PERMX", "FLOWATI+", "FLOWATJ+"], report=1)

    assert len(grid_df.columns) == 35
    assert len(grid_df["i"]) == 35840


def test_get_df():
    """Test the data retrieval functionality

    get_df() in the ensemble context is an aggregator, that will aggregate
    data from individual realaizations to the ensemble level, with
    optional merging capabilities performed on realization level."""
    testdir = os.path.dirname(os.path.abspath(__file__))
    ens = ScratchEnsemble(
        "reektest", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0"
    )
    smry = ens.load_smry(column_keys="FO*", time_index="yearly")
    assert not ens.get_df("unsmry--yearly").empty
    assert not ens.get_df("unsmry--yearly.csv").empty
    assert not ens.get_df("share/results/tables/unsmry--yearly").empty
    assert not ens.get_df("share/results/tables/unsmry--yearly.csv").empty
    with pytest.raises(KeyError):
        # pylint: disable=pointless-statement
        ens.get_df("unsmry--monthly")
    ens.load_smry(column_keys="FO*", time_index="monthly")
    assert not ens.get_df("unsmry--monthly").empty
    with pytest.raises(KeyError):
        # pylint: disable=pointless-statement
        ens.get_df("unsmry-monthly")

    # Tests that we can do merges directly:
    params = ens.get_df("parameters.txt")
    smryparams = ens.get_df("unsmry--yearly", merge="parameters")
    # The set union is to handle the REAL column present in both smry and params:
    assert len(smryparams.columns) == len(set(smry.columns).union(params.columns))

    # Test multiple merges:
    outputs = ens.load_txt("outputs.txt")
    assert len(
        ens.get_df("unsmry--yearly", merge=["parameters", "outputs.txt"]).columns
    ) == len(set(smry.columns).union(params.columns).union(outputs.columns))

    # Try merging dataframes:
    ens.load_csv("share/results/volumes/simulator_volume_fipnum.csv")

    # Inject a mocked dataframe to the realization, there is
    # no "add_data" API for ensembles, but we can use the apply()
    # functionality
    def fipnum2zone():
        """Helper function for injecting mocked frame into
        each realization"""
        return pd.DataFrame(
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

    ens.apply(fipnum2zone, localpath="fipnum2zone")
    volframe = ens.get_df("simulator_volume_fipnum", merge="fipnum2zone")

    assert "ZONE" in volframe
    assert "FIPNUM" in volframe
    assert "STOIIP_OIL" in volframe
    assert len(volframe["ZONE"].unique()) == 3

    # Merge with scalar data:
    ens.load_scalar("npv.txt")
    vol_npv = ens.get_df("simulator_volume_fipnum", merge="npv.txt")
    # (this particular data combination does not really make sense)
    assert "STOIIP_OIL" in vol_npv
    assert "npv.txt" in vol_npv


def test_apply(tmpdir):
    """
    Test the callback functionality
    """
    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    tmpdir.chdir()

    symlink_iter(os.path.join(testdir, "data/testensemble-reek001"), "iter-0")

    ens = ScratchEnsemble("reektest", "realization-*/iter-0")

    def ex_func1():
        """Example function that will return a constant dataframe"""
        return pd.DataFrame(
            index=["1", "2"], columns=["foo", "bar"], data=[[1, 2], [3, 4]]
        )

    result = ens.apply(ex_func1)
    assert isinstance(result, pd.DataFrame)
    assert "REAL" in result.columns
    assert len(result) == 10

    # Check that we can internalize as well
    ens.apply(ex_func1, localpath="df-1234")
    int_df = ens.get_df("df-1234")
    assert "REAL" in int_df
    assert len(int_df) == len(result)

    if SKIP_FMU_TOOLS:
        return

    # Test if we can wrap the volumetrics-parser in fmu.tools:
    # It cannot be applied directly, as we need to combine the
    # realization's root directory with the relative path coming in:

    def rms_vol2df(kwargs):
        """Example function for bridging with fmu.tools to parse volumetrics"""
        fullpath = os.path.join(kwargs["realization"].runpath(), kwargs["filename"])
        # The supplied callback should not fail too easy.
        if os.path.exists(fullpath):
            return volumetrics.rmsvolumetrics_txt2df(fullpath)
        return pd.DataFrame()

    rmsvols_df = ens.apply(
        rms_vol2df, filename="share/results/volumes/" + "geogrid_vol_oil_1.txt"
    )
    assert rmsvols_df["STOIIP_OIL"].sum() > 0
    assert len(rmsvols_df["REAL"].unique()) == 4


def test_manifest(tmpdir):
    """Test initializing ensembles with manifest """

    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    # Make a dummy manifest:
    manifest = {
        "description": "Demo ensemble for testing of fmu-ensemble",
        "project_id": "Foobar",
        "coordinate_system": "somethingfunny",
    }

    # Initialize with a ready made manifest dict:
    ens = ScratchEnsemble(
        "reektest",
        testdir + "/data/testensemble-reek001/" + "realization-*/iter-0",
        manifest=manifest,
    )
    assert "project_id" in ens.manifest

    vens = ens.to_virtual()
    assert "project_id" in vens.manifest

    # Initialize without, and add it later:
    ens = ScratchEnsemble(
        "reektest", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0"
    )
    assert "coordinate_system" not in ens.manifest
    ens.manifest = manifest
    assert "coordinate_system" in ens.manifest

    # Dump to random filename, and load from that file:
    with open(str(tmpdir.join("foo.yml")), "w") as file_h:
        file_h.write(yaml.dump(manifest))
    ens = ScratchEnsemble(
        "reektest",
        testdir + "/data/testensemble-reek001/" + "realization-*/iter-0",
        manifest=str(tmpdir.join("foo.yml")),
    )
    assert "description" in ens.manifest

    # Load from non-existing file:
    ens = ScratchEnsemble(
        "reektest",
        testdir + "/data/testensemble-reek001/" + "realization-*/iter-0",
        manifest=str(tmpdir.join("foo-notexisting.yml")),
    )
    assert isinstance(ens.manifest, dict)
    assert not ens.manifest  # (empty dictionary)
    vens = ens.to_virtual()
    assert not vens.manifest

    # Load from empty file:
    with open(str(tmpdir.join("empty")), "w") as file_h:
        file_h.write("")

    ens = ScratchEnsemble(
        "reektest",
        testdir + "/data/testensemble-reek001/" + "realization-*/iter-0",
        manifest=str(tmpdir.join("empty")),
    )
    assert not ens.manifest
