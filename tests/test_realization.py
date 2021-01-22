"""Testing fmu-ensemble."""
# pylint: disable=protected-access

import os
import datetime
import shutil
import logging
import pandas as pd
import yaml
from dateutil.relativedelta import relativedelta

import pytest
import ecl.summary

import numpy as np

from .test_ensembleset import symlink_iter
from fmu import ensemble


try:
    SKIP_FMU_TOOLS = False
    from fmu.tools import volumetrics
except ImportError:
    SKIP_FMU_TOOLS = True

logger = logging.getLogger(__name__)


def test_single_realization(tmpdir):
    """Test internalization of properties pertaining
    to single realizations"""
    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    tmpdir.chdir()

    realdir = os.path.join(testdir, "data/testensemble-reek001", "realization-0/iter-0")
    real = ensemble.ScratchRealization(realdir)

    assert os.path.isabs(real.runpath())
    assert os.path.exists(real.runpath())

    assert len(real.files) == 4
    assert "parameters.txt" in real.data
    assert isinstance(real.parameters["RMS_SEED"], int)
    assert real.parameters["RMS_SEED"] == 422851785
    assert isinstance(real.parameters["MULTFLT_F1"], float)
    assert isinstance(
        real.load_txt("parameters.txt", convert_numeric=False, force_reread=True)[
            "RMS_SEED"
        ],
        str,
    )
    # We have rerun load_txt on parameters, but file count
    # should not increase:
    assert len(real.files) == 4

    with pytest.raises(IOError):
        real.load_txt("nonexistingfile.txt")

    # Load more data from text files:
    assert "NPV" in real.load_txt("outputs.txt")
    assert len(real.files) == 5
    assert "outputs.txt" in real.data
    assert "top_structure" in real.data["outputs.txt"]

    # STATUS file
    status = real.get_df("STATUS")
    assert not status.empty
    assert isinstance(status, pd.DataFrame)
    assert "ECLIPSE" in status.loc[49, "FORWARD_MODEL"]
    assert int(status.loc[49, "DURATION"]) == 141

    # CSV file loading
    vol_df = real.load_csv("share/results/volumes/simulator_volume_fipnum.csv")
    assert len(real.files) == 6
    assert isinstance(vol_df, pd.DataFrame)
    assert vol_df["STOIIP_TOTAL"].sum() > 0

    # Test later retrieval of cached data:
    vol_df2 = real.get_df("share/results/volumes/simulator_volume_fipnum.csv")
    assert vol_df2["STOIIP_TOTAL"].sum() > 0

    # Test scalar import
    assert "OK" in real.keys()  # Imported in __init__
    assert real["OK"] == "All jobs complete 22:47:54"
    # NB: Trailing whitespace from the OK-file is removed.
    assert isinstance(real["OK"], str)

    # Check that we can "reimport" the OK file
    real.load_scalar("OK", force_reread=True)
    assert "OK" in real.keys()  # Imported in __init__
    assert real["OK"] == "All jobs complete 22:47:54"
    assert isinstance(real["OK"], str)
    assert len(real.files[real.files.LOCALPATH == "OK"]) == 1

    real.load_scalar("npv.txt")
    assert real.get_df("npv.txt") == 3444
    assert real["npv.txt"] == 3444
    assert isinstance(real.data["npv.txt"], (int, np.integer))
    assert "npv.txt" in real.files.LOCALPATH.values
    assert real.files[real.files.LOCALPATH == "npv.txt"]["FILETYPE"].values[0] == "txt"

    real.load_scalar("emptyscalarfile")
    # Activate this test when filter() is merged:
    # assert real.contains('emptyfile')
    assert "emptyscalarfile" in real.data
    assert isinstance(real["emptyscalarfile"], str)
    assert "emptyscalarfile" in real.files["LOCALPATH"].values

    # Check that FULLPATH always has absolute paths
    assert all([os.path.isabs(x) for x in real.files["FULLPATH"]])

    with pytest.raises(IOError):
        real.load_scalar("notexisting.txt")

    # Test internal storage:
    localpath = "share/results/volumes/simulator_volume_fipnum.csv"
    assert localpath in real.data
    assert isinstance(real.get_df(localpath), pd.DataFrame)
    assert isinstance(real.get_df("parameters.txt"), dict)
    assert isinstance(real.get_df("outputs.txt"), dict)

    # Test shortcuts to the internal datastore
    assert isinstance(real.get_df("simulator_volume_fipnum.csv"), pd.DataFrame)
    # test without extension:
    assert isinstance(
        real.get_df("share/results/volumes/" + "simulator_volume_fipnum"), pd.DataFrame
    )
    assert isinstance(real.get_df("parameters"), dict)
    # test basename and no extension:
    assert isinstance(real.get_df("simulator_volume_fipnum"), pd.DataFrame)

    # Some CSV files might already contain the REAL column, this is not allowed
    foo_df = pd.DataFrame(columns=["REAL", "FOOBAR"], data=[[0, 1], [2, 3]])
    foo_df.to_csv(os.path.join(realdir, "foo-real.csv"), index=False)
    real.load_csv("foo-real.csv")  # A warning will be issued.
    assert "REAL" not in real.get_df("foo-real")
    assert "FOOBAR" in real.get_df("foo-real")

    with pytest.raises((ValueError, KeyError)):
        real.get_df("notexisting.csv")

    # Test __delitem__()
    keycount = len(real.keys())
    del real["parameters.txt"]
    assert len(real.keys()) == keycount - 1

    # At realization level, wrong filenames should throw exceptions,
    # at ensemble level it is fine.
    with pytest.raises(IOError):
        real.load_csv("bogus.csv")


def test_status_load(tmpdir):
    """Test loading of STATUS file with different errors in them

    These files are custom text files, and can have stray error
    messages in them. Robustness (i.e. no crash) is more
    important than best-effort parsing. Better parsing
    is left for issue #12
    """

    # Mock a realization:
    tmpdir.join("realization-0").mkdir()

    # Test with some selected STATUS files:
    with open(str(tmpdir.join("realization-0/STATUS")), "w") as status_fh:
        status_fh.write("")
    real = ensemble.ScratchRealization(str(tmpdir.join("realization-0")))
    assert real.get_df("STATUS").empty

    with open(str(tmpdir.join("realization-0/STATUS")), "w") as status_fh:
        status_fh.write("\n")
    real = ensemble.ScratchRealization(str(tmpdir.join("realization-0")))
    assert real.get_df("STATUS").empty

    with open(str(tmpdir.join("realization-0/STATUS")), "w") as status_fh:
        status_fh.write("foo bar bogus com\n")
    real = ensemble.ScratchRealization(str(tmpdir.join("realization-0")))
    assert real.get_df("STATUS").empty

    # Two sucessfull jobs
    with open(str(tmpdir.join("realization-0/STATUS")), "w") as status_fh:
        status_fh.write("first line always ignored\n")
        status_fh.write("INCLUDE_PC                      : 12:40:55 .... 12:40:55  \n")
        status_fh.write("ECLIPSE100_2014.2               : 12:40:55 .... 12:43:16\n")
    real = ensemble.ScratchRealization(str(tmpdir.join("realization-0")))
    status = real.get_df("STATUS")
    assert len(status) == 2
    assert "FORWARD_MODEL" in status
    assert "STARTTIME" in status
    assert "ENDTIME" in status
    assert "DURATION" in status
    assert (status["DURATION"].values == [0, 141]).all()  # in seconds

    # Two sucessfull jobs, but with error string
    with open(str(tmpdir.join("realization-0/STATUS")), "w") as status_fh:
        status_fh.write("first line always ignored\n")
        status_fh.write("INCLUDE_PC                      : 12:40:55 .... 12:40:55  \n")
        status_fh.write(
            "ECLIPSE100_2014.2               : 12:40:55 "
            ".... 12:43:16 SOMEERRORSTRING_FOO\n"
        )
    real = ensemble.ScratchRealization(str(tmpdir.join("realization-0")))
    status = real.get_df("STATUS")
    assert len(status) == 2
    assert "FORWARD_MODEL" in status
    assert (status["DURATION"].values == [0, 141]).all()  # in seconds
    assert status["errorstring"].values[1] == "SOMEERRORSTRING_FOO"

    with open(str(tmpdir.join("realization-0/STATUS")), "w") as status_fh:
        status_fh.write("first line always ignored\n")
        status_fh.write("INCLUDE_PC                      : 12:40:55 .... 12:40:55  \n")
        status_fh.write("ECLIPSE100_2014.2               : 12:40:55 ....")
    real = ensemble.ScratchRealization(str(tmpdir.join("realization-0")))
    status = real.get_df("STATUS")
    assert len(status) == 2
    assert "FORWARD_MODEL" in status
    assert status["DURATION"].values[0] == 0  # in seconds
    assert np.isnan(status["DURATION"].values[1])

    with open(str(tmpdir.join("realization-0/STATUS")), "w") as status_fh:
        status_fh.write("first line always ignored\n")
        status_fh.write("INCLUDE_PC                      : 12:40:55 .... 12:40:55  \n")
        status_fh.write("error message: something went really wrong")
    real = ensemble.ScratchRealization(str(tmpdir.join("realization-0")))
    status = real.get_df("STATUS")
    assert len(status) == 2
    assert "FORWARD_MODEL" in status
    assert status["DURATION"].values[0] == 0  # in seconds
    # NB: The current code is not able to pick this error string

    with open(str(tmpdir.join("realization-0/STATUS")), "w") as status_fh:
        status_fh.write("first line always ignored\n")
        status_fh.write("INCLUDE_PC                      : 12:XX:55 .... 12:40:55  \n")
    real = ensemble.ScratchRealization(str(tmpdir.join("realization-0")))
    status = real.get_df("STATUS")
    assert len(status) == 1
    assert "FORWARD_MODEL" in status
    assert np.isnan(status["DURATION"].values[0])

    with open(str(tmpdir.join("realization-0/STATUS")), "w") as status_fh:
        status_fh.write("first line always ignored\n")
        status_fh.write("INCLUDE_PC                      : 12:40.... 12:40:55  \n")
    real = ensemble.ScratchRealization(str(tmpdir.join("realization-0")))
    status = real.get_df("STATUS")
    assert len(status) == 1
    assert "FORWARD_MODEL" in status
    assert np.isnan(status["DURATION"].values[0])  # Unsupported time syntax


def test_batch():
    """Test batch processing at time of object initialization"""
    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    realdir = os.path.join(testdir, "data/testensemble-reek001", "realization-0/iter-0")
    real = ensemble.ScratchRealization(
        realdir,
        batch=[
            {"load_scalar": {"localpath": "npv.txt"}},
            {"load_smry": {"column_keys": "FOPT", "time_index": "yearly"}},
            {"load_smry": {"column_keys": "*", "time_index": "daily"}},
            {"load_smry": {"column_keys": "*", "time_index": "weekly"}},
            {"illegal-ignoreme": {}},
        ],
    )
    assert real.get_df("npv.txt") == 3444
    assert len(real.get_df("unsmry--daily")["FOPR"]) > 2
    assert len(real.get_df("unsmry--yearly")["FOPT"]) > 2
    assert len(real.get_df("unsmry--weekly")["FOPT"]) > 2


def test_volumetric_rates():
    """Test computation of volumetric rates from cumulative vectors"""

    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    realdir = os.path.join(testdir, "data/testensemble-reek001", "realization-0/iter-0")
    real = ensemble.ScratchRealization(realdir)

    # Should work without prior internalization:
    cum_df = real.get_smry(column_keys=["F*T", "W*T*"], time_index="yearly")
    vol_rate_df = real.get_volumetric_rates(
        column_keys=["F*T", "W*T*"], time_index="yearly"
    )
    assert vol_rate_df.index.name == "DATE"
    assert "FWCR" not in vol_rate_df  # We should not compute FWCT..
    assert "FOPR" in vol_rate_df
    assert "FWPR" in vol_rate_df

    # Test that computed rates can be summed up to cumulative at end:
    assert vol_rate_df["FOPR"].sum() == cum_df["FOPT"].iloc[-1]
    assert vol_rate_df["FGPR"].sum() == cum_df["FGPT"].iloc[-1]
    assert vol_rate_df["FWPR"].sum() == cum_df["FWPT"].iloc[-1]

    # Since rates are valid forwards in time, the last
    # row should have a zero, since that is the final simulated
    # date for the cumulative vector
    assert vol_rate_df["FOPR"].iloc[-1] == 0

    # Check that we allow cumulative allocated vectors:
    cumvecs = real.get_volumetric_rates(column_keys=["F*TH", "W*TH*"])
    assert not cumvecs.empty
    assert "FOPRH" in cumvecs
    assert "WOPRH:OP_1" in cumvecs

    assert real.get_volumetric_rates(column_keys="FOOBAR").empty
    assert real.get_volumetric_rates(column_keys=["FOOBAR"]).empty
    assert real.get_volumetric_rates(column_keys={}).empty

    with pytest.raises(ValueError):
        real.get_volumetric_rates(column_keys="FOPT", time_index="bogus")

    mcum = real.get_smry(column_keys="FOPT", time_index="monthly")
    dmcum = real.get_volumetric_rates(column_keys="FOPT", time_index="monthly")
    assert dmcum["FOPR"].sum() == mcum["FOPT"].iloc[-1]

    # Pick 10 **random** dates to get the volumetric rates between:
    daily_dates = real.get_smry_dates(freq="daily", normalize=False)
    subset_dates = np.random.choice(daily_dates, size=10, replace=False)
    subset_dates.sort()
    dcum = real.get_smry(column_keys="FOPT", time_index=subset_dates)
    ddcum = real.get_volumetric_rates(column_keys="FOPT", time_index=subset_dates)
    assert ddcum["FOPR"].iloc[-1] == 0

    # We are probably neither at the start or at the end of the production
    # interval.
    cumulative_error = ddcum["FOPR"].sum() - (
        dcum["FOPT"].loc[subset_dates[-1]] - dcum["FOPT"].loc[subset_dates[0]]
    )

    # Give some slack, we might have done a lot of interpolation
    # here.
    assert cumulative_error / ddcum["FOPR"].sum() < 0.000001

    # Test the time_unit feature.
    vol_rate_days = real.get_volumetric_rates(
        column_keys=["F*T"], time_index="yearly", time_unit="days"
    )
    vol_rate_months = real.get_volumetric_rates(
        column_keys=["F*T"], time_index="yearly", time_unit="months"
    )
    vol_rate_years = real.get_volumetric_rates(
        column_keys=["F*T"], time_index="yearly", time_unit="years"
    )

    # Sample test on correctness.
    # Fine-accuracy (wrt leap days) is not tested here:
    assert vol_rate_days["FWIR"].iloc[0] * 27.9 < vol_rate_months["FWIR"].iloc[0]
    assert vol_rate_days["FWIR"].iloc[0] * 31.1 > vol_rate_months["FWIR"].iloc[0]
    assert vol_rate_months["FWIR"].iloc[0] * 12 == pytest.approx(
        vol_rate_years["FWIR"].iloc[0]
    )

    assert vol_rate_days["FWIR"].iloc[0] * 364.9 < vol_rate_years["FWIR"].iloc[0]
    assert vol_rate_days["FWIR"].iloc[0] * 366.1 > vol_rate_years["FWIR"].iloc[0]

    with pytest.raises(ValueError):
        real.get_volumetric_rates(
            column_keys=["F*T"], time_index="yearly", time_unit="bogus"
        )

    # Try with the random dates
    dayscum = real.get_volumetric_rates(
        column_keys="FOPT", time_index=subset_dates, time_unit="days"
    )
    assert all(np.isfinite(dayscum["FOPR"]))
    diffdays = pd.DataFrame(pd.to_datetime(dayscum.index)).diff().shift(-1)
    dayscum["DIFFDAYS"] = [x.days for x in diffdays["DATE"]]
    # Calculate cumulative production from the computed volumetric daily rates:
    dayscum["FOPRcum"] = dayscum["FOPR"] * dayscum["DIFFDAYS"]
    # Check that this sum is equal to FOPT between first and last date:
    assert dayscum["FOPRcum"].sum() == pytest.approx(dcum["FOPT"][-1] - dcum["FOPT"][0])
    # (here we could catch an error in case we don't support leap days)

    # Monthly rates between the random dates:
    monthlyrates = real.get_volumetric_rates(
        column_keys="FOPT", time_index=subset_dates, time_unit="months"
    )
    assert all(np.isfinite(monthlyrates["FOPR"]))

    # Total number of months in production period
    delta = relativedelta(vol_rate_days.index[-1], vol_rate_days.index[0])
    months = delta.years * 12 + delta.months
    tworows = real.get_volumetric_rates(
        column_keys="FOPT",
        time_index=[vol_rate_days.index[0], vol_rate_days.index[-1]],
        time_unit="months",
    )
    assert tworows["FOPR"].iloc[0] * months == pytest.approx(cum_df["FOPT"].iloc[-1])

    # Check for defaults and error handling:
    assert not real.get_smry(column_keys=None).empty
    assert not real.get_smry(column_keys=[None]).empty
    assert not real.get_smry(column_keys=[None, "WOPT:BOGUS"]).empty
    assert not real.get_smry(column_keys=["WOPT:BOGUS", None]).empty
    column_count = len(real.get_smry())
    # Columns repeatedly asked for should not be added:
    assert len(real.get_smry(column_keys=[None, "FOPT"])) == column_count
    assert len(real.get_smry(column_keys=[None, "FOPT", "FOPT"])) == column_count
    assert real.get_smry(column_keys=["WOPT:BOGUS"]).empty

    assert "FOPT" in real.get_smry(column_keys=["WOPT:BOGUS", "FOPT"])


def test_datenormalization():
    """Test normalization of dates, where
    dates can be ensured to be on dategrid boundaries"""

    # fmu.ensemble.util.normalized_dates is also tested in test_util.py

    # Check that we normalize correctly with get_smry():
    # realization-0 here has its last summary date at 2003-01-02
    testdir = os.path.dirname(os.path.abspath(__file__))
    realdir = os.path.join(testdir, "data/testensemble-reek001", "realization-0/iter-0")
    real = ensemble.ScratchRealization(realdir)
    raw = real.get_smry(column_keys="FOPT", time_index="raw")
    assert str(raw.index[-1]) == "2003-01-02 00:00:00"
    daily = real.get_smry(column_keys="FOPT", time_index="daily")
    assert str(daily.index[-1]) == "2003-01-02"
    monthly = real.get_smry(column_keys="FOPT", time_index="monthly")
    assert str(monthly.index[-1]) == "2003-02-01"
    yearly = real.get_smry(column_keys="FOPT", time_index="yearly")
    assert str(yearly.index[-1]) == "2004-01-01"
    weekly = real.get_smry(column_keys="FOPT", time_index="weekly")
    assert str(weekly.index[-1]) == "2003-01-06"  # First Monday after 2003-01-02
    weekly = real.get_smry(column_keys="FOPT", time_index="W-MON")
    assert str(weekly.index[-1]) == "2003-01-06"  # First Monday after 2003-01-02
    weekly = real.get_smry(column_keys="FOPT", time_index="W-TUE")
    assert str(weekly.index[-1]) == "2003-01-07"  # First Tuesday after 2003-01-02
    weekly = real.get_smry(column_keys="FOPT", time_index="W-THU")
    assert str(weekly.index[-1]) == "2003-01-02"  # First Thursday after 2003-01-02

    # Check that time_index=None and time_index="raw" behaves like default
    raw = real.load_smry(column_keys="FOPT", time_index="raw")
    assert list(real.load_smry(column_keys="FOPT")["FOPT"].values) == list(
        raw["FOPT"].values
    )
    assert list(
        real.load_smry(column_keys="FOPT", time_index=None)["FOPT"].values
    ) == list(raw["FOPT"].values)

    # Check that we get the same correct normalization
    # with load_smry()
    real.load_smry(column_keys="FOPT", time_index="raw")
    assert str(real.get_df("unsmry--raw")["DATE"].iloc[-1]) == "2003-01-02 00:00:00"
    real.load_smry(column_keys="FOPT", time_index="daily")
    assert str(real.get_df("unsmry--daily")["DATE"].iloc[-1]) == "2003-01-02"
    real.load_smry(column_keys="FOPT", time_index="monthly")
    assert str(real.get_df("unsmry--monthly")["DATE"].iloc[-1]) == "2003-02-01"
    real.load_smry(column_keys="FOPT", time_index="yearly")
    assert str(real.get_df("unsmry--yearly")["DATE"].iloc[-1]) == "2004-01-01"
    real.load_smry(column_keys="FOPT", time_index="weekly")
    assert str(real.get_df("unsmry--weekly")["DATE"].iloc[-1]) == "2003-01-06"


def test_singlereal_ecl(tmp="TMP"):
    """Test Eclipse specific functionality for realizations"""

    testdir = os.path.dirname(os.path.abspath(__file__))
    realdir = os.path.join(testdir, "data/testensemble-reek001", "realization-0/iter-0")
    real = ensemble.ScratchRealization(realdir)

    # Eclipse summary files:
    assert isinstance(real.get_eclsum(), ecl.summary.EclSum)
    if not os.path.exists(tmp):
        os.mkdir(tmp)
    real.load_smry().to_csv(os.path.join(tmp, "real0smry.csv"), index=False)
    assert real.load_smry().shape == (378, 474)
    # 378 dates, 470 columns + DATE column

    assert real.load_smry(column_keys=["FOP*"])["FOPT"].max() > 6000000
    assert real.get_smryvalues("FOPT")["FOPT"].max() > 6000000

    # get_smry() should be analogue to load_smry(), but it should
    # not modify the internalized dataframes!
    internalized_df = real["unsmry--raw"]
    fresh_df = real.get_smry(column_keys=["G*"])
    assert "GGIR:OP" in fresh_df.columns
    assert "GGIR:OP" not in internalized_df.columns
    # Test that the internalized was not touched:
    assert "GGIR:OP" not in real["unsmry--raw"].columns

    assert "FOPT" in real.get_smry(column_keys=["F*"], time_index="monthly")
    assert "FOPT" in real.get_smry(column_keys="F*", time_index="yearly")
    assert "FOPT" in real.get_smry(column_keys="FOPT", time_index="daily")
    assert "FOPT" in real.get_smry(column_keys="FOPT", time_index="weekly")
    assert "FOPT" in real.get_smry(column_keys="FOPT", time_index="raw")

    # Test date functionality
    assert isinstance(real.get_smry_dates(), list)
    assert isinstance(real.get_smry_dates(freq="last"), list)
    assert isinstance(real.get_smry_dates(freq="last")[0], datetime.date)
    assert len(real.get_smry_dates()) == len(real.get_smry_dates(freq="monthly"))
    monthly = real.get_smry_dates(freq="monthly")
    assert monthly[-1] > monthly[0]  # end date is later than start
    assert len(real.get_smry_dates(freq="yearly")) == 5
    assert len(monthly) == 38
    assert len(real.get_smry_dates(freq="daily")) == 1098

    # Try ISO-date for time_index:
    singledata = real.get_smry(time_index="2000-05-05", column_keys="FOPT")
    assert "FOPT" in singledata
    assert "2000-05-05" in singledata.index

    # start and end should be included:
    assert (
        len(
            real.get_smry_dates(
                start_date="2000-06-05", end_date="2000-06-07", freq="daily"
            )
        )
        == 3
    )
    # No month boundary between start and end, but we
    # should have the starts and ends included
    assert (
        len(
            real.get_smry_dates(
                start_date="2000-06-05", end_date="2000-06-07", freq="monthly"
            )
        )
        == 2
    )
    # Date normalization should be overridden here:
    assert (
        len(
            real.get_smry_dates(
                start_date="2000-06-05",
                end_date="2000-06-07",
                freq="monthly",
                normalize=True,
            )
        )
        == 2
    )

    # Start_date and end_date at the same date should work
    assert len(real.get_smry_dates(start_date="2000-01-01", end_date="2000-01-01")) == 1
    assert (
        len(
            real.get_smry_dates(
                start_date="2000-01-01", end_date="2000-01-01", normalize=True
            )
        )
        == 1
    )

    # Check that we can go way outside the smry daterange:
    assert (
        len(
            real.get_smry_dates(
                start_date="1978-01-01", end_date="2030-01-01", freq="yearly"
            )
        )
        == 53
    )
    assert (
        len(
            real.get_smry_dates(
                start_date="1978-01-01",
                end_date="2030-01-01",
                freq="yearly",
                normalize=True,
            )
        )
        == 53
    )

    assert (
        len(
            real.get_smry_dates(
                start_date="2000-06-05",
                end_date="2000-06-07",
                freq="raw",
                normalize=True,
            )
        )
        == 2
    )
    assert (
        len(
            real.get_smry_dates(
                start_date="2000-06-05",
                end_date="2000-06-07",
                freq="raw",
                normalize=False,
            )
        )
        == 2
    )

    # Test caching/internalization of summary files

    # This should be false, since only the full localpath is in keys():
    assert "unsmry--raw.csv" not in real.keys()
    assert "share/results/tables/unsmry--raw.csv" in real.keys()
    assert "FOPT" in real["unsmry--raw"]
    with pytest.raises((ValueError, KeyError)):
        # This does not exist before we have asked for it
        # pylint: disable=pointless-statement
        "FOPT" in real["unsmry--yearly"]


def test_independent_realization(tmp="TMP"):
    """Test what we are able to load a single Eclipse run
    that might have nothing to do with FMU"""

    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    datadir = os.path.join(testdir, "data")
    tmpdir = os.path.join(datadir, tmp)
    if os.path.exists(tmpdir):
        shutil.rmtree(tmpdir)
    os.mkdir(tmpdir)
    # Let the directory contain only the UNSMRY and SMSPEC file
    shutil.copyfile(
        os.path.join(
            datadir,
            "testensemble-reek001/realization-2/iter-0/eclipse/"
            + "model/2_R001_REEK-2.UNSMRY",
        ),
        os.path.join(tmpdir, "2_R001_REEK-2.UNSMRY"),
    )
    shutil.copyfile(
        os.path.join(
            datadir,
            "testensemble-reek001/realization-2/iter-0/eclipse/"
            + "model/2_R001_REEK-2.SMSPEC",
        ),
        os.path.join(tmpdir, "2_R001_REEK-2.SMSPEC"),
    )

    # This should not fail, but with a nice constructive warning to the user hinting
    # about the solution
    empty = ensemble.ScratchRealization(tmpdir)
    assert not empty.index  # The index is None in such realizations.

    # This is how it must be done:
    real = ensemble.ScratchRealization(tmpdir, index="999")
    assert real.index == 999

    # No auto-discovery here:
    assert real.get_smry().empty

    # Explicit discovery:
    real.find_files("*UNSMRY")
    assert not real.get_smry().empty

    # However, we can do something with an undefined index:
    noindex = ensemble.ScratchRealization(tmpdir)
    noindex.find_files("*UNSMRY")
    assert not real.get_smry().empty

    shutil.rmtree(tmpdir)


def test_filesystem_changes():
    """Test loading of sparse realization (random data missing)

    Performed by filesystem manipulations from the original realizations.
    Clean up from previous runs are attempted, and also done when it finishes.
    (after a failed test run, filesystem is tainted)
    """

    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    datadir = testdir + "/data"
    tmpensname = ".deleteme_ens"
    # Clean up earlier failed runs:
    if os.path.exists(datadir + "/" + tmpensname):
        shutil.rmtree(datadir + "/" + tmpensname, ignore_errors=True)
    os.mkdir(datadir + "/" + tmpensname)
    shutil.copytree(
        datadir + "/testensemble-reek001/realization-0",
        datadir + "/" + tmpensname + "/realization-0",
    )

    realdir = datadir + "/" + tmpensname + "/realization-0/iter-0"

    # Load untainted realization, nothing bad should happen
    real = ensemble.ScratchRealization(realdir)

    # Remove SMSPEC file and reload:
    shutil.move(
        realdir + "/eclipse/model/2_R001_REEK-0.SMSPEC",
        realdir + "/eclipse/model/2_R001_REEK-0.SMSPEC-FOOO",
    )
    real = ensemble.ScratchRealization(realdir)  # this should go fine
    # This should just return None. Logging info is okay.
    assert real.get_eclsum() is None
    # This should return None
    assert real.get_smry_dates() is None
    # This should return empty dataframe:
    assert isinstance(real.load_smry(), pd.DataFrame)
    assert real.load_smry().empty

    assert isinstance(real.get_smry(), pd.DataFrame)
    assert real.get_smry().empty

    # Also move away UNSMRY and redo:
    shutil.move(
        realdir + "/eclipse/model/2_R001_REEK-0.UNSMRY",
        realdir + "/eclipse/model/2_R001_REEK-0.UNSMRY-FOOO",
    )
    real = ensemble.ScratchRealization(realdir)  # this should go fine
    # This should just return None
    assert real.get_eclsum() is None
    # This should return None
    assert real.get_smry_dates() is None
    # This should return empty dataframe:
    assert isinstance(real.load_smry(), pd.DataFrame)
    assert real.load_smry().empty

    # Reinstate summary data:
    shutil.move(
        realdir + "/eclipse/model/2_R001_REEK-0.UNSMRY-FOOO",
        realdir + "/eclipse/model/2_R001_REEK-0.UNSMRY",
    )
    shutil.move(
        realdir + "/eclipse/model/2_R001_REEK-0.SMSPEC-FOOO",
        realdir + "/eclipse/model/2_R001_REEK-0.SMSPEC",
    )

    # Remove jobs.json, this file should not be critical
    # but the status dataframe should have less information
    statuscolumnswithjson = len(real.get_df("STATUS").columns)
    os.remove(realdir + "/jobs.json")
    real = ensemble.ScratchRealization(realdir)  # this should go fine

    statuscolumnswithoutjson = len(real.get_df("STATUS").columns)
    assert statuscolumnswithoutjson > 0
    # Check that some STATUS info is missing.
    assert statuscolumnswithoutjson < statuscolumnswithjson

    # Remove parameters.txt
    shutil.move(realdir + "/parameters.txt", realdir + "/parameters.text")
    real = ensemble.ScratchRealization(realdir)
    # Should not fail

    # Move it back so the realization is valid again
    shutil.move(realdir + "/parameters.text", realdir + "/parameters.txt")

    # Remove STATUS altogether:
    shutil.move(realdir + "/STATUS", realdir + "/MOVEDSTATUS")
    real = ensemble.ScratchRealization(realdir)
    # Should not fail

    # Try with an empty STATUS file:
    fhandle = open(realdir + "/STATUS", "w")
    fhandle.close()
    real = ensemble.ScratchRealization(realdir)
    assert real.get_df("STATUS").empty
    # This demonstrates we can fool the Realization object, and
    # should perhaps leads to relaxation of the requirement..

    # Try with a STATUS file with error message on first job
    # the situation where there is one successful job.
    fhandle = open(realdir + "/STATUS", "w")
    fhandle.write(
        (
            "Current host                    : st-rst16-02-03/x86_64  "
            "file-server:10.14.10.238\n"
            "LSF JOBID: not running LSF\n"
            "COPY_FILE                       : 20:58:57 .... 20:59:00   "
            "EXIT: 1/Executable: /project/res/komodo/2018.02/root/etc/ERT/"
            "Config/jobs/util/script/copy_file.py failed with exit code: 1\n"
        )
    )
    fhandle.close()
    real = ensemble.ScratchRealization(realdir)
    # When issue 37 is resolved, update this to 1 and check the
    # error message is picked up.
    assert len(real.get_df("STATUS")) == 1
    fhandle = open(realdir + "/STATUS", "w")
    fhandle.write(
        (
            "Current host                    : st-rst16-02-03/x86_64  "
            "file-server:10.14.10.238\n"
            "LSF JOBID: not running LSF\n"
            "COPY_FILE                       : 20:58:55 .... 20:58:57\n"
            "COPY_FILE                       : 20:58:57 .... 20:59:00  "
            " EXIT: 1/Executable: /project/res/komodo/2018.02/root/etc/ERT/"
            "Config/jobs/util/script/copy_file.py failed with exit code: 1 "
        )
    )
    fhandle.close()
    real = ensemble.ScratchRealization(realdir)
    assert len(real.get_df("STATUS")) == 2
    # Check that we have the error string picked up:
    assert real.get_df("STATUS")["errorstring"].dropna().values[0] == (
        "EXIT: 1/Executable: /project/res/komodo/2018.02/root/"
        "etc/ERT/Config/jobs/util/script/copy_file.py failed with exit code: 1"
    )  # noqa

    # Check that we can move the Eclipse files to another place
    # in the realization dir and still load summary data:
    shutil.move(realdir + "/eclipse", realdir + "/eclipsedir")
    real = ensemble.ScratchRealization(realdir)

    # load_smry() is now the same as no UNSMRY file found,
    # an empty dataframe (and there would be some logging)
    assert real.load_smry().empty

    # Now discover the UNSMRY file explicitly, then load_smry()
    # should work.
    unsmry_file = real.find_files("eclipsedir/model/*.UNSMRY")
    # Non-empty dataframe:
    assert not real.load_smry().empty
    assert len(unsmry_file) == 1
    assert isinstance(unsmry_file, pd.DataFrame)

    # Test having values with spaces in parameters.txt
    # Unquoted valued with spaces will be truncated,
    # quoted valued will be correctly parsed
    # (read_csv(sep='\s+') is the parser)
    param_file = open(realdir + "/parameters.txt", "a")
    param_file.write("FOOBAR 1 2 3 4 5 6\n")
    param_file.write('FOOSPACES "1 2 3 4 5 6"\n')
    param_file.close()

    real = ensemble.ScratchRealization(realdir)
    assert real.parameters["FOOBAR"] == 1
    assert real.parameters["FOOSPACES"] == "1 2 3 4 5 6"

    # Clean up when finished. This often fails on network drives..
    shutil.rmtree(datadir + "/" + tmpensname, ignore_errors=True)


def test_apply(tmpdir):
    """
    Test the callback functionality
    """
    testdir = os.path.dirname(os.path.abspath(__file__))
    tmpdir.chdir()

    symlink_iter(os.path.join(testdir, "data/testensemble-reek001"), "iter-0")
    realdir = "realization-0/iter-0"
    real = ensemble.ScratchRealization(realdir)

    def ex_func1():
        """Example constant function"""
        return pd.DataFrame(
            index=["1", "2"], columns=["foo", "bar"], data=[[1, 2], [3, 4]]
        )

    result = real.apply(ex_func1)
    assert isinstance(result, pd.DataFrame)

    # Apply and store the result:
    real.apply(ex_func1, localpath="df-1234")
    internalized_result = real.get_df("df-1234")
    assert isinstance(internalized_result, pd.DataFrame)
    assert (result == internalized_result).all().all()

    # Check that the submitted function can utilize data from **kwargs
    def ex_func2(kwargs):
        """Example function using kwargs"""
        arg = kwargs["foo"]
        return pd.DataFrame(
            index=["1", "2"], columns=["foo", "bar"], data=[[arg, arg], [arg, arg]]
        )

    result2 = real.apply(ex_func2, foo="bar")
    assert result2.iloc[0, 0] == "bar"

    # We require applied function to return only DataFrames.
    def scalar_func():
        """Dummy scalar function"""
        return 1

    with pytest.raises(ValueError):
        real.apply(scalar_func)

    # The applied function should have access to the realization object:
    def real_func(kwargs):
        """Example function that accesses the realization object"""
        return pd.DataFrame(
            index=[0], columns=["path"], data=kwargs["realization"].runpath()
        )

    origpath = real.apply(real_func)
    assert os.path.exists(origpath.iloc[0, 0])

    # Do not allow supplying the realization object to apply:
    with pytest.raises(ValueError):
        real.apply(real_func, realization="foo")

    if SKIP_FMU_TOOLS:
        return
    # Test if we can wrap the volumetrics-parser in fmu.tools:
    # It cannot be applied directly, as we need to combine the
    # realization's root directory with the relative path coming in:

    def rms_vol2df(kwargs):
        """Small function that is to be supplied to .apply()"""
        return volumetrics.rmsvolumetrics_txt2df(
            os.path.join(kwargs["realization"].runpath(), kwargs["filename"])
        )

    rmsvol_df = real.apply(
        rms_vol2df, filename="share/results/volumes/" + "geogrid_vol_oil_1.txt"
    )
    assert rmsvol_df["STOIIP_OIL"].sum() > 0

    # Also try internalization:
    real.apply(
        rms_vol2df,
        filename="share/results/volumes/" + "geogrid_vol_oil_1.txt",
        localpath="share/results/volumes/geogrid--oil.csv",
    )
    assert real.get_df("geogrid--oil")["STOIIP_OIL"].sum() > 0

    # Run rms_vol2df in batch when initializing:
    real = ensemble.ScratchRealization(
        realdir,
        batch=[
            {
                "apply": {
                    "callback": rms_vol2df,
                    "filename": "share/results/volumes/" + "geogrid_vol_oil_1.txt",
                    "localpath": "share/results/volumes/geogrid--oil.csv",
                }
            }
        ],
    )
    assert real.get_df("geogrid--oil")["STOIIP_OIL"].sum() > 0


def test_drop():
    """Test the drop functionality, where can delete
    parts of internalized data"""
    testdir = os.path.dirname(os.path.abspath(__file__))
    realdir = os.path.join(testdir, "data/testensemble-reek001", "realization-0/iter-0")
    real = ensemble.ScratchRealization(realdir)

    parametercount = len(real.parameters)
    real.drop("parameters", key="RMS_SEED")
    assert len(real.parameters) == parametercount - 1

    real.drop("parameters", keys=["L_1GO", "E_1GO"])
    assert len(real.parameters) == parametercount - 3

    real.drop("parameters", key="notexistingkey")
    # This will go unnoticed
    assert len(real.parameters) == parametercount - 3

    real.load_smry(column_keys="FOPT", time_index="monthly")
    datecount = len(real.get_df("unsmry--monthly"))
    real.drop("unsmry--monthly", rowcontains="2000-01-01")
    assert len(real.get_df("unsmry--monthly")) == datecount - 1

    real.drop("parameters")
    assert "parameters.txt" not in real.keys()


def test_find_files_comps():
    """Test the more exotic features of find_files

    Components extracted from filenames.
    """

    testdir = os.path.dirname(os.path.abspath(__file__))
    realdir = os.path.join(testdir, "data/testensemble-reek001", "realization-0/iter-0")
    real = ensemble.ScratchRealization(realdir)

    # Make some filenames we can later "find", including some problematic ones.
    findable_files = [
        "foo--bar--com.gri",
        "foo-bar--com.gri",
        "foo---bar--com.gri",
        "--bar--.gri",
    ]
    for filename in findable_files:
        with open(os.path.join(realdir, filename), "w") as fileh:
            fileh.write("baah")

    real.find_files("*.gri")

    files_df = real.files.set_index("BASENAME")
    assert "COMP0" not in real.files  # We are 1-based, not zero.
    assert "COMP1" in real.files
    assert "COMP2" in real.files
    assert "COMP3" in real.files
    assert "COMP4" not in real.files

    assert files_df.loc["foo--bar--com.gri"]["COMP1"] == "foo"
    assert files_df.loc["foo--bar--com.gri"]["COMP2"] == "bar"
    assert files_df.loc["foo--bar--com.gri"]["COMP3"] == "com"
    assert files_df.loc["foo-bar--com.gri"]["COMP1"] == "foo-bar"
    assert files_df.loc["foo-bar--com.gri"]["COMP2"] == "com"
    assert files_df.loc["foo---bar--com.gri"]["COMP1"] == "foo"
    assert files_df.loc["foo---bar--com.gri"]["COMP2"] == "-bar"
    assert files_df.loc["foo---bar--com.gri"]["COMP3"] == "com"
    assert files_df.loc["--bar--.gri"]["COMP1"] == ""
    assert files_df.loc["--bar--.gri"]["COMP2"] == "bar"
    assert files_df.loc["--bar--.gri"]["COMP3"] == ""

    # Cleanup
    for filename in findable_files:
        if os.path.exists(os.path.join(realdir, filename)):
            os.unlink(os.path.join(realdir, filename))


def test_find_files_yml():
    """Test the more exotic features of find_files

    Meta-data in yaml files.
    """
    testdir = os.path.dirname(os.path.abspath(__file__))
    realdir = os.path.join(testdir, "data/testensemble-reek001", "realization-0/iter-0")
    real = ensemble.ScratchRealization(realdir)

    # Setup example files with some yaml data:
    findable_files = ["grid1.gri", "grid2.gri"]
    for filename in findable_files:
        with open(os.path.join(realdir, filename), "w") as fileh:
            fileh.write("baah")
        yamlfile = "." + filename + ".yml"
        with open(os.path.join(realdir, yamlfile), "w") as fileh:
            fileh.write(yaml.dump(dict(a=dict(x=1, y=2), b="bar")))

    # Now find the gri files, and add metadata:
    files_df = real.find_files("*.gri", metayaml=True)

    assert "a--x" in files_df
    assert "a--y" in files_df
    assert "b" in files_df
    assert files_df["b"].unique()[0] == "bar"
    assert files_df["a--x"].astype(int).unique()[0] == 1
    assert files_df["a--y"].astype(int).unique()[0] == 2

    # Cleanup
    for filename in findable_files:
        if os.path.exists(os.path.join(realdir, filename)):
            os.unlink(os.path.join(realdir, filename))
        yamlfile = "." + filename + ".yml"
        if os.path.exists(os.path.join(realdir, yamlfile)):
            os.unlink(os.path.join(realdir, yamlfile))


def test_get_smry_meta():
    """
    Test getting eclsum metadata for single realization.
    """
    testdir = os.path.dirname(os.path.abspath(__file__))
    realdir = os.path.join(testdir, "data/testensemble-reek001", "realization-0/iter-0")
    real = ensemble.ScratchRealization(realdir)

    meta = real.get_smry_meta(column_keys=["*"])
    assert isinstance(meta, dict)
    assert "FOPT" in meta
    assert "FOPTH" in meta
    assert meta["FOPT"]["unit"] == "SM3"
    assert meta["FOPR"]["unit"] == "SM3/DAY"
    assert meta["FOPT"]["is_total"]
    assert not meta["FOPR"]["is_total"]
    assert not meta["FOPT"]["is_rate"]
    assert meta["FOPR"]["is_rate"]
    assert not meta["FOPT"]["is_historical"]
    assert meta["FOPTH"]["is_historical"]

    assert meta["WOPR:OP_1"]["wgname"] == "OP_1"
    assert meta["WOPR:OP_1"]["keyword"] == "WOPR"
    if "wgname" in meta["FOPT"]:
        # Not enforced yet to have None fields actually included
        assert meta["FOPT"]["wgname"] is None

    # Can create dataframes like this:
    meta_df = pd.DataFrame.from_dict(meta, orient="index")
    hist_keys = meta_df[meta_df["is_historical"]].index
    assert all([key.split(":")[0].endswith("H") for key in hist_keys])

    # When virtualizing a realization, smry data must be loaded
    # for smry metadata to be conserved
    real.load_smry()
    vreal = real.to_virtual()
    vmeta = vreal.get_smry_meta()
    assert "FOPT" in vmeta
    assert vmeta["FOPR"]["unit"] == "SM3/DAY"


def test_get_df_merge():
    """Test that we can merge on the fly using get_df()"""
    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    realdir = os.path.join(testdir, "data/testensemble-reek001", "realization-0/iter-0")
    real = ensemble.ScratchRealization(realdir)
    onlysmry = real.load_smry(column_keys="F*", time_index="monthly")
    assert "parameters.txt" in real.data

    paramcount = len(real.parameters)
    smrycount = len(onlysmry.columns)
    smry = real.get_df("unsmry--monthly", merge="parameters.txt")
    assert len(smry.columns) == paramcount + smrycount
    assert len(smry["SORG1"].unique()) == 1

    # Merge with list should also work:
    smry = real.get_df("unsmry--monthly", merge=["parameters.txt"])
    assert len(smry.columns) == paramcount + smrycount

    # Merge with empty list:
    smry = real.get_df("unsmry--monthly", merge=[])
    assert len(smry.columns) == smrycount

    # Merge with multiple output sets:
    outputs = real.load_txt("outputs.txt")
    smry = real.get_df("unsmry--monthly", merge=["parameters", "outputs"])
    assert len(smry.columns) == paramcount + smrycount + len(outputs)

    # Merge with scalar data, and combination of scalar and dict data:
    real.load_scalar("npv.txt")
    smry = real.get_df("unsmry--monthly", merge="npv.txt")
    assert len(smry.columns) == smrycount + 1
    smry = real.get_df("unsmry--monthly", merge=["parameters.txt", "npv.txt"])
    assert len(smry.columns) == smrycount + paramcount + 1

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

    scalar_dict = real.get_df("npv.txt", merge="outputs")
    assert "npv.txt" in scalar_dict
    assert "top_structure" in scalar_dict

    # Inject a random dict and merge with:
    real.data["foodict"] = dict(BAR="COM")
    dframe = real.get_df("parameters", merge="foodict")
    assert "BAR" in dframe
    assert "SORG1" in dframe

    # Merge something that is not mergeable
    real.data["randtable"] = pd.DataFrame(
        columns=["BARF", "ARBF"], data=[[1, 3], [2, 4]]
    )
    with pytest.raises(TypeError):
        # pylint: disable=pointless-statement
        real.get_df("parameters", merge="randtable")

    with pytest.raises(pd.errors.MergeError):
        # pylint: disable=pointless-statement
        real.get_df("unsmry--monthly", merge="randtable")
