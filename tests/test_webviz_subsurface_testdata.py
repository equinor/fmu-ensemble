"""Testing loading the webviz subsurface testdata."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import datetime

import pytest

from fmu.ensemble import etc
from fmu.ensemble import ScratchEnsemble, ScratchRealization
from fmu.ensemble.common import use_concurrent


def check_testdata():
    """Check if we have webviz subsurface testdata, skip if not"""
    testdir = os.path.dirname(os.path.abspath(__file__))
    if not os.path.exists(os.path.join(testdir, "data/webviz-subsurface-testdata")):
        print("Skipping loading webviz-subsurface-testdata")
        print("Do")
        print(" $ cd tests/data")
        print(
            " $ git clone git clone --depth 1 https://github.com/equinor/webviz-subsurface-testdata"
        )
        print("to download and use with pytest")
        pytest.skip()


def test_webviz_subsurface_testdata():
    """Check that we can load the webviz subsurface testdata"""

    check_testdata()

    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    ensdir = os.path.join(testdir, "data/webviz-subsurface-testdata/reek_fullmatrix/")
    ens = ScratchEnsemble("reek_fullmatrix", ensdir + "realization-*/iter-0")

    smry_monthly = ens.load_smry()
    assert "REAL" in smry_monthly
    assert len(smry_monthly["REAL"].unique()) == 40

    ens.load_csv("share/results/tables/relperm.csv")
    ens.load_csv("share/results/tables/equil.csv")
    ens.load_csv("share/results/tables/rft.csv")
    ens.load_csv("share/results/tables/pvt.csv")

    ens.load_csv("share/results/volumes/simulator_volume_fipnum.csv")
    ens.load_csv("share/results/volumes/geogrid--oil.csv")
    ens.load_csv("share/results/volumes/simgrid--oil.csv")

    assert len(ens.keys()) == 11


def test_webviz_subsurface_testdata_batch():
    """Check that we can load the webviz subsurface testdata in batch

    Also display timings, this should reveal that concurrent operations
    are actually faster.
    """
    testdir = os.path.dirname(os.path.abspath(__file__))
    ensdir = os.path.join(testdir, "data/webviz-subsurface-testdata/reek_fullmatrix/")
    start_time = datetime.datetime.now()
    batch_cmds = [
        {"load_smry": {"column_keys": "*", "time_index": "yearly"}},
        {"load_smry": {"column_keys": "*", "time_index": "monthly"}},
        {"load_smry": {"column_keys": "*", "time_index": "last"}},
        {"load_smry": {"column_keys": "*", "time_index": "daily"}},
        {"load_csv": {"localpath": "share/results/tables/relperm.csv"}},
        {"load_csv": {"localpath": "share/results/tables/equil.csv"}},
        {"load_csv": {"localpath": "share/results/tables/rft.csv"}},
        {"load_csv": {"localpath": "share/results/tables/pvt.csv"}},
        {
            "load_csv": {
                "localpath": "share/results/volumes/simulator_volume_fipnum.csv"
            }
        },
        {"load_csv": {"localpath": "share/results/volumes/geogrid--oil.csv"}},
        {"load_csv": {"localpath": "share/results/volumes/simgrid--oil.csv"}},
    ]
    ens = ScratchEnsemble(
        "reek_fullmatrix", ensdir + "realization-*/iter-0", batch=batch_cmds
    )
    end_time = datetime.datetime.now()
    elapsed = (end_time - start_time).total_seconds()
    print("FMU_CONCURRENCY: {}".format(use_concurrent()))
    print("Elapsed time for batch ensemble initialization: {}".format(elapsed))
    assert len(ens.keys()) == 3 + len(batch_cmds)  # 3 more than length of batch
