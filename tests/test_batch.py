"""Testing batch functions in fmu-ensemble."""

import os
import logging

import yaml

from fmu.ensemble import ScratchEnsemble, EnsembleSet

logger = logging.getLogger(__name__)


def test_batch():
    """Test batch processing at time of object initialization"""
    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    ens = ScratchEnsemble(
        "reektest",
        testdir + "/data/testensemble-reek001/" + "realization-*/iter-0",
        batch=[
            {"load_scalar": {"localpath": "npv.txt"}},
            {"load_smry": {"column_keys": "FOPT", "time_index": "yearly"}},
            {"load_smry": {"column_keys": "*", "time_index": "daily"}},
        ],
    )
    assert len(ens.get_df("npv.txt")) == 5
    assert len(ens.get_df("unsmry--daily")["FOPR"]) == 5490
    assert len(ens.get_df("unsmry--yearly")["FOPT"]) == 25

    # Also possible to batch process afterwards:
    ens = ScratchEnsemble(
        "reektest", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0"
    )
    ens.process_batch(
        batch=[
            {"load_scalar": {"localpath": "npv.txt"}},
            {"load_smry": {"column_keys": "FOPT", "time_index": "yearly"}},
            {"load_smry": {"column_keys": "*", "time_index": "daily"}},
        ]
    )
    assert len(ens.get_df("npv.txt")) == 5
    assert len(ens.get_df("unsmry--daily")["FOPR"]) == 5490
    assert len(ens.get_df("unsmry--yearly")["FOPT"]) == 25


def test_yaml():
    """Test loading batch commands from yaml files"""

    # This is subject to change

    yamlstr = """
scratch_ensembles:
  iter1: data/testensemble-reek001/realization-*/iter-0
batch:
  - load_scalar:
      localpath: npv.txt
  - load_smry:
      column_keys: FOPT
      time_index: yearly
  - load_smry:
      column_keys: "*"
      time_index: daily"""
    ymlconfig = yaml.safe_load(yamlstr)

    testdir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(testdir)
    ensset = EnsembleSet()

    for ensname, enspath in ymlconfig["scratch_ensembles"].items():
        ensset.add_ensemble(ScratchEnsemble(ensname, paths=enspath))
    ensset.process_batch(ymlconfig["batch"])

    assert "parameters.txt" in ensset.keys()
    assert "OK" in ensset.keys()
    assert "npv.txt" in ensset.keys()
    assert not ensset.get_df("unsmry--yearly").empty
