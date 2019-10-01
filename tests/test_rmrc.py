# -*- coding: utf-8 -*-
"""Testing VirtualEnsembles for RMRC problems."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import numpy as np
import pandas as pd
import pytest

import xtgeo

from fmu.ensemble import etc
from fmu.ensemble import ScratchEnsemble, VirtualEnsemble

fmux = etc.Interaction()
logger = fmux.basiclogger(__name__, level="INFO")

if not fmux.testsetup():
    raise SystemExit()


def test_rmrc():
    """Test the properties of a virtualized ScratchEnsemble on JS data"""

    ens_path = "/scratch/johan_sverdrup2/peesv/2019a_b003p2p0_pred_102_9aug_ff_a/"
    storage = "/scratch/johan_sverdrup2/rmrc/ens1/"
    iteration_name = "pred"
    local_paths = ["share/maps/depth", "share/maps/isochores"]

    if not os.path.exists(ens_path):
        pytest.skip("Test data not available")

    logger.info("Constructing ensemble object from disk")
    ens = ScratchEnsemble(
        "ens1", os.path.join(ens_path, "realization-*/{}".format(iteration_name))
    )
    logger.info("Loading smry")
    ens.load_smry(time_index="yearly")
    logger.info("Finding *gri files")
    ens.find_files(
        "share/maps/depth/*.gri",
        metadata={"surfacetype": "depthsurface"},
        metayaml=True,
    )
    ens.find_files(
        "share/maps/isochores/*.gri",
        metadata={"surfacetype": "isochores"},
        metayaml=True,
    )
    logger.info("Dumping to disk (js_vens_dump)")
    ens.to_virtual().to_disk(
        "js_vens_dump", delete=True, dumpcsv=True, includefiles=True, symlinks=True
    )
    logger.info("Loading back from disk")
    vens = VirtualEnsemble(fromdisk="js_vens_dump")

    logger.info("Doing asserts")
    assert not vens.files.empty

    assert "surfacetype" in vens.files
    assert set(vens.files["surfacetype"].dropna().unique()) == set(
        ["depthsurface", "isochores"]
    )

    for _, surfacefilerow in vens.files[vens.files["FILETYPE"] == "gri"].iterrows():
        assert os.path.exists(
            "js_vens_dump/__discoveredfiles/realization-"
            + str(surfacefilerow["REAL"])
            + "/"
            + surfacefilerow["LOCALPATH"]
        )
