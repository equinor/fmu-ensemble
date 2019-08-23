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
logger = fmux.basiclogger(__name__, level="WARNING")

if not fmux.testsetup():
    raise SystemExit()


def test_rmrc():
    """Test the properties of a virtualized ScratchEnsemble on JS data"""

    ens_path = "/scratch/johan_sverdrup2/peesv/2019a_b003p2p0_pred_102_9aug_ff_a/"
    storage = "/scratch/johan_sverdrup2/rmrc/ens1/"
    iteration_name = "pred"
    local_paths = ["share/maps/depth", "share/maps/isochores"]

    ens = ScratchEnsemble(
        "ens1", os.path.join(ens_path, "realization-?/{}".format(iteration_name))
    )
    ens.load_smry(time_index="yearly")
    ens.find_files("share/maps/depth/*.gri", metadata={"surfacetype": "depthsurface"})
    ens.find_files("share/maps/isochores/*.gri", metadata={"surfacetype": "isochores"})
    ens.to_virtual().to_disk(
        "js_vens_dump", delete=True, includefiles=True, symlinks=True
    )

    vens = VirtualEnsemble(fromdisk="js_vens_dump")

    assert not vens.files.empty

    assert "surfacetype" in vens.files
    assert set(vens.files["surfacetype"].dropna().unique()) == set(["depthsurface", "isochores"])

    for _, surfacefilerow in vens.files[vens.files["FILETYPE"] == "gri"].iterrows():
        assert os.path.exists(
            "js_vens_dump/__discoveredfiles/realization-"
            + str(surfacefilerow["REAL"])
            + "/"
            + surfacefilerow["LOCALPATH"]
        )
