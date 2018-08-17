# -*- coding: utf-8 -*-
"""Testing fmu-ensemble, EnsembleSet clas."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import glob

from fmu import config
from fmu import ensemble

fmux = config.etc.Interaction()
logger = fmux.basiclogger(__name__)

if not fmux.testsetup():
    raise SystemExit()


def test_ensembleset_reek001():
    """Test import of a stripped 5 realization ensemble,
    manually doubled to two identical ensembles
    """

    ensdir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "data/testensemble-reek001/")

    # Copy iter-0 to iter-1, creating an identical ensemble
    # we can load for testing.
    for realizationdir in glob.glob(ensdir + '/realization-*'):
        if os.path.exists(realizationdir + '/iter-1'):
            os.remove(realizationdir + '/iter-1')
        os.symlink(realizationdir + '/iter-0',
                   realizationdir + '/iter-1')

    iter0 = ensemble.ScratchEnsemble('iter-0',
                                     ensdir + '/realization-*/iter-0')
    iter1 = ensemble.ScratchEnsemble('iter-1',
                                     ensdir + '/realization-*/iter-1')

    ensset = ensemble.EnsembleSet("reek001", [iter0, iter1])
    assert len(ensset) == 2
    assert len(ensset['iter-0'].get_status_data()) == 250
    assert len(ensset['iter-1'].get_status_data()) == 250

    # Try adding the same object over again
    ensset.add_ensemble(iter0)
    assert len(ensset) == 2  # Unchanged!

    # Initialize starting from empty ensemble
    ensset2 = ensemble.EnsembleSet("reek001", [])
    ensset2.add_ensemble(iter0)
    ensset2.add_ensemble(iter1)
    assert len(ensset2) == 2

    # Initialize directly from path with globbing:
    ensset3 = ensemble.EnsembleSet("reek001direct", [])
    ensset3.add_ensembles_frompath(ensdir)
    assert len(ensset3) == 2

    # Delete the symlinks when we are done.
    for realizationdir in glob.glob(ensdir + '/realization-*'):
        os.remove(realizationdir + '/iter-1')
