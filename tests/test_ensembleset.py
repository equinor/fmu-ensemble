# -*- coding: utf-8 -*-
"""Testing fmu-ensemble, EnsembleSet clas."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import glob
import pytest
import pandas as pd

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

    if '__file__' in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath('.')
    ensdir = os.path.join(testdir,
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
    assert len(ensset['iter-0'].get_df('STATUS')) == 250
    assert len(ensset['iter-1'].get_df('STATUS')) == 250

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

    # Testing aggregation of parameters
    paramsdf = ensset3.parameters
    paramsdf.to_csv('enssetparams.csv', index=False)
    assert isinstance(paramsdf, pd.DataFrame)
    assert len(ensset3.parameters) == 10
    assert len(ensset3.parameters.columns) == 27
    assert 'ENSEMBLE' in ensset3.parameters.columns
    assert 'REAL' in ensset3.parameters.columns

    outputs = ensset3.from_txt('outputs.txt')
    assert 'NPV' in outputs.columns

    # Test Eclipse summary handling:
    assert len(ensset3.get_smry_dates(freq='report')) == 641
    assert len(ensset3.get_smry_dates(freq='monthly')) == 37
    assert len(ensset3.get_smry(column_keys=['FOPT'],
                                time_index='yearly')) == 40

    # Check errors when we ask for stupid stuff
    with pytest.raises(ValueError):
        ensset3.from_csv('bogus.csv')
    with pytest.raises(ValueError):
        ensset3.get_df('bogus.csv')

    # Test aggregation of csv files:
    vol_df = ensset3.from_csv('share/results/volumes/' +
                              'simulator_volume_fipnum.csv')
    assert 'REAL' in vol_df
    assert 'ENSEMBLE' in vol_df
    assert len(vol_df['REAL'].unique()) == 3
    assert len(vol_df['ENSEMBLE'].unique()) == 2

    # Delete the symlinks when we are done.
    for realizationdir in glob.glob(ensdir + '/realization-*'):
        os.remove(realizationdir + '/iter-1')
