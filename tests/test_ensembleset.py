# -*- coding: utf-8 -*-
"""Testing fmu-ensemble, EnsembleSet clas."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import glob
import pytest
import shutil
import pandas as pd

from fmu import config
from fmu import ensemble

fmux = config.etc.Interaction()
logger = fmux.basiclogger(__name__)

if not fmux.testsetup():
    raise SystemExit()


def test_ensembleset_reek001(tmp='TMP'):
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
    try:
        ensset.add_ensemble(iter0)
    except ValueError:
        pass
    assert len(ensset) == 2  # Unchanged!

    # Initialize starting from empty ensemble
    ensset2 = ensemble.EnsembleSet("reek001", [])
    ensset2.add_ensemble(iter0)
    ensset2.add_ensemble(iter1)
    assert len(ensset2) == 2

    # Check that we can skip the empty list:
    ensset2x = ensemble.EnsembleSet("reek001")
    ensset2x.add_ensemble(iter0)
    ensset2x.add_ensemble(iter1)
    assert len(ensset2x) == 2

    # Initialize directly from path with globbing:
    ensset3 = ensemble.EnsembleSet("reek001direct", [])
    ensset3.add_ensembles_frompath(ensdir)
    assert len(ensset3) == 2

    # Alternative globbing:
    ensset4 = ensemble.EnsembleSet("reek001direct2", frompath=ensdir)
    assert len(ensset4) == 2

    # Testing aggregation of parameters
    paramsdf = ensset3.parameters
    if not os.path.exists(tmp):
        os.mkdir(tmp)
    paramsdf.to_csv(os.path.join(tmp, 'enssetparams.csv'), index=False)
    assert isinstance(paramsdf, pd.DataFrame)
    assert len(ensset3.parameters) == 10
    assert len(ensset3.parameters.columns) == 27
    assert 'ENSEMBLE' in ensset3.parameters.columns
    assert 'REAL' in ensset3.parameters.columns

    outputs = ensset3.load_txt('outputs.txt')
    assert 'NPV' in outputs.columns

    # Test Eclipse summary handling:
    assert len(ensset3.get_smry_dates(freq='report')) == 641
    assert len(ensset3.get_smry_dates(freq='monthly')) == 37
    assert len(ensset3.load_smry(column_keys=['FOPT'],
                                 time_index='yearly')) == 50
    monthly = ensset3.load_smry(column_keys=['F*'],
                                time_index='monthly')
    assert 'ENSEMBLE' == monthly.columns[0]
    assert 'REAL' == monthly.columns[1]
    assert 'DATE' == monthly.columns[2]

    # Check that we can retrieve cached versions
    assert len(ensset3.get_df('unsmry-monthly')) == 380
    assert len(ensset3.get_df('unsmry-yearly')) == 50
    monthly.to_csv(os.path.join(tmp, 'ensset-monthly.csv'), index=False)

    with pytest.raises(ValueError):
        ensset3.get_df('unsmry-weekly')

    # Check errors when we ask for stupid stuff
    with pytest.raises(ValueError):
        ensset3.load_csv('bogus.csv')
    with pytest.raises(ValueError):
        ensset3.get_df('bogus.csv')

    # Test aggregation of csv files:
    vol_df = ensset3.load_csv('share/results/volumes/' +
                              'simulator_volume_fipnum.csv')
    assert 'REAL' in vol_df
    assert 'ENSEMBLE' in vol_df
    assert len(vol_df['REAL'].unique()) == 3
    assert len(vol_df['ENSEMBLE'].unique()) == 2
    assert len(ensset3.keys()) == 7

    # Test scalar imports:
    ensset3.load_scalar('npv.txt')
    npv = ensset3.get_df('npv.txt')
    assert 'ENSEMBLE' in npv
    assert 'REAL' in npv
    assert 'npv.txt' in npv
    assert len(npv) == 10
    # Scalar import with forced numerics:
    ensset3.load_scalar('npv.txt', convert_numeric=True, force_reread=True)
    npv = ensset3.get_df('npv.txt')
    assert len(npv) == 8

    predel_len = len(ensset3.keys())
    ensset3.drop('parameters.txt')
    assert len(ensset3.keys()) == predel_len - 1

    # Initialize differently, using only the root path containing
    # realization-*
    ensset4 = ensemble.EnsembleSet("foo", frompath=ensdir)
    assert len(ensset4) == 2
    assert isinstance(ensset4['iter-0'], ensemble.ScratchEnsemble)
    assert isinstance(ensset4['iter-1'], ensemble.ScratchEnsemble)

    # Delete the symlinks when we are done.
    for realizationdir in glob.glob(ensdir + '/realization-*'):
        os.remove(realizationdir + '/iter-1')


def test_pred_dir():
    """Test import of a stripped 5 realization ensemble,
    manually doubled to two identical ensembles,
    plus a prediction ensemble
    """

    if '__file__' in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath('.')
    ensdir = os.path.join(testdir,
                          "data/testensemble-reek001/")

    # Copy iter-0 to iter-1, creating an identical ensemble
    # we can load for testing. Delete in case it exists
    for realizationdir in glob.glob(ensdir + '/realization-*'):
        if os.path.exists(realizationdir + '/iter-1'):
            os.remove(realizationdir + '/iter-1')
        os.symlink(realizationdir + '/iter-0',
                   realizationdir + '/iter-1')
        if os.path.exists(realizationdir + '/pred-dg3'):
            os.remove(realizationdir + '/pred-dg3')
        os.symlink(realizationdir + '/iter-0',
                   realizationdir + '/pred-dg3')

    # Initialize differently, using only the root path containing
    # realization-*. The frompath argument does not support
    # anything but iter-* naming convention for ensembles (yet?)
    ensset = ensemble.EnsembleSet("foo", frompath=ensdir)
    assert len(ensset) == 2
    assert isinstance(ensset['iter-0'], ensemble.ScratchEnsemble)
    assert isinstance(ensset['iter-1'], ensemble.ScratchEnsemble)

    # We need to be more explicit to include the pred-dg3 directory:
    pred_ens = ensemble.ScratchEnsemble('pred-dg3',
                                        ensdir + "realization-*/pred-dg3")
    ensset.add_ensemble(pred_ens)
    assert isinstance(ensset['pred-dg3'], ensemble.ScratchEnsemble)
    assert len(ensset) == 3

    # Check the flagging in aggregated data:
    yearlysum = ensset.load_smry(time_index='yearly')
    assert 'ENSEMBLE' in yearlysum.columns

    ens_list = list(yearlysum['ENSEMBLE'].unique())
    assert len(ens_list) == 3
    assert 'pred-dg3' in ens_list
    assert 'iter-0' in ens_list
    assert 'iter-1' in ens_list

    # Try to add a new ensemble with a similar name to an existing:
    foo_ens = ensemble.ScratchEnsemble('pred-dg3',
                                       ensdir + "realization-*/iter-1")
    with pytest.raises(ValueError):
        ensset.add_ensemble(foo_ens)
    assert len(ensset) == 3

    # Delete the symlinks when we are done.
    for realizationdir in glob.glob(ensdir + '/realization-*'):
        os.remove(realizationdir + '/iter-1')
        os.remove(realizationdir + '/pred-dg3')


def test_mangling_data():
    """Test import of a stripped 5 realization ensemble,
    manually doubled to two identical ensembles,
    and then with some data removed
    """

    if '__file__' in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath('.')
    ensdir = os.path.join(testdir,
                          "data/testensemble-reek001/")

    # Copy iter-0 to iter-1, creating an identical ensemble
    # we can load for testing. Delete in case it exists
    for realizationdir in glob.glob(ensdir + '/realization-*'):
        if os.path.exists(realizationdir + '/iter-1'):
            if os.path.islink(realizationdir + '/iter-1'):
                os.remove(realizationdir + '/iter-1')
            else:
                shutil.rmtree(realizationdir + '/iter-1')
        # Symlink each file/dir individually (so we can remove some)
        os.mkdir(realizationdir + '/iter-1')
        for realizationcomponent in glob.glob(realizationdir + '/iter-0/*'):
            if ('parameters.txt' not in realizationcomponent) and \
               ('outputs.txt' not in realizationcomponent):
                os.symlink(realizationcomponent,
                           realizationcomponent.replace('iter-0', 'iter-1'))

    ensset = ensemble.EnsembleSet("foo", frompath=ensdir)
    assert len(ensset) == 2
    assert isinstance(ensset['iter-0'], ensemble.ScratchEnsemble)
    assert isinstance(ensset['iter-1'], ensemble.ScratchEnsemble)

    assert 'parameters.txt' in ensset.keys()

    # We should only have parameters in iter-0
    params = ensset.get_df('parameters.txt')
    assert len(params) == 5
    assert params['ENSEMBLE'].unique() == 'iter-0'

    ensset.load_txt('outputs.txt')
    assert 'outputs.txt' in ensset.keys()
    assert len(ensset.get_df('outputs.txt') == 4)

    # When it does not exist in any of the ensembles, we
    # should error
    with pytest.raises(ValueError):
        ensset.get_df('foobar')

    # Delete the symlinks when we are done.
    for realizationdir in glob.glob(ensdir + '/realization-*'):
        shutil.rmtree(realizationdir + '/iter-1')
