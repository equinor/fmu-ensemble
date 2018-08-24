# -*- coding: utf-8 -*-
"""Testing fmu-ensemble."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import numpy

from fmu import config
from fmu import ensemble

fmux = config.etc.Interaction()
logger = fmux.basiclogger(__name__)

if not fmux.testsetup():
    raise SystemExit()


def test_reek001():
    """Test import of a stripped 5 realization ensemble"""

    if '__file__' in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath('.')

    reekensemble = ensemble.ScratchEnsemble('reektest',
                                            testdir +
                                            '/data/testensemble-reek001/' +
                                            'realization-*/iter-0')
    assert isinstance(reekensemble, ensemble.ScratchEnsemble)
    assert reekensemble.name == 'reektest'
    assert len(reekensemble) == 5

    assert len(reekensemble.files[
        reekensemble.files.LOCALPATH == 'jobs.json']) == 5
    assert len(reekensemble.files[
        reekensemble.files.LOCALPATH == 'parameters.txt']) == 5
    assert len(reekensemble.files[
        reekensemble.files.LOCALPATH == 'STATUS']) == 5

    statusdf = reekensemble.get_df('STATUS')
    assert len(statusdf) == 250  # 5 realizations, 50 jobs in each
    assert 'DURATION' in statusdf.columns  # calculated
    assert 'argList' in statusdf.columns  # from jobs.json
    assert int(statusdf.loc[245, 'DURATION']) == 195  # sample check
    # STATUS in real4 is modified to simulate that Eclipse never finished:
    assert numpy.isnan(statusdf.loc[249, 'DURATION'])

    statusdf.to_csv('status.csv', index=False)

    # Parameters.txt
    paramsdf = reekensemble.from_txt('parameters.txt')
    assert len(paramsdf) == 5  # 5 realizations
    paramsdf = reekensemble.parameters  # also test as property
    assert len(paramsdf.columns) == 26  # 25 parameters, + REAL column
    paramsdf.to_csv('params.csv', index=False)

    # The column FOO in parameters is only present in some, and
    # is present with NaN in real0:
    assert 'FOO' in reekensemble.parameters.columns
    assert len(reekensemble.parameters['FOO'].dropna()) == 1
    # (NaN ine one real, and non-existing in the others is the same thing)

    # Test loading of another txt file:
    reekensemble.from_txt('outputs.txt')
    assert 'NPV' in reekensemble.from_txt('outputs.txt').columns
    # Check implicit discovery
    assert 'outputs.txt' in reekensemble.files['LOCALPATH'].values

    # File discovery:
    reekensemble.find_files('share/results/volumes/*csv',
                            metadata={'GRID': 'simgrid'})

    reekensemble.files.to_csv('files.csv', index=False)

    # CSV files
    vol_df = reekensemble.from_csv('share/results/volumes/' +
                                   'simulator_volume_fipnum.csv')
    assert 'REAL' in vol_df
    assert len(vol_df['REAL'].unique()) == 3  # missing in 2 reals
    vol_df.to_csv('simulatorvolumes.csv', index=False)

    # Realization deletion:
    reekensemble.remove_realizations([1, 3])
    assert len(reekensemble) == 3

    # Readd the same realizations
    reekensemble.add_realizations([testdir +
                                   '/data/testensemble-reek001/' +
                                   'realization-1/iter-0',
                                   testdir +
                                   '/data/testensemble-reek001/' +
                                   'realization-3/iter-0'])
    assert len(reekensemble) == 5
    assert len(reekensemble.files) == 19
    # File discovery must be repeated for the newly added realizations
    reekensemble.find_files('share/results/volumes/*csv',
                            metadata={'GRID': 'simgrid'})
    assert len(reekensemble.files) == 20
    # Test addition of already added realization:
    reekensemble.add_realizations(testdir +
                                  '/data/testensemble-reek001/' +
                                  'realization-1/iter-0')
    assert len(reekensemble) == 5
    assert len(reekensemble.files) == 19  # discovered files are lost!


def test_ensemble_ecl():
    """Eclipse specific functionality"""

    if '__file__' in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath('.')

    reekensemble = ensemble.ScratchEnsemble('reektest',
                                            testdir +
                                            '/data/testensemble-reek001/' +
                                            'realization-*/iter-0')

    # Eclipse summary keys:
    assert len(reekensemble.get_smrykeys('FOPT')) == 1
    assert len(reekensemble.get_smrykeys('F*')) == 49
    assert len(reekensemble.get_smrykeys(['F*', 'W*'])) == 49 + 280
    assert len(reekensemble.get_smrykeys('BOGUS')) == 0

    # reading ensemble dataframe

    # When asking the ensemble for FOPR, we also get REAL as a column
    # in return:
    assert len(reekensemble.get_smry(column_keys=['FOPR']).columns) == 3
    assert len(reekensemble.get_smry(column_keys=['FOP*']).columns) == 11
    assert len(reekensemble.get_smry(column_keys=['FGPR',
                                                  'FOP*']).columns) == 12
    assert len(reekensemble.get_smry(column_keys=['FGPR',
                                                  'FOP*']).index) == 1700

    # Date list handling:
    assert len(reekensemble.get_smry_dates(freq='report')) == 641
    assert len(reekensemble.get_smry_dates(freq='yearly')) == 4
    assert len(reekensemble.get_smry_dates(freq='monthly')) == 37
    assert len(reekensemble.get_smry_dates(freq='daily')) == 1098
    assert len(reekensemble.get_smry_dates(freq='last')) == 1

    # Time interpolated dataframes with summary data:
    yearly = reekensemble.get_smry_dates(freq='yearly')
    assert len(reekensemble.get_smry(column_keys=['FOPT'],
                                     time_index=yearly)) == 20
    # Check that we can shortcut get_smry_dates:
    assert len(reekensemble.get_smry(column_keys=['FOPT'],
                                     time_index='yearly')) == 20

    assert len(reekensemble.get_smry(column_keys=['FOPR'],
                                     time_index='last')) == 5
    # eclipse well names list
    assert len(reekensemble.get_wellnames('OP*')) == 5

    # eclipse well groups list
    assert len(reekensemble.get_groupnames()) == 3
