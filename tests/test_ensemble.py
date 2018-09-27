# -*- coding: utf-8 -*-
"""Testing fmu-ensemble."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import numpy
import pandas as pd

import pytest

from fmu import config
from fmu.ensemble import ScratchEnsemble, ScratchRealization

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

    reekensemble = ScratchEnsemble('reektest',
                                   testdir +
                                   '/data/testensemble-reek001/' +
                                   'realization-*/iter-0')
    assert isinstance(reekensemble, ScratchEnsemble)
    assert reekensemble.name == 'reektest'
    assert len(reekensemble) == 5

    assert isinstance(reekensemble[0], ScratchRealization)

    assert len(reekensemble.files[
        reekensemble.files.LOCALPATH == 'jobs.json']) == 5
    assert len(reekensemble.files[
        reekensemble.files.LOCALPATH == 'parameters.txt']) == 5
    assert len(reekensemble.files[
        reekensemble.files.LOCALPATH == 'STATUS']) == 5

    statusdf = reekensemble.get_df('STATUS')
    assert len(statusdf) == 250  # 5 realizations, 50 jobs in each
    assert 'REAL' in statusdf.columns
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
    paramsdf = reekensemble.get_df('parameters.txt')
    assert len(paramsdf) == 5
    assert len(paramsdf.columns) == 26  # 25 parameters, + REAL column
    paramsdf.to_csv('params.csv', index=False)

    # Check that the ensemble object has not tainted the realization dataframe:
    assert 'REAL' not in reekensemble._realizations[0].get_df('parameters.txt')

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
    csvpath = 'share/results/volumes/simulator_volume_fipnum.csv'
    vol_df = reekensemble.from_csv(csvpath)

    # Check that we have not tainted the realization dataframes:
    assert 'REAL' not in reekensemble._realizations[0].get_df(csvpath)

    assert 'REAL' in vol_df
    assert len(vol_df['REAL'].unique()) == 3  # missing in 2 reals
    vol_df.to_csv('simulatorvolumes.csv', index=False)

    # Test retrival of cached data
    vol_df2 = reekensemble.get_df(csvpath)

    assert 'REAL' in vol_df2
    assert len(vol_df2['REAL'].unique()) == 3  # missing in 2 reals

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
    assert len(reekensemble.files) == 24

    # File discovery must be repeated for the newly added realizations
    reekensemble.find_files('share/results/volumes/' +
                            'simulator_volume_fipnum.csv',
                            metadata={'GRID': 'simgrid'})
    assert len(reekensemble.files) == 25
    # Test addition of already added realization:
    reekensemble.add_realizations(testdir +
                                  '/data/testensemble-reek001/' +
                                  'realization-1/iter-0')
    assert len(reekensemble) == 5
    assert len(reekensemble.files) == 24  # discovered files are lost!

    keycount = len(reekensemble.keys())
    reekensemble.remove_data('parameters.txt')
    assert len(reekensemble.keys()) == keycount - 1


def test_ensemble_ecl():
    """Eclipse specific functionality"""

    if '__file__' in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath('.')

    reekensemble = ScratchEnsemble('reektest',
                                   testdir +
                                   '/data/testensemble-reek001/' +
                                   'realization-*/iter-0')

    # Eclipse summary keys:
    assert len(reekensemble.get_smrykeys('FOPT')) == 1
    assert len(reekensemble.get_smrykeys('F*')) == 49
    assert len(reekensemble.get_smrykeys(['F*', 'W*'])) == 49 + 280
    assert len(reekensemble.get_smrykeys('BOGUS')) == 0

    # reading ensemble dataframe

    monthly = reekensemble.from_smry(column_keys=['F*'], time_index='monthly')
    assert monthly.columns[0] == 'REAL'  # Enforce order of columns.
    assert monthly.columns[1] == 'DATE'
    assert len(monthly) == 190
    # Check that the result was cached in memory, not necessarily on disk..
    assert isinstance(reekensemble.get_df('unsmry-monthly.csv'), pd.DataFrame)

    assert len(reekensemble.keys()) == 3

    # When asking the ensemble for FOPR, we also get REAL as a column
    # in return. Note that the internal stored version will be
    # overwritten by each from_smry()
    assert len(reekensemble.from_smry(column_keys=['FOPR']).columns) == 3
    assert len(reekensemble.from_smry(column_keys=['FOP*']).columns) == 11
    assert len(reekensemble.from_smry(column_keys=['FGPR',
                                                   'FOP*']).columns) == 12

    # Check that there is now a cached version with raw dates:
    assert isinstance(reekensemble.get_df('unsmry-raw.csv'), pd.DataFrame)
    # The columns are not similar, this is allowed!

    # If you get 3205 here, it means that you are using the union of
    # raw dates from all realizations, which is not correct
    assert len(reekensemble.from_smry(column_keys=['FGPR',
                                                   'FOP*']).index) == 1700

    # Date list handling:
    assert len(reekensemble.get_smry_dates(freq='report')) == 641
    assert len(reekensemble.get_smry_dates(freq='raw')) == 641
    assert len(reekensemble.get_smry_dates(freq='yearly')) == 4
    assert len(reekensemble.get_smry_dates(freq='monthly')) == 37
    assert len(reekensemble.get_smry_dates(freq='daily')) == 1098
    assert len(reekensemble.get_smry_dates(freq='last')) == 1

    # Time interpolated dataframes with summary data:
    yearly = reekensemble.get_smry_dates(freq='yearly')
    assert len(reekensemble.from_smry(column_keys=['FOPT'],
                                      time_index=yearly)) == 20
    # NB: This is cached in unsmry-custom.csv, not unsmry-yearly!
    # This usage is discouraged. Use 'yearly' in such cases.

    # Check that we can shortcut get_smry_dates:
    assert len(reekensemble.from_smry(column_keys=['FOPT'],
                                      time_index='yearly')) == 25

    assert len(reekensemble.from_smry(column_keys=['FOPR'],
                                      time_index='last')) == 5
    assert isinstance(reekensemble.get_df('unsmry-last.csv'), pd.DataFrame)

    # eclipse well names list
    assert len(reekensemble.get_wellnames('OP*')) == 5

    # eclipse well groups list
    assert len(reekensemble.get_groupnames()) == 3

    # delta between two ensembles
    diff = reekensemble - reekensemble
    assert len(diff.get_smry(column_keys=['FOPR', 'FGPR',
                                          'FWCT']).columns) == 5

    # eclipse summary vector statistics for a given ensemble
    df_stats = reekensemble.get_smry_stats(column_keys=['FOPR', 'FGPR'],
                                           time_index='monthly')
    assert isinstance(df_stats, dict)
    assert len(df_stats.keys()) == 2
    assert isinstance(df_stats['FOPR'], pd.DataFrame)
    assert len(df_stats['FOPR'].index) == 37

    # check if wild cards also work for get_smry_stats
    df_stats = reekensemble.get_smry_stats(column_keys=['FOP*', 'FGP*'],
                                           time_index='monthly')
    assert len(df_stats.keys()) == len(reekensemble.get_smrykeys(['FOP*',
                                                                 'FGP*']))

    # Check webviz requirements for dataframe
    assert 'min' in df_stats['FOPR'].columns
    assert 'max' in df_stats['FOPR'].columns
    assert 'name' in df_stats['FOPR'].columns
    assert df_stats['FOPR']['name'].unique() == 'FOPR'
    assert 'index' in df_stats['FOPR'].columns  # This is DATE (!)
    assert 'mean' in df_stats['FOPR'].columns
    assert 'p10' in df_stats['FOPR'].columns
    assert 'p90' in df_stats['FOPR'].columns
    assert df_stats['FOPR']['min'].iloc[-1] < \
        df_stats['FOPR']['max'].iloc[-1]


def test_observation_import():
    if '__file__' in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath('.')

    reekensemble = ScratchEnsemble('reektest',
                                   testdir +
                                   '/data/testensemble-reek001/' +
                                   'realization-*/iter-0')

    obs = reekensemble.from_obs_yaml(testdir +
                                     '/data/testensemble-reek001/' +
                                     '/share/observations/observations.yaml')

    assert len(obs.keys()) == 2
    df_mismatch = reekensemble.ensemble_mismatch()

    assert len(df_mismatch.columns) == 7


def test_filedescriptors():
    """Test how filedescriptors are used.

    The lazy_load option to EclSum affects this, if it is set to True
    file descriptors are not closed (and True is the default).
    In order to be able to open thousands of smry files, we need
    to always close the file descriptors when possible, and therefore
    lazy_load should be set to False in realization.py"""

    if '__file__' in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath('.')

    fd_dir = '/proc/' + str(os.getpid()) + '/fd'
    if not os.path.exists(fd_dir):
        print("Counting file descriptors on non-Linux not supported")
        return
    fd_count1 = len(os.listdir(fd_dir))
    reekensemble = ScratchEnsemble('reektest',
                                   testdir +
                                   '/data/testensemble-reek001/' +
                                   'realization-*/iter-0')

    fd_count2 = len(os.listdir(fd_dir))
    reekensemble.from_smry()
    fd_count3 = len(os.listdir(fd_dir))
    del reekensemble
    fd_count4 = len(os.listdir(fd_dir))

    # As long as lazy_load = False, we should have 5,5,5,5 from this
    # If lazy_load is True (default), then we get 15, 15, 25, 20
    # print(fd_count1, fd_count2, fd_count3, fd_count4)

    assert fd_count1 == fd_count4


def test_read_eclgrid():

    if not os.path.exists('/scratch/fmu/akia/3_r001_reek/realization-1'):
	pytest.skip("Only works on Stavanger Linux")

    ensemble_path = '/scratch/fmu/akia/3_r001_reek/realization-*1/iter-0'
    reekensemble = ScratchEnsemble('ensemblename',
                                   ensemble_path)
    grid_df = reekensemble.get_eclgrid(['PERMX', 'FLOWATI+', 'FLOWATJ+'],
                                        report=4)

    assert len(grid_df.columns) == 14
    assert len(grid_df['i']) == 35840
