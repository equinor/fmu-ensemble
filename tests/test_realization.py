# -*- coding: utf-8 -*-
"""Testing fmu-ensemble."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import datetime
import pytest
import shutil
import pandas as pd
import ert.ecl

from fmu import config
from fmu import ensemble

fmux = config.etc.Interaction()
logger = fmux.basiclogger(__name__)

if not fmux.testsetup():
    raise SystemExit()


def test_single_realization():

    testdir = os.path.dirname(os.path.abspath(__file__))
    realdir = os.path.join(testdir, 'data/testensemble-reek001',
                           'realization-0/iter-0')
    real = ensemble.ScratchRealization(realdir)

    assert len(real.files) == 3
    assert 'parameters.txt' in real.data
    assert isinstance(real.parameters['RMS_SEED'], int)
    assert real.parameters['RMS_SEED'] == 422851785
    assert isinstance(real.parameters['MULTFLT_F1'], float)
    assert isinstance(real.from_txt('parameters.txt',
                                    convert_numeric=False,
                                    force_reread=True)['RMS_SEED'],
                      str)
    # We have rerun from_txt on parameters, but file count
    # should not increase:
    assert len(real.files) == 3

    with pytest.raises(IOError):
        real.from_txt('nonexistingfile.txt')

    # Load more data from text files:
    assert 'NPV' in real.from_txt('outputs.txt')
    assert len(real.files) == 4
    assert 'outputs.txt' in real.data
    assert 'top_structure' in real.data['outputs.txt']

    # STATUS file
    status = real.get_df('STATUS')
    assert isinstance(status, pd.DataFrame)
    assert len(status)
    assert 'ECLIPSE' in status.loc[49, 'FORWARD_MODEL']
    assert int(status.loc[49, 'DURATION']) == 141

    # CSV file loading
    vol_df = real.from_csv('share/results/volumes/simulator_volume_fipnum.csv')
    assert len(real.files) == 5
    assert isinstance(vol_df, pd.DataFrame)
    assert vol_df['STOIIP_TOTAL'].sum() > 0

    # Test later retrieval of cached data:
    vol_df2 = real.get_df('share/results/volumes/simulator_volume_fipnum.csv')
    assert vol_df2['STOIIP_TOTAL'].sum() > 0

    # Test internal storage:
    localpath = 'share/results/volumes/simulator_volume_fipnum.csv'
    assert localpath in real.data
    assert isinstance(real.get_df(localpath), pd.DataFrame)
    assert isinstance(real.get_df('parameters.txt'), dict)
    assert isinstance(real.get_df('outputs.txt'), dict)

    # Test shortcuts to the internal datastore
    assert isinstance(real.get_df('simulator_volume_fipnum.csv'), pd.DataFrame)
    # test without extension:
    assert isinstance(real.get_df('share/results/volumes/' +
                                  'simulator_volume_fipnum'),
                      pd.DataFrame)
    assert isinstance(real.get_df('parameters'), dict)
    # test basename and no extension:
    assert isinstance(real.get_df('simulator_volume_fipnum'), pd.DataFrame)

    with pytest.raises(ValueError):
        real.get_df('notexisting.csv')

    # At realization level, wrong filenames should throw exceptions,
    # at ensemble level it is fine.
    with pytest.raises(IOError):
        real.from_csv('bogus.csv')


def test_singlereal_ecl():
    """Test Eclipse specific functionality for realizations"""

    testdir = os.path.dirname(os.path.abspath(__file__))
    realdir = os.path.join(testdir, 'data/testensemble-reek001',
                           'realization-0/iter-0')
    real = ensemble.ScratchRealization(realdir)

    # Eclipse summary files:
    assert isinstance(real.get_eclsum(), ert.ecl.EclSum)
    real.from_smry().to_csv('real0smry.csv', index=False)
    assert real.from_smry().shape == (378, 471)
    # 378 dates, 470 columns + DATE column

    assert real.from_smry(column_keys='FOP*')['FOPT'].max() > 6000000
    assert real.get_smryvalues('FOPT')['FOPT'].max() > 6000000

    # Test date functionality
    assert isinstance(real.get_smry_dates(), list)
    assert isinstance(real.get_smry_dates(freq='last'), list)
    assert isinstance(real.get_smry_dates(freq='last')[0], datetime.date)
    assert len(real.get_smry_dates()) == \
        len(real.get_smry_dates(freq='monthly'))
    monthly = real.get_smry_dates(freq='monthly')
    assert monthly[-1] > monthly[0]  # end date is later than start
    assert len(real.get_smry_dates(freq='yearly')) == 4
    assert len(monthly) == 37
    assert len(real.get_smry_dates(freq='daily')) == 1098

    # Test caching/internalization of summary files

    # This should be false, since only the full localpath is in keys():
    assert 'unsmry-raw.csv' not in real.keys()
    assert 'share/results/tables/unsmry-raw.csv' in real.keys()
    assert 'FOPT' in real['unsmry-raw']
    with pytest.raises(ValueError):
        # This does not exist before we have asked for it
        'FOPT' in real['unsmry-yearly']

def test_filesystem_changes():
    """Test loading of sparse realization (random data missing)

    Performed by filesystem manipulations from the original realizations.
    Clean up from previous runs are attempted, and also done when it finishes.
    (after a failed test run, filesystem is tainted)
    """

    if '__file__' in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath('.')

    datadir = testdir + '/data'
    tmpensname = ".deleteme_ens"
    # Clean up earlier failed runs:
    if os.path.exists(datadir + '/' + tmpensname):
        shutil.rmtree(datadir + '/' + tmpensname)
    os.mkdir(datadir + '/' + tmpensname)
    #os.mkdir(datadir + '/' + tmpensname + '/realization-0')
    shutil.copytree(datadir + '/testensemble-reek001/realization-0',
                    datadir + '/' + tmpensname + '/realization-0')

    realdir = datadir + '/' + tmpensname + '/realization-0/iter-0'

    # Load untainted realization, nothing bad should happen
    real = ensemble.ScratchRealization(realdir)

    # Remove SMSPEC file and reload:
    shutil.move(realdir + '/eclipse/model/2_R001_REEK-0.SMSPEC',
                realdir + '/eclipse/model/2_R001_REEK-0.SMSPEC-FOOO')
    real = ensemble.ScratchRealization(realdir)  # this should go fine
    # This should just return None. Logging info is okay.
    assert real.get_eclsum() == None
    # This should return None
    assert real.get_smry_dates() == None
    # This should return empty dataframe:
    assert isinstance(real.from_smry(), pd.DataFrame)
    assert len(real.from_smry()) == 0

    # Also move away UNSMRY and redo:
    shutil.move(realdir + '/eclipse/model/2_R001_REEK-0.UNSMRY',
                realdir + '/eclipse/model/2_R001_REEK-0.UNSMRY-FOOO')
    real = ensemble.ScratchRealization(realdir)  # this should go fine
    # This should just return None
    assert real.get_eclsum() == None
    # This should return None
    assert real.get_smry_dates() == None
    # This should return empty dataframe:
    assert isinstance(real.from_smry(), pd.DataFrame)
    assert len(real.from_smry()) == 0

    # Reinstate summary data:
    shutil.move(realdir + '/eclipse/model/2_R001_REEK-0.UNSMRY-FOOO',
                realdir + '/eclipse/model/2_R001_REEK-0.UNSMRY')
    shutil.move(realdir + '/eclipse/model/2_R001_REEK-0.SMSPEC-FOOO',
                realdir + '/eclipse/model/2_R001_REEK-0.SMSPEC')

    # Remove jobs.json, this file should not be critical
    # but the status dataframe should have less information
    statuscolumnswithjson = len(real.get_df('STATUS').columns)
    os.remove(realdir + '/jobs.json')
    real = ensemble.ScratchRealization(realdir)  # this should go fine

    statuscolumnswithoutjson = len(real.get_df('STATUS').columns)
    print(statuscolumnswithoutjson)
    print(statuscolumnswithjson)
    assert statuscolumnswithoutjson > 0
    # Check that some STATUS info is missing.
    assert statuscolumnswithoutjson < statuscolumnswithjson

    # Remove parameters.txt
    shutil.move(realdir + '/parameters.txt', realdir + '/parameters.text')
    with pytest.raises(IOError):
        real = ensemble.ScratchRealization(realdir)
        # Discuss whether we should relax this requirement!!

    # Move it back so the realization is valid again
    shutil.move(realdir + '/parameters.text', realdir + '/parameters.txt')

    # Remove STATUS altogether:
    shutil.move(realdir + '/STATUS', realdir + '/MOVEDSTATUS')
    with pytest.raises(IOError):
        real = ensemble.ScratchRealization(realdir)
        # This is also currently a hard requirement.

    # Try with an empty STATUS file:
    fhandle = open(realdir + '/STATUS', 'w')
    fhandle.close()
    real = ensemble.ScratchRealization(realdir)
    assert len(real.get_df('STATUS')) == 0
    # This demonstrates we can fool the Realization object, and
    # should perhaps leads to relaxation of the requirement..

    # Check that we can move the Eclipse files to another place
    # in the realization dir and still load summary data:
    shutil.move(realdir + '/eclipse',
                realdir + '/eclipsedir')
    real = ensemble.ScratchRealization(realdir)

    # from_smry() is now the same as no UNSMRY file found,
    # an empty dataframe (and there would be some logging)
    assert len(real.from_smry()) == 0

    # Now discover the UNSMRY file explicitly, then from_smry()
    # should work.
    real.find_files('eclipsedir/model/*.UNSMRY')
    # Non-empty dataframe:
    assert len(real.from_smry()) > 0

    # Clean up when finished
    shutil.rmtree(datadir + '/' + tmpensname)
