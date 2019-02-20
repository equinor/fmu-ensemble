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
import ecl.summary

from fmu import config
from fmu import ensemble

fmux = config.etc.Interaction()
logger = fmux.basiclogger(__name__)

if not fmux.testsetup():
    raise SystemExit()


def test_single_realization():

    if '__file__' in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath('.')

    realdir = os.path.join(testdir, 'data/testensemble-reek001',
                           'realization-0/iter-0')
    real = ensemble.ScratchRealization(realdir)

    assert len(real.files) == 4
    assert 'parameters.txt' in real.data
    assert isinstance(real.parameters['RMS_SEED'], int)
    assert real.parameters['RMS_SEED'] == 422851785
    assert isinstance(real.parameters['MULTFLT_F1'], float)
    assert isinstance(real.load_txt('parameters.txt',
                                    convert_numeric=False,
                                    force_reread=True)['RMS_SEED'],
                      str)
    # We have rerun load_txt on parameters, but file count
    # should not increase:
    assert len(real.files) == 4

    with pytest.raises(IOError):
        real.load_txt('nonexistingfile.txt')

    # Load more data from text files:
    assert 'NPV' in real.load_txt('outputs.txt')
    assert len(real.files) == 5
    assert 'outputs.txt' in real.data
    assert 'top_structure' in real.data['outputs.txt']

    # STATUS file
    status = real.get_df('STATUS')
    assert isinstance(status, pd.DataFrame)
    assert len(status)
    assert 'ECLIPSE' in status.loc[49, 'FORWARD_MODEL']
    assert int(status.loc[49, 'DURATION']) == 141

    # CSV file loading
    vol_df = real.load_csv('share/results/volumes/simulator_volume_fipnum.csv')
    assert len(real.files) == 6
    assert isinstance(vol_df, pd.DataFrame)
    assert vol_df['STOIIP_TOTAL'].sum() > 0

    # Test later retrieval of cached data:
    vol_df2 = real.get_df('share/results/volumes/simulator_volume_fipnum.csv')
    assert vol_df2['STOIIP_TOTAL'].sum() > 0

    # Test scalar import
    assert 'OK' in real.keys()  # Imported in __init__
    assert real['OK'] == "All jobs complete 22:47:54 "  # Mind the last space
    assert isinstance(real['OK'], str)

    # Check that we can "reimport" the OK file
    real.load_scalar('OK', force_reread=True)
    assert 'OK' in real.keys()  # Imported in __init__
    assert real['OK'] == "All jobs complete 22:47:54 "  # Mind the last space
    assert isinstance(real['OK'], str)
    assert len(real.files[real.files.LOCALPATH == 'OK']) == 1

    real.load_scalar('npv.txt')
    assert real.get_df('npv.txt') == 3444
    assert real['npv.txt'] == 3444
    assert isinstance(real.data['npv.txt'], int)
    assert 'npv.txt' in real.files.LOCALPATH.values
    assert real.files[real.files.LOCALPATH == 'npv.txt']['FILETYPE'].values[0]\
        == 'txt'

    real.load_scalar('emptyscalarfile')
    # Activate this test when filter() is merged:
    # assert real.contains('emptyfile')
    assert 'emptyscalarfile' in real.data
    assert isinstance(real['emptyscalarfile'], str)
    assert 'emptyscalarfile' in real.files.LOCALPATH.values

    with pytest.raises(IOError):
        real.load_scalar('notexisting.txt')

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

    # Test __delitem__()
    keycount = len(real.keys())
    del real['parameters.txt']
    assert len(real.keys()) == keycount - 1

    # At realization level, wrong filenames should throw exceptions,
    # at ensemble level it is fine.
    with pytest.raises(IOError):
        real.load_csv('bogus.csv')


def test_datenormalization():
    from fmu.ensemble.realization import normalize_dates
    from datetime import date

    start = date(1997, 11, 5)
    end = date(2020, 3, 2)

    print(normalize_dates(start, end, 'monthly'))

    assert normalize_dates(start, end, 'monthly') == \
        (date(1997, 11, 1), date(2020, 4, 1))
    assert normalize_dates(start, end, 'yearly') == \
        (date(1997, 1, 1), date(2021, 1, 1))

    # Check it does not touch already aligned dates
    assert normalize_dates(date(1997, 11, 1),
                           date(2020, 4, 1), 'monthly') == \
        (date(1997, 11, 1), date(2020, 4, 1))
    assert normalize_dates(date(1997, 1, 1),
                           date(2021, 1, 1), 'yearly') == \
        (date(1997, 1, 1), date(2021, 1, 1))


def test_singlereal_ecl(tmp='TMP'):
    """Test Eclipse specific functionality for realizations"""

    testdir = os.path.dirname(os.path.abspath(__file__))
    realdir = os.path.join(testdir, 'data/testensemble-reek001',
                           'realization-0/iter-0')
    real = ensemble.ScratchRealization(realdir)

    # Eclipse summary files:
    assert isinstance(real.get_eclsum(), ecl.summary.EclSum)
    if not os.path.exists(tmp):
        os.mkdir(tmp)
    real.load_smry().to_csv(os.path.join(tmp, 'real0smry.csv'), index=False)
    assert real.load_smry().shape == (378, 474)
    # 378 dates, 470 columns + DATE column

    assert real.load_smry(column_keys=['FOP*'])['FOPT'].max() > 6000000
    assert real.get_smryvalues('FOPT')['FOPT'].max() > 6000000

    # get_smry() should be analogue to load_smry(), but it should
    # not modify the internalized dataframes!
    internalized_df = real['unsmry--raw']
    df = real.get_smry(column_keys=['G*'])
    assert 'GGIR:OP' in df.columns
    assert 'GGIR:OP' not in internalized_df.columns
    # Test that the internalized was not touched:
    assert 'GGIR:OP' not in real['unsmry--raw'].columns

    assert 'FOPT' in real.get_smry(column_keys=['F*'], time_index='monthly')
    assert 'FOPT' in real.get_smry(column_keys='F*', time_index='yearly')
    assert 'FOPT' in real.get_smry(column_keys='FOPT', time_index='daily')
    assert 'FOPT' in real.get_smry(column_keys='FOPT', time_index='raw')

    # Test date functionality
    assert isinstance(real.get_smry_dates(), list)
    assert isinstance(real.get_smry_dates(freq='last'), list)
    assert isinstance(real.get_smry_dates(freq='last')[0], datetime.date)
    assert len(real.get_smry_dates()) == \
        len(real.get_smry_dates(freq='monthly'))
    monthly = real.get_smry_dates(freq='monthly')
    assert monthly[-1] > monthly[0]  # end date is later than start
    assert len(real.get_smry_dates(freq='yearly')) == 5
    assert len(monthly) == 38
    assert len(real.get_smry_dates(freq='daily')) == 1098

    # Test caching/internalization of summary files

    # This should be false, since only the full localpath is in keys():
    assert 'unsmry--raw.csv' not in real.keys()
    assert 'share/results/tables/unsmry--raw.csv' in real.keys()
    assert 'FOPT' in real['unsmry--raw']
    with pytest.raises(ValueError):
        # This does not exist before we have asked for it
        'FOPT' in real['unsmry--yearly']


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
        shutil.rmtree(datadir + '/' + tmpensname, ignore_errors=True)
    os.mkdir(datadir + '/' + tmpensname)
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
    assert real.get_eclsum() is None
    # This should return None
    assert real.get_smry_dates() is None
    # This should return empty dataframe:
    assert isinstance(real.load_smry(), pd.DataFrame)
    assert len(real.load_smry()) == 0

    assert isinstance(real.get_smry(), pd.DataFrame)
    assert len(real.get_smry()) == 0

    # Also move away UNSMRY and redo:
    shutil.move(realdir + '/eclipse/model/2_R001_REEK-0.UNSMRY',
                realdir + '/eclipse/model/2_R001_REEK-0.UNSMRY-FOOO')
    real = ensemble.ScratchRealization(realdir)  # this should go fine
    # This should just return None
    assert real.get_eclsum() is None
    # This should return None
    assert real.get_smry_dates() is None
    # This should return empty dataframe:
    assert isinstance(real.load_smry(), pd.DataFrame)
    assert len(real.load_smry()) == 0

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
    assert statuscolumnswithoutjson > 0
    # Check that some STATUS info is missing.
    assert statuscolumnswithoutjson < statuscolumnswithjson

    # Remove parameters.txt
    shutil.move(realdir + '/parameters.txt', realdir + '/parameters.text')
    real = ensemble.ScratchRealization(realdir)
    # Should not fail

    # Move it back so the realization is valid again
    shutil.move(realdir + '/parameters.text', realdir + '/parameters.txt')

    # Remove STATUS altogether:
    shutil.move(realdir + '/STATUS', realdir + '/MOVEDSTATUS')
    real = ensemble.ScratchRealization(realdir)
    # Should not fail

    # Try with an empty STATUS file:
    fhandle = open(realdir + '/STATUS', 'w')
    fhandle.close()
    real = ensemble.ScratchRealization(realdir)
    assert len(real.get_df('STATUS')) == 0
    # This demonstrates we can fool the Realization object, and
    # should perhaps leads to relaxation of the requirement..

    # Try with a STATUS file with error message on first job
    # the situation where there is one successful job.
    fhandle = open(realdir + '/STATUS', 'w')
    fhandle.write("""Current host                    : st-rst16-02-03/x86_64  file-server:10.14.10.238 
LSF JOBID: not running LSF
COPY_FILE                       : 20:58:57 .... 20:59:00   EXIT: 1/Executable: /project/res/komodo/2018.02/root/etc/ERT/Config/jobs/util/script/copy_file.py failed with exit code: 1
""")  # noqa
    fhandle.close()
    real = ensemble.ScratchRealization(realdir)
    # When issue 37 is resolved, update this to 1 and check the
    # error message is picked up.
    assert len(real.get_df('STATUS')) == 1
    fhandle = open(realdir + '/STATUS', 'w')
    fhandle.write("""Current host                    : st-rst16-02-03/x86_64  file-server:10.14.10.238 
LSF JOBID: not running LSF
COPY_FILE                       : 20:58:55 .... 20:58:57
COPY_FILE                       : 20:58:57 .... 20:59:00   EXIT: 1/Executable: /project/res/komodo/2018.02/root/etc/ERT/Config/jobs/util/script/copy_file.py failed with exit code: 1
""")  # noqa
    fhandle.close()
    real = ensemble.ScratchRealization(realdir)
    assert len(real.get_df('STATUS')) == 2
    # Check that we have the error string picked up:
    assert real.get_df('STATUS')['errorstring'].dropna().values[0] == \
        "EXIT: 1/Executable: /project/res/komodo/2018.02/root/etc/ERT/Config/jobs/util/script/copy_file.py failed with exit code: 1"  # noqa

    # Check that we can move the Eclipse files to another place
    # in the realization dir and still load summary data:
    shutil.move(realdir + '/eclipse',
                realdir + '/eclipsedir')
    real = ensemble.ScratchRealization(realdir)

    # load_smry() is now the same as no UNSMRY file found,
    # an empty dataframe (and there would be some logging)
    assert len(real.load_smry()) == 0

    # Now discover the UNSMRY file explicitly, then load_smry()
    # should work.
    unsmry_file = real.find_files('eclipsedir/model/*.UNSMRY')
    # Non-empty dataframe:
    assert len(real.load_smry()) > 0
    assert len(unsmry_file) == 1
    assert isinstance(unsmry_file, pd.DataFrame)

    # Test having values with spaces in parameters.txt
    # Current "wanted" behaviour is to ignore the anything after
    #  <key><whitespace><value><whitespace><ignoreremainder>
    # which is similar to ERT CSV_EXPORT1
    param_file = open(realdir + '/parameters.txt', 'a')
    param_file.write('FOOBAR 1 2 3 4 5 6')
    param_file.close()

    real = ensemble.ScratchRealization(realdir)
    assert real.parameters['FOOBAR'] == 1

    # Clean up when finished. This often fails on network drives..
    shutil.rmtree(datadir + '/' + tmpensname, ignore_errors=True)


def test_apply(tmp='TMP'):
    """
    Test the callback functionality
    """
    testdir = os.path.dirname(os.path.abspath(__file__))
    realdir = os.path.join(testdir, 'data/testensemble-reek001',
                           'realization-0/iter-0')
    real = ensemble.ScratchRealization(realdir)

    def ex_func1():
        return pd.DataFrame(index=['1', '2'], columns=['foo', 'bar'],
                            data=[[1, 2], [3, 4]])
    result = real.apply(ex_func1)
    assert isinstance(result, pd.DataFrame)

    # Apply and store the result:
    real.apply(ex_func1, localpath='df-1234')
    internalized_result = real.get_df('df-1234')
    assert isinstance(internalized_result, pd.DataFrame)
    assert (result == internalized_result).all().all()

    # Check that the submitted function can utilize data from **kwargs
    def ex_func2(kwargs):
        arg = kwargs['foo']
        return pd.DataFrame(index=['1', '2'], columns=['foo', 'bar'],
                            data=[[arg, arg], [arg, arg]])

    result2 = real.apply(ex_func2, foo='bar')
    assert result2.iloc[0, 0] == 'bar'

    # We require applied function to return only DataFrames.
    def scalar_func():
        return 1
    with pytest.raises(ValueError):
        real.apply(scalar_func)

    # The applied function should have access to the realization object:
    def real_func(kwargs):
        return pd.DataFrame(index=[0], columns=['path'],
                            data=kwargs['realization'].runpath())
    origpath = real.apply(real_func)
    assert os.path.exists(origpath.iloc[0, 0])

    # Do not allow supplying the realization object to apply:
    with pytest.raises(ValueError):
        real.apply(real_func, realization='foo')


def test_drop(tmp='TMP'):

    testdir = os.path.dirname(os.path.abspath(__file__))
    realdir = os.path.join(testdir, 'data/testensemble-reek001',
                           'realization-0/iter-0')
    real = ensemble.ScratchRealization(realdir)

    parametercount = len(real.parameters)
    real.drop('parameters', key='RMS_SEED')
    assert len(real.parameters) == parametercount - 1

    real.drop('parameters', keys=['L_1GO', 'E_1GO'])
    assert len(real.parameters) == parametercount - 3

    real.drop('parameters', key='notexistingkey')
    # This will go unnoticed
    assert len(real.parameters) == parametercount - 3

    real.load_smry(column_keys='FOPT', time_index='monthly')
    if not os.path.exists(tmp):
        os.mkdir(tmp)
    real.get_df('unsmry--monthly').to_csv(os.path.join(tmp, 'foo.csv'),
                                         index=False)
    datecount = len(real.get_df('unsmry--monthly'))
    real.drop('unsmry--monthly', rowcontains='2000-01-01')
    assert len(real.get_df('unsmry--monthly')) == datecount - 1

    real.drop('parameters')
    assert 'parameters.txt' not in real.keys()
