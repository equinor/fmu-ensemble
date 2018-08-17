# -*- coding: utf-8 -*-
"""Testing fmu-ensemble."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os

from fmu import config
from fmu import ensemble

fmux = config.etc.Interaction()
logger = fmux.basiclogger(__name__)

if not fmux.testsetup():
    raise SystemExit()


def test_reek001():
    """Test import of a stripped 5 realization ensemble"""

    testdir = os.path.dirname(os.path.abspath(__file__))
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

    statusdf = reekensemble.get_status_data()
    assert len(statusdf) == 250  # 5 realizations, 50 jobs in each
    assert 'DURATION' in statusdf.columns  # calculated
    assert 'argList' in statusdf.columns  # from jobs.json
    assert int(statusdf.loc[249, 'DURATION']) == 150  # sample check

    statusdf.to_csv('status.csv', index=False)

    # Parameters.txt
    paramsdf = reekensemble.get_parameters(convert_numeric=False)
    assert len(paramsdf) == 5  # 5 realizations
    paramsdf = reekensemble.parameters  # also test as property
    assert len(paramsdf.columns) == 25  # 24 parameters, + REAL column
    paramsdf.to_csv('params.csv', index=False)

    # File discovery:
    reekensemble.find_files('share/results/volumes/*csv',
                            metadata={'GRID': 'simgrid'})

    reekensemble.files.to_csv('files.csv', index=False)

    # Eclipse summary files
    assert len(reekensemble.get_smrykeys('FOPT')) == 1
    assert len(reekensemble.get_smrykeys('F*')) == 49
    assert len(reekensemble.get_smrykeys(['F*', 'W*'])) == 49 + 280
    assert len(reekensemble.get_smrykeys('BOGUS')) == 0

    # CSV files
    vol_df = reekensemble.get_csv('share/results/volumes/' +
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
    assert len(reekensemble.files) == 17
    # File discovery must be repeated for the newly added realizations
    reekensemble.find_files('share/results/volumes/*csv',
                            metadata={'GRID': 'simgrid'})
    assert len(reekensemble.files) == 18
    # Test addition of already added realization:
    reekensemble.add_realizations(testdir +
                                  '/data/testensemble-reek001/' +
                                  'realization-1/iter-0')
    assert len(reekensemble) == 5
    assert len(reekensemble.files) == 17  # discovered files are lost!

    # reading ensemble dataframe
    assert len(reekensemble.get_ens_smry(['FOPR']).columns) == 1
    assert len(reekensemble.get_ens_smry('FOP*').columns) == 9
    assert len(reekensemble.get_ens_smry(['FGPR', 'FOP*']).columns) == 10
    assert len(reekensemble.get_ens_smry(['FGPR', 'FOP*']).index) == 3294

    # eclipse well names list
    assert len(reekensemble.get_wellnames('OP*')) == 5

    # eclipse well groups list
    assert len(reekensemble.get_groupnames()) == 3

