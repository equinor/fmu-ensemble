# -*- coding: utf-8 -*-
"""Testing fmu-ensemble."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from fmu import config
from fmu import ensemble

fmux = config.etc.Interaction()
logger = fmux.basiclogger(__name__)

if not fmux.testsetup():
    raise SystemExit()


def test_reek001():
    """Test import of a stripped 5 realization ensemble"""
    reekensemble = ensemble.ScratchEnsemble('reektest',
                                            'data/testensemble-reek001/' +
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

    # Parameters.txt
    paramsdf = reekensemble.get_parametersdata(convert_numeric=False)
    assert len(paramsdf) == 5  # 5 realizations
    assert len(paramsdf.columns) == 25  # 24 parameters, + REAL column

    # File discovery:
    reekensemble.find_files('share/results/volumes/*txt')

    # Realization deletion:
    reekensemble.remove_realizations([1, 3])
    assert len(reekensemble) == 3
