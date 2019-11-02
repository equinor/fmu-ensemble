# -*- coding: utf-8 -*-
"""Testing AzureScratchEnsemble"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from fmu.ensemble import AzureScratchEnsemble
from fmu.ensemble import ScratchEnsemble, VirtualEnsemble, ScratchRealization
from fmu.ensemble import etc

import os
import shutil
import pandas as pd
import yaml

fmux = etc.Interaction()
logger = fmux.basiclogger(__name__, level="WARNING")

if not fmux.testsetup():
    raise SystemExit()



def test_empty_ensemble():

    # initialize an empty AzureScratchEnsemble
    aens = AzureScratchEnsemble('MyEns', 
                            ensemble_path=None, 
                            iter_name=None, 
                            manifest={'Some key' : 'Some value'}, 
                            ignoredirs=None,
                                            )

    # test individual functions

    subdirs = ['/one', '/one/two', '/one/three', '/two/one', '/three']
    ignoredirs = ['one']

    assert aens.remove_ignored_dirs(subdirs, ignoredirs) == ['/two/one', '/three']
    assert aens.manifest == {'Some key' : 'Some value'}, aens.manifest


def test_reek_5real():

    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testrootdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testrootdir = os.path.abspath(".")

    try: 
        os.mkdir(os.path.join(testrootdir, 'TMP')) 
    except OSError as error: 
        print(error)

    testdir = os.path.join(testrootdir, 'TMP', 'dumped_ens')

    # delete existing data in the testdir if already there
    if os.path.isdir(testdir):
        shutil.rmtree(testdir)


    reekensemble = AzureScratchEnsemble('reektest', 
            ensemble_path = os.path.join(testrootdir, 'data/testensemble-reek001/'),
            iter_name = 'iter-0',
            manifest = {'Reek has no' : 'manifest'},
            ignoredirs = None,
        )

    # check that what is being passed on to ScratchEnsemble and VirtualEnsemble makes sense
    assert isinstance(reekensemble, AzureScratchEnsemble)
    assert isinstance(reekensemble.scratchensemble, ScratchEnsemble)
    assert isinstance(reekensemble.virtualensemble, VirtualEnsemble)
    assert isinstance(reekensemble.scratchensemble[0], ScratchRealization)
    assert reekensemble.name == 'reektest'
    assert len(reekensemble.scratchensemble) == 5
    assert len(reekensemble) == 5


    reekensemble.upload(testdir)

    # check that some known files are present in the dumped data
    assert os.path.isfile(os.path.join(testdir, 'data/share/smry.csv'))
    assert os.path.isfile(os.path.join(testdir, 'data/share/OK.csv'))
    assert os.path.isfile(os.path.join(testdir, 'data/realization-4/parameters.txt'))

    # index
    indexdf = pd.read_csv(os.path.join(testdir, 'index/index.csv'))
    assert sorted(list(indexdf['REAL'].fillna(-999).unique())) == [-999,0,1,2,3,4]

    # check that some known files are present in the index
    assert 'data/share/smry.csv' in list(indexdf['LOCALPATH'])

    # check that the manifest looks right
    with open(os.path.join(testdir, 'manifest/manifest.yml'), 'r') as stream:
            manifest = yaml.load(stream, Loader=yaml.FullLoader)

    assert 'Reek has no' in manifest.keys()
    assert 'rmrc' in manifest.keys()
    assert manifest.get('rmrc').get('name') == 'reektest'