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
    aens = AzureScratchEnsemble(ensemble_path=None, 
                                iter_name=None, 
                                manifest={'Some key' : 'Some value'}, 
                                ignoredirs=None,
                                            )

    # test some individual functions

    subdirs = ['/one', '/one/two', '/one/three', '/two/one', '/three']
    ignoredirs = ['one']

    assert aens.remove_ignored_dirs(subdirs, ignoredirs) == ['/two/one', '/three']
    assert aens.manifest == {'Some key' : 'Some value'}, aens.manifest
    unique1 = aens.make_unique_foldername()
    unique2 = aens.make_unique_foldername()
    assert unique1 != unique2
    assert len(unique1) == len(unique2) == (len('rmrc-')+6)
    assert unique1.startswith('rmrc-')

def test_reek_5real():

    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testrootdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testrootdir = os.path.abspath(".")

    testdir = os.path.join(testrootdir, 'TMP')

    # delete existing data in the testdir if already there
    if os.path.isdir(testdir):
        print('** DELETING ALL CONTENTS IN {} **'.format(testdir))
        shutil.rmtree(testdir)

    try: 
        os.mkdir(os.path.join(testrootdir, 'TMP')) 
    except OSError as error: 
        print(error)


    reekensemble = AzureScratchEnsemble(
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
    assert len(reekensemble.scratchensemble) == 5
    assert len(reekensemble) == 5


    reekensemble.upload(testdir)

    testdumpeddir = reekensemble._tmp_storage_path

    assert testdumpeddir.startswith(testdir)

    # check that some known files are present in the dumped data
    assert os.path.isfile(os.path.join(testdumpeddir, 'data/share/smry.csv'))
    assert os.path.isfile(os.path.join(testdumpeddir, 'data/share/OK.csv'))
    assert os.path.isfile(os.path.join(testdumpeddir, 'data/realization-4/parameters.txt'))

    # index
    indexdf = pd.read_csv(os.path.join(testdumpeddir, 'index/index.csv'))
    assert sorted(list(indexdf['REAL'].fillna(-999).unique())) == [-999,0,1,2,3,4]

    # check that some known files are present in the index
    assert 'data/share/smry.csv' in list(indexdf['LOCALPATH'])

    # check that the manifest looks right
    with open(os.path.join(testdumpeddir, 'manifest/manifest.yml'), 'r') as stream:
            manifest = yaml.load(stream, Loader=yaml.FullLoader)

    assert 'Reek has no' in manifest.keys()
    assert 'rmrc' in manifest.keys()
