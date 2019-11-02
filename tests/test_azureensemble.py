# -*- coding: utf-8 -*-
"""Testing AzureScratchEnsemble"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from fmu.ensemble import AzureScratchEnsemble, etc

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

