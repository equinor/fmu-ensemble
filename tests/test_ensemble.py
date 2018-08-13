# -*- coding: utf-8 -*-
"""Testing fmu-ensemble."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from fmu import config
from fmu.ensemble import Ensemble

fmux = config.etc.Interaction()
logger = fmux.basiclogger(__name__)

if not fmux.testsetup():
    raise SystemExit()


def test_reek001():
        """Test import of a stripped 5 realization ensemble"""
        reekensemble = Ensemble('reektest',
                                'data/testensemble-reek001/' +
                                'realization-*/iter-0')
        assert isinstance(reekensemble, Ensemble)
        assert reekensemble.name == 'reektest'
