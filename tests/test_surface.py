# -*- coding: utf-8 -*-
"""Testing fmu-ensemble."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import shutil

import numpy
import pandas as pd

import pytest

from fmu.ensemble import etc
from fmu.ensemble import EnsembleSurface


def test_something():
    print('OK')