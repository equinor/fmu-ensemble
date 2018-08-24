# -*- coding: utf-8 -*-
"""Module for parsing an ensemble from FMU. This class represents an
ensemble, which is nothing but a collection of realizations.

The typical task of this class is book-keeping of each realization,
and abilities to aggregate any information that each realization can
provide.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from fmu.config import etc


xfmu = etc.Interaction()
logger = xfmu.functionlogger(__name__)


class Delta(object):

    def __init__(self, ref, ior):
        """
        The Delta object is used to substract or add Ensembles.

        Args:
            ref (obj): Name identifier for the ensemble ref case.
            ior (obj): Name identifier for the ensemble ior case.
        """

        self.ref = ref
        self.ior = ior
        ref_ok = set(ref.get_ok().query('OK == True')['REAL'].tolist())
        ior_ok = set(ior.get_ok().query('OK == True')['REAL'].tolist())
        self.combined = list(ref_ok & ior_ok)

    def get_diff_smmry(self, column_keys=None):

        time_index = self.ref.get_smry_dates(freq='daily')
        ref = self.ref.get_smry(time_index=time_index, column_keys=column_keys,
                                stacked=True)
        ref = ref[ref['REAL'].isin(self.combined)]

        ior = self.ior.get_smry(time_index=time_index, column_keys=column_keys,
                                stacked=True)
        ior = ior[ior['REAL'].isin(self.combined)]

        diff = ior - ref
        diff['DATE'] = ref['DATE']
        diff['REAL'] = ref['REAL']

        return diff
