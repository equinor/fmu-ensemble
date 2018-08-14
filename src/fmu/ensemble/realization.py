# -*- coding: utf-8 -*-
"""Implementation of realization classes

A realization is a set of results from one subsurface model
realization. A realization can be either defined from 
its output files from the FMU run on the file system,
it can be computed from other realizations, or it can be
an archived realization.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import re
import pandas as pd

from fmu import config

fmux = config.etc.Interaction()
logger = fmux.basiclogger(__name__)

if not fmux.testsetup():
    raise SystemExit()

class ScratchRealization():
    """A representation of results still present on disk

    ScratchRealization's point to the filesystem for their
    contents. 

    A realization must at least contain a STATUS file. 
    Additionally, jobs.json and parameters.txt will be attempted
    loaded by default.

    The realization is defined by the pointers to the filesystem.
    When asked for, this object will return data from the
    filesystem (or from cache if already computed).

    The files dataframe is the central filesystem pointer
    repository for the object. It will at least contain
    the columns
    * FULLPATH absolute path to a file
    * FILETYPE filename extension (after last dot)
    * LOCALPATH relative filename inside realization diretory
    * BASENAME filename only. No path. Includes extension
    
    This dataframe is available as a read-only property from the object

    Args:
        path (str): absolute or relative path to a directory 
            containing a realizations files.
        realidxregexp: a compiled regular expression which
            is used to determine the realization index (integer)
            from the path. First match is the index.
            Default: realization-(\d+)
    """
    def __init__(self, path,
                 realidxregexp=re.compile(r'.*realization-(\d+)')):
        self._origpath = path

        self.files = pd.DataFrame(columns=['FULLPATH', 'FILETYPE',
                                           'LOCALPATH', 'BASENAME'])
         
        abspath = os.path.abspath(path)
        realidxmatch = re.match(realidxregexp, abspath)
        if realidxmatch:
            self.index = int(realidxmatch.group(1))
        else:
            logger.warn('Realization %s not valid, skipping' %
                      abspath)
            raise ValueError 
        
        # Now look for a minimal subset of files
        if os.path.exists(os.path.join(abspath, 'STATUS')):
            self.files = self.files.append({'LOCALPATH': 'STATUS',
                                            'FILETYPE': 'STATUS',
                                            'FULLPATH': os.path.join(abspath,
                                                                    'STATUS')},
                                           ignore_index=True)
        else:
            logger.warn("Invalid realization, no STATUS file, %s",
                            abspath)
            raise ValueError
        if os.path.exists(os.path.join(abspath, 'jobs.json')):
            self.files = self.files.append({'LOCALPATH': 'jobs.json',
                                            'FILETYPE': 'json',
                                            'FULLPATH': os.path.join(abspath,
                                                               'jobs.json')},
                                           ignore_index=True)

        if os.path.exists(os.path.join(abspath, 'parameters.txt')):
            self.files = self.files.append({'LOCALPATH': 'parameters.txt',
                                            'FILETYPE': 'txt',
                                            'FULLPATH': os.path.join(abspath,
                                                                     'parameters.txt')},
                                           ignore_index=True)
    @property
    def parameters(self):
        """Return the contents of parameters.txt as a dict

        Strings will attempted to be parsed as numeric, and 
        dictionary datatypes will be either int, float or string.

        Parsing is aggressive, parameter values that are by chance
        integers in a particular realization will be integers,
        but should aggregate well with floats from other realizations.

        Returns:
            dict: keys are the first column in parameters.txt, values from 
                the second column
        """
        paramfile = self.files[self.files.LOCALPATH=='parameters.txt']
        params = pd.read_table(paramfile.FULLPATH.values[0], sep=r'\s+',
                               index_col=0,
                               header=None)[1].to_dict()
        for key in params:
            params[key] = parse_number(params[key])
        return params

def parse_number(value):
    """Try to parse the string first as an integer, then as float,
    if both fails, return the original string.

    Caveats: Know your Python numbers:
    https://stackoverflow.com/questions/379906/how-do-i-parse-a-string-to-a-float-or-int-in-python

    Beware; this is a minefield. 

    Returns:
        int, float or string
    """
    if isinstance(value, int):
        return value
    elif isinstance(value, float):
        if int(value) == value:
            return int(value)
        else:
            return value
    else:
        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                return value 
    
class VirtualRealization():
    """A computed or archived realization.

    Computed or archived, one cannot assume to have access to the file
    system containing original data.

    Datatables that in a ScratchRealization was available through the
    files dataframe, is now available as dataframes in a dict accessed
    by the localpath in the files dataframe from ScratchRealization-

    """
    def __init__(self, path=None, description=None):
        self._description = ''
        if description:
            self._description = description
