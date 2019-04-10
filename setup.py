#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""
from glob import glob
import os
from os.path import basename
from os.path import splitext

from setuptools import setup, find_packages
import versioneer

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

def relpath(*args):
    """Return path of args relative to this file"""
    root = os.path.dirname(__file__)
    if isinstance(args, str):
        return os.path.join(root, args)
    return os.path.join(root, *args)


def requirements():
    reqs = []
    with open(relpath('requirements.txt'), 'r') as f:
        reqs = [req.strip() for req in f]
    return reqs

requirements = requirements()

setup_requirements = [
    'pytest-runner',
]

test_requirements = [
    'pytest',
]

fmuensemble_function = ('fmuensemble='
                        'fmu.ensemble.unknowrunner:main')

# -----------------------------------------------------------------------------
# Explaining versions:
# As system the PEP 440 major.minor.micro is used:
# - major: API or any very larger changes
# - minor: Functionality added, mostly backward compatibility but some
#          functions may change. Also includes larger refactoring of code.
# - micro: Added functionality and bug fixes with no expected side effects
# - Provide a tag on the form 3.4.0 for each release!
#
# Also, a verymicro may _sometimes_ exist (allowed in PEP440); which can be:
# - One single, very easy to understand, bugfixes
# - Additions in documentations (not affecting code)
# - These may not be tagged explicity!
#
# Hence, use major.minor.micro or major.minor.micro.verymicro scheme.
# -----------------------------------------------------------------------------


def the_version():
    """Process the version, to avoid non-pythonic version schemes.

    Means that e.g. 1.5.12+2.g191571d.dirty is turned to 1.5.12.2.dev0

    This function must be ~identical to fmu-tools._theversion.py
    """

    version = versioneer.get_version()
    sver = version.split('.')
    print('\nFrom TAG description: {}'.format(sver))

    useversion = 'UNSET'
    if len(sver) == 3:
        useversion = version
    else:
        bugv = sver[2].replace('+', '.')

        if 'dirty' in version:
            ext = '.dev0'
        else:
            ext = ''
        useversion = '{}.{}.{}{}'.format(sver[0], sver[1], bugv, ext)

    print('Using version {}\n'.format(useversion))
    return useversion

setup(
    name='fmu-ensemble',
    version=the_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Library for various config scripts in FMU scope",
    long_description=readme + '\n\n' + history,
    author="Håvard Berland",
    author_email='havb@equinor.com',
    url='https://git.equinor.com/fmu-utilities/fmu-ensemble',
    packages=find_packages('src'),
#    namespace_packages=['fmu'],
    package_dir={'': 'src'},
    py_modules=[splitext(basename(path))[0] for path in glob('src/*.py')],
    entry_points={
        'console_scripts': [fmuensemble_function]
    },
    include_package_data=True,
    install_requires=requirements,
    zip_safe=False,
    keywords='fmu, ensemble',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.6',
    ],
    test_suite='tests',
    tests_require=test_requirements,
    setup_requires=setup_requirements,
)
