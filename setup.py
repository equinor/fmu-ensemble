#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""
from glob import glob
from os.path import basename
from os.path import splitext

from setuptools import setup, find_packages
import versioneer

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
]

setup_requirements = [
    'pytest-runner',
]

test_requirements = [
    'pytest',
]

fmuensemble_function = ('fmuensemble='
                        'fmu.ensemble.unknowrunner:main')

setup(
    name='fmu.ensemble',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Library for various config scripts in FMU scope",
    long_description=readme + '\n\n' + history,
    author="Jan C. Rivenaes",
    author_email='jriv@statoil.com',
    url='https://git.equinor.com/fmu-utilities/fmu-ensemble',
    packages=find_packages('src'),
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
