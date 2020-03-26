#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""
from glob import glob
import os
from os.path import basename
from os.path import splitext

from setuptools import setup, find_packages
from setuptools_scm import get_version
from sphinx.setup_command import BuildDoc

with open("README.rst") as readme_file:
    readme = readme_file.read()

with open("HISTORY.rst") as history_file:
    history = history_file.read()


def relpath(*args):
    """Return path of args relative to this file"""
    root = os.path.dirname(__file__)
    if isinstance(args, str):
        return os.path.join(root, args)
    return os.path.join(root, *args)


def parse_requirements(filename):
    """Load requirements from a pip requirements file"""
    try:
        lineiter = (line.strip() for line in open(filename))
        return [line for line in lineiter if line and not line.startswith("#")]
    except IOError:
        return []


REQUIREMENTS = parse_requirements("requirements.txt")

SETUP_REQUIREMENTS = ["pytest-runner", "setuptools>=28", "setuptools_scm"]

TEST_REQUIREMENTS = parse_requirements("requirements_dev.txt")

EXTRAS_REQUIRE = {"Parquet": ["pyarrow"]}

setup(
    name="fmu-ensemble",
    use_scm_version={"write_to": "src/fmu/ensemble/version.py"},
    cmdclass={"build_sphinx": BuildDoc},
    description="Python API to ensembles produced by ERT",
    long_description=readme + "\n\n" + history,
    author="Håvard Berland",
    author_email="havb@equinor.com",
    url="https://git.equinor.com/equinor/fmu-ensemble",
    license="GPLv3",
    packages=find_packages("src"),
    package_dir={"": "src"},
    py_modules=[splitext(basename(path))[0] for path in glob("src/*.py")],
    include_package_data=True,
    install_requires=REQUIREMENTS,
    zip_safe=False,
    keywords="fmu, ensemble",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.6",
    ],
    test_suite="tests",
    tests_require=TEST_REQUIREMENTS,
    setup_requires=SETUP_REQUIREMENTS,
    extras_require=EXTRAS_REQUIRE,
)
