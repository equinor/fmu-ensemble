#!/usr/bin/env python

"""The setup script."""
from setuptools import setup, find_packages

try:
    from sphinx.setup_command import BuildDoc

    cmdclass = {"build_sphinx": BuildDoc}
except ImportError:
    # sphinx not installed - do not provide build_sphinx cmd
    cmdclass = {}


with open("README.rst") as readme_file:
    readme = readme_file.read()

with open("HISTORY.rst", "rb") as history_file:
    # Norwegian characters in HISTORY.rst
    history = history_file.read().decode("UTF-8")

REQUIREMENTS = [
    "ecl>=2.9",
    "numpy",
    "pandas",
    "pyyaml>=5.1",
]

SETUP_REQUIREMENTS = ["setuptools>=28", "setuptools_scm"]

with open("test_requirements.txt") as f:
    test_requirements = f.read().splitlines()
with open("docs_requirements.txt") as f:
    docs_requirements = f.read().splitlines()

EXTRAS_REQUIRE = {
    "tests": test_requirements,
    "docs": docs_requirements,
    "parquet": ["pyarrow"],
}

setup(
    name="fmu-ensemble",
    use_scm_version={"write_to": "src/fmu/ensemble/version.py"},
    cmdclass=cmdclass,
    description="Python API to ensembles produced by ERT",
    long_description=readme + "\n\n" + history,
    author="Håvard Berland",
    author_email="havb@equinor.com",
    url="https://github.com/equinor/fmu-ensemble",
    license="GPLv3",
    packages=find_packages("src"),
    package_dir={"": "src"},
    include_package_data=True,
    install_requires=REQUIREMENTS,
    zip_safe=False,
    keywords="fmu, ensemble",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    test_suite="tests",
    tests_require=test_requirements,
    setup_requires=SETUP_REQUIREMENTS,
    extras_require=EXTRAS_REQUIRE,
    python_requires=">=3.6",
)
