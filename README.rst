.. image:: https://badge.fury.io/py/fmu-ensemble.svg
    :target: https://badge.fury.io/py/fmu-ensemble

.. image:: https://img.shields.io/github/actions/workflow/status/equinor/fmu-ensemble/fmu-ensemble.yml?branch=master
    :target: https://github.com/equinor/fmu-ensemble/actions?query=workflow%3Afmu-ensemble

.. image:: https://codecov.io/gh/equinor/fmu-ensemble/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/equinor/fmu-ensemble

.. image:: https://img.shields.io/badge/python-3.8%20|%203.9%20|%203.10%20|%203.11%20|%203.12-blue.svg
    :target: https://www.python.org

.. image:: https://img.shields.io/badge/License-GPLv3-blue.svg
    :target: https://www.gnu.org/licenses/gpl-3.0

==============================
Introduction to *FMU Ensemble*
==============================

FMU Ensemble is a Python module for handling simulation ensembles
originating from an FMU (Fast Model Update) workflow.

For documentation, see the
`github pages for this repository <https://equinor.github.io/fmu-ensemble/>`_.

Ensembles consist of realizations. Realizations consist of (input and)
output from their associated *jobs* stored in text or binary files.
Selected file formats (text and binary) are supported.

This module will help you handle ensembles and realizations (and their
associated data) as Python objects, and thereby facilitating the use
use of other Python visualizations modules like webviz, plotly or
interactive usage in IPython/Jupyter.

If run as a post-workflow in Ert, a simple script using this library
can replace and extend the existing *CSV_EXPORT1* workflow

This software is released under GPL v3.0
