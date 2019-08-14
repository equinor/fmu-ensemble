==============================
Introduction to *fmu.ensemble*
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


.. image:: https://api.codacy.com/project/badge/Grade/58c0d639256e4c2da33c32ac0a3e45a9
   :alt: Codacy Badge
   :target: https://app.codacy.com/app/berland/fmu-ensemble?utm_source=github.com&utm_medium=referral&utm_content=equinor/fmu-ensemble&utm_campaign=Badge_Grade_Dashboard