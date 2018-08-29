==========================
The library *fmu.ensemble*
==========================

FMU Ensemble is a Python module for handling simulation ensembles
originating from an FMU (Fast Model Update) workflow.

Ensembles consist of realizations. Realizations consist of input and
output from their associated *jobs* stored in text or binary files.

This module will help you handle ensembles and realizations (and their
associated data) as Python objects, and thereby facilitating the use
use of other Python visualizations modules like webviz, plotly or
interactive usage in IPython/Jupyter.

If run as a post-workflow in Ert, a simple script using this library
can replace and extend the existing *CSV_EXPORT1* workflow

This software is internal to Equinor ASA and shall not be shared
externally.
