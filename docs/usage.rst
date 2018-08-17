=====
Usage
=====

Interactive usage of fmu-ensemble:

.. code-block:: python

   from fmu import ensemble

   iorensemble = ensemble.ScratchEnsemble('IOR case DG3',
                    '/scratch/snorreberg/foobert/r089/realization-*/iter-3')

When doing this, some rudimentary loading of the ensemble is
performed, like loading STATUS and parameters.txt. It is the intention
that this operation should be fast, and any heavy data lifting is not
done until the user requests it.

Note that `iter-3` from the path is *not* processed. The ensemble name
is taken from the first argument to `ScratchEnsemble`.

When an ensemble is loaded into memory, you can ask for certain properties,

.. code-block:: python

    # Obtain a Pandas Dataframe of the parameters
    iorensemble.parameters

    # Unique realizations indices in the ensemble:
    iorensemble.parameters['REAL'].unique()

    # List of parameters available:
    iorensemble.parameters.columns

Any CSV file that each realization directory contains can be aggregated
by the objects. Give a path, relative to the realization directories, and
the ensemble object can look it up:

.. code-block:: python

    # Obtain a specific CSV file from each realization as a Pandas
    # Dataframe. The dataframe will contain the merged results
    # from all realization, and tagged by the realization index in the
    # column REAL
    vol_df = reekensemble.get_csv('share/results/volumes/simulatorvolumes.csv')


