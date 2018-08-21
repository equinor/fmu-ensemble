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

.. code-block:: python

    # The results from the Eclipse summary data can be obtained as this
    smry = reekensemble.get_smry(column_keys=['F*', 'W*'], time_index='monthly')

Summary data is typical to combine with parameters data, in order to be able
to analyze how the parameter values influence simulated data. The classical 
CSV export from ERT is doing exactly this, and the same export can be
accomplished using the following code

.. code-block:: python

    import pandas as pd
    smry = reekensemble.get_smry(time_index='monthly')
    params = reekensemble.parameters
    # Match the two tables where the value of REAL is identical:
    smry_params = pd.merge(smry, params)
    smry_params.to_csv('smry_params.csv', index=False)

For finer control, you can specify exactly which summary vectors you want
to include, the time resolution, and perhaps also a subset of the parameters.

