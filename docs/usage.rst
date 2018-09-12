Usage
=====

fmu-ensemble is designed for use in several scenarios:

* Interactive use in the (i)python interpreter
* Interactive usage inside a Jupyter environment
* Part of an ERT workflow, typically after the ensemble is finished as
  a *POST_WORKFLOW_HOOK*
* Part of other scripts or utilities, either for analysis or
  preparatory work before f.ex. a webviz instance is generated

As an introduction to the module, we will go through interactive usage
in the python interpreter. Whether you use ipython or jupyter does not
matter. It is recommended to choose ipython over python.


Prerequisites
-------------

Basic knowlegde of Python is needed to use the module. For simple use,
copy-paste from other projects will take you far. For something extra,
it is strongly recommended to spend time learning the `Pandas`_
library and understand how you can in very short Python code do a lot
of data processing and handling.

.. _Pandas: https://pandas.pydata.org/

Basic interactive usage
-----------------------

Loading an ensemble
^^^^^^^^^^^^^^^^^^^

An ensemble must be loaded from the file system (typically `/scratch`)
into Python's memory first.

.. code-block:: python

   from fmu import ensemble

   ens = ensemble.ScratchEnsemble('reek_r001_iter0',
            '/scratch/fmustandmat/r001_reek_scratch/realization-*/iter-3')

   # Type the object name to check what you got
   ens
   # the output should be something like
   #   <Ensemble reek_r001_iter0, 50 realizations>
            
Change the path to your own if you do not want to try this particular ensemble.

Pay attention to the wildcard path. ``iter-3`` is fixed here, and you
cannot use ``iter-*`` in this call, as that would not be an ensemble. If
you want to load ``iter-*`` you are initalizing an *ensemble set* which
is documented further down.

When doing this, only rudimentary loading of the ensemble is
performed, like loading ``STATUS`` and ``parameters.txt``. It is the intention
that this operation should be fast, and any heavy data lifting is not
done until the user requests it.

When an ensemble is loaded into memory, you can ask for certain properties,

.. code-block:: python

    # Obtain a Pandas Dataframe of the parameters
    ens.parameters

    # Unique realizations indices in the ensemble:
    ens.parameters['REAL'].unique()

    # List of parameters available:
    ens.parameters.columns


Reading Eclipse data
^^^^^^^^^^^^^^^^^^^^

The ensemble class has specific features for output from Eclipse
simulations, or output from any simulator in the binary format used by
Eclipse (e.g. OPM etc).

.. code-block:: python

    # Get a dataframe with monthly summary data for all field vectors
    # and all well vectors
    smry = ens.from_smry(column_keys=['F*', 'W*'], time_index='monthly')

The python object ``smry`` is now a Pandas DataFrame (a table)
containing the summary data you requested. Each row is the values for
a specific realization at a specific time. Pandas DataFrames can be
written to disk as CSV files quite easily using e.g.
``smry.to_csv('summaryvectors.csv', index=False)``. Look up Pandas
documentation for further possibilities.

By default, Eclipse summary files will be searched for in
`eclipse/model`, and then files with the suffix `*.UNSMRY`. In case
you either have multiple `UNSMRY` files in that directory, or if you
have them in a different directory you need to hint to the exact
location beforehand, using the *file discovery* feature. If your
Eclipse output files is at the realization root (the old standard),
you only need to issue

.. code-block:: python

    ens.find_files("*.UNSMRY")

prior to running `from_smry()`. If your problem is multiple Eclipse
run in the same directory, you have to explicitly discover the full
path for the file in the call to `find_files()`.


Internalized data
^^^^^^^^^^^^^^^^^

The ensemble object (which is just a collection of realization
objects) will internalize the data it reads when you call
``from_<something>()``, meaning that it will keep the dataframes
produced in memory for later retrieval. You can ask the ensemble
objects for what data it currently contains by calling ``ens.keys()``
(this is a call that is forwarded to each realization, and you are
seeing all keys that are in at least one realization)

The ensemble object is able to aggregate any data that its
realizations has, using the general function ``get_df()``. When we
asked for the ensemble parameters above, what actually happened is a
call to ``get_df('parameters.txt')``, and when we got all summary
vectors for all realizations merged into one table above,
``get_df('unsmry-monthly.csv')`` was called under the hood.

In the objects, these dataframes are stored with filenames as
keys. When checking ``keys()`` after having run ``from_smry()``, you
will see a pathname in front of ``unsmry-monthly.csv`` which is where
the dataframe will be written to if you want to dump a realization to
disk. For convenience in interactive use, you do not need to write the
entire pathname when calling ``get_df()``, but *only* when there is no
ambiguity. You may also skip the extension ``.csv`` or ``.txt``.

Reading data from text files
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Text files in this concept is a special case of text files arranged
similarly to the already mentioned ``parameters.txt``

.. code-block:: text

    <key1> <value1>
    <key2> <value2>
    etc..

Think of the values in such text files as scalar values for
realizations, but you can put anything into them. You can use as many
of these kinds of text files as you want, in order to categorize
inputs and/or outputs. As an example, put any scalar results that you
produce though any code into a file called ``outputs.txt`` in every
realization directory, and call
``myensembleobject.from_txt('outputs.txt')``.


Reading tabular data from CSV files
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

CSV files are handled analogously to txt files, in that you read them
in by ``from_csv(filename)`` (where ``filename`` is the filename local
to each realization). The data will be stored with the filename as the
key, and you can get back the aggregated data set using
``get_df(filename)``.

In aggregations from ensembles, the first column will always be
``REAL`` which is the realization index. The next columns will be from
the CSV data you loaded.

In case you need to clean up imported files, it is possible to delete columns and rows from internalized dataframes through the `drop()` functionality. For an ensemble object called `ens` you may issue the following:

.. code-block:: python

    ens.drop('parameters.txt', key='BOGUSDATA')
    ens.drop('parameters.txt', keys=['FOO1', 'FOO2', 'FOO3'])
    ens.drop('geo_gas_volumes.csv', rowcontains='Totals') # Deletes all rows with 'Totals' anywhere.
    ens.drop('geo_oil_volumes.csv', column='Giip')
    ens.drop('unsmry-monthly', rowcontains='2000-01-01') # Enter dates as strings

When called on `ScratchEnsemble` object the drops occur in each linked
realization object, while on virtual ensembles, it occurs directly in
its dataframe.
