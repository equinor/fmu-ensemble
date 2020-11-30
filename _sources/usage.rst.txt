Usage
=====

fmu-ensemble is designed for use in several scenarios:

* Interactive use in the (i)python interpreter or Jupyter
* Part of an ERT workflow, typically after the ensemble is finished as
  a *POST_WORKFLOW_HOOK*
* Part of other scripts or utilities, either for analysis or
  preparatory work before f.ex. a webviz instance is generated

As an introduction to the module, we will go through interactive usage
in the python interpreter. Whether you use ipython or jupyter does not
matter. It is recommended to choose `ipython` over `python`.


Prerequisites
-------------

Basic knowlegde of Python is needed to use the module. For simple use,
copy-paste from other projects will take you far. For something extra,
it is strongly recommended to spend time learning the `Pandas`_
library and understand how you can in very short Python code do a lot
of data processing and handling. Most data is exposed as Pandas
dataframes.

.. _Pandas: https://pandas.pydata.org/

Basic interactive usage
-----------------------

Loading an ensemble
^^^^^^^^^^^^^^^^^^^

An ensemble must be loaded from the filesystem (typically `/scratch`)
into Python's memory first.

.. code-block:: python

   from fmu import ensemble

   ens = ensemble.ScratchEnsemble('reek_r001_iter0',
            '/scratch/fmustandmat/r001_reek_scratch/realization-*/iter-3')

   # Type the object name to check what you got
   ens
   # the output should be something like
   #   <Ensemble reek_r001_iter0, 50 realizations>
            
You name your ensemble in the first argument. This name is used when you combine
the ensemble with other ensembles into an ``EnsembleSet``. The path is where on
the filesystem your realizations roots are. The realization root is also called
RUNPATH in ERT terminology, and is where you have the ``STATUS`` file among
others.

When you initialize single ensembles, ensure you do not mix ``iter-3`` with
``iter-*``, where the latter only makes sense when you initialize an
*EnsembleSet*, see below.

When a `ScratchEnsemble` object is intialized, only rudimentary loading of the
ensemble is performed, like loading ``STATUS`` and ``parameters.txt``. It is the
intention that this operation should be fast, and any heavy data parsing is not
done until the user requests it.

When an ensemble is loaded into memory, you can ask for certain properties,

.. code-block:: python

    # Obtain a Pandas Dataframe of the parameters
    ens.parameters

    # Unique realizations indices with parameters.txt 
    ens.parameters['REAL'].unique()

    # List of parameters available:
    ens.parameters.columns

Loading multiple ensembles
^^^^^^^^^^^^^^^^^^^^^^^^^^

If you have multiple ensembles in the typical ``realization-*/iter-*`` 
directory structure, you can load all these ensembles in one go:

.. code-block:: python

    ens_set = ensemble.EnsembleSet('hm_attempt01',
                  frompath='/scratch/fmustandmat/r001_reek_scratch/')

This will look for realizations and iterations and group them
accordingly.  Ensemble names will be inferred from the iteration
directory level, and will be named `iter-X`

If you have run prediction ensembles, which do not match `iter-X` in
the directory name, you have to add them manually to the ensemble set:

.. code-block:: python

    # Augment the existing ens_set object
    ens_set.add_ensemble(ensemble.ScratchEnsemble('pred-dg3',
        '/scratch/fmustandmat/r001_reek_scratch/realization-*/pred-dg3/'))

EnsembleSet object can be treated almost as Ensemble
objects. Operations on ensemble sets will typically be applied to each
ensemble member. A difference is that aggregated data structures
always have an extra column called ``ENSEMBLE`` that contains the
ensemble names.

If you in ERT have exported a "runpath file", you can initialize an
EnsembleSet from that file with

.. code-block:: python

    # Load from an ERT runpath file
    ens_set = ensemble.EnsembleSet('hm',
        runpath='/foo/bar/ert-runpath-file')

The realization and iteration integers are taken directly from the information
in this file. For runpath files with only one ensemble, it is also possible
to initialize ScratchEnsembles directly.

It is possible to load directory structures like ``iter_*/real_*``,
but you will need to look more closely into the API for the
EnsembleSet object, and provide regular expressions for determining
the iteration names and realization indices.

Obtaining warning and error messages
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Application/script authors can configure logging output to console by e.g.

.. code-block:: python

    import logging

    logging.basicConfig(level=logging.INFO)

See documentation on `Python logging`_ for more details.

.. _Python logging: https://docs.python.org/3/library/logging.html


Reading Eclipse data
^^^^^^^^^^^^^^^^^^^^

The ensemble class has specific support for parsing binary files produced
by reservoir simulator outputting the Eclipse binary format. This support
is through `libecl_`.

.. _libecl: https://github.com/equinor/libecl

.. code-block:: python

    # Get a dataframe with monthly summary data for all field vectors
    # and all well vectors
    smry = ens.get_smry(column_keys=['F*', 'W*'], time_index='monthly')

The Python object ``smry`` is now a Pandas DataFrame (a table)
containing the summary data you requested. Each row is the values for
a specific realization at a specific time. Pandas DataFrames can be
written to disk as CSV files quite easily using e.g.
``smry.to_csv('summaryvectors.csv', index=False)``. For `time_index` you
may also try `yearly`, `daily` or `raw`. Check the function 
documentation for further possibilities.

If you replace `get_smry` with `load_smry` the same dataframe will also be
internalized, see below.

By default, Eclipse summary files will be searched for in `eclipse/model`,
and then files with the suffix `*.UNSMRY`. In case you either have multiple
`UNSMRY` files in that directory, or if you have them in a different
directory you need to hint to the exact location beforehand, using the
*file discovery* (`find_files()`) feature. If your Eclipse output files is
at the realization root (the old standard), you only need to issue

.. code-block:: python

    ens.find_files("*.UNSMRY")

prior to running `load_smry()`. If your problem is multiple Eclipse
run in the same directory, you have to explicitly discover the full
path for the file in the call to `find_files()`. If you have used the
`runpathfile` feature of ensemble initialization, file discovery of
the correct `UNSMRY` file is done automatically.

Rate handling in Eclipse summary vectors
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Eclipse summary vectors with of *rate* type (oil rate, water rate etc.) are
to be interpreted carefully. A value of e.g. `FOPR` at a specific date
means that the value is valid backwards in time, until the prior point in
time where data is available.  For correct rates, you must use the `raw`
time index for `get_smry()`, anything else will only give you an
approximation. Also, you can not assume that summing the rates at every
point in time corresponds to the associated cumulative summary vectors,
e.g. `FOPT`, as there are multiple features into play here with efficienty
factors etc.

It is however possible to ask an ensemble or realization to compute so
called "volumetric rates", which are then computed from cumulative columns.
Eclipse summary rate data is ignored in this computation, only e.g. `FOPT`.
You can then ask to get a "volumetric rate" for `FOPT` at various time
indices, yearly will give you yearly volumes, monthly will give monthly
volumes etc. The data is returned as `FOPR` but you must be careful not to
mix its meaning with the original `FOPR`. It is also possible to supply a
custom time index (with arbitrary time between each index), but where the 
volumetric rates are scaled to correspond to daily/monthly/yearly rates.
These will sum up to the cumulative given correct integration (with
time interval length weigthing). 

.. code-block:: python

    # Examples for volumetric rate computations, yearly rates:
    yearly_volumes = ens.get_volumetric_rates(column_keys='FOPT',
                                              time_index='yearly')
    # For each month, compute the average daily rate:
    daily_rates = ens.get_volumetric_rates(column_keys='FOPT',
                                           time_index='monthly',
                                           time_unit='days')


Internalized data
^^^^^^^^^^^^^^^^^

The ensemble object (which holds a collection of realization
objects) will internalize the data it reads if and when you call
``load_<something>()``, meaning that it will keep the dataframes
produced in memory for later retrieval. You can ask the ensemble
objects for what data it currently contains by calling ``ens.keys()``
(this is a call that is forwarded to each realization, and you are
seeing all keys that are in at least one realization). Note that for
ScratchEnsemble objects, the data is held in each realization object, and
aggregated upon request.

The ensemble object is able to aggregate any data that its
realizations contains, using the general function ``get_df()``. When we
asked for the ensemble parameters above, what actually happened is a
call to ``get_df('parameters.txt')``.

In the objects, these dataframes are stored with filenames as keys. When
checking ``keys()`` after having run ``load_smry()``, you will see a
pathname in front of ``unsmry--monthly.csv`` which is where the dataframe
will be written to if you want to dump a realization or realization to
disk. For convenience in interactive use, you do not need to write the
entire pathname when calling ``get_df()``, but *only* when there is no
ambiguity. You may also skip the extension ``.csv`` or ``.txt``.

Reading data from text files
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Text files in this context is a special case of text files arranged
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
``myensembleobject.load_txt('outputs.txt')``.

Scalar data
^^^^^^^^^^^

There is support for text files containing only one value, either
string or numeric. There should be nothing else than the value itself
in the text file, except for comments after a comment character.

.. code-block:: python

    ens.load_scalar('npv.txt')

You are advised to add the option `convert_numeric=True` when the
values are actually numeric. This ensures that the loaded data is
interpreted as numbers, and thrown away if not. When strings are
present in in erroneous realizations, it will break aggregation as all
the data for all realizations will be treated as strings.

Scalar data will be aggregated to ensembles and ensemble sets. When
aggregated, a dataframe with the realization index in the first column
and the values in the second column. This value column has the same
name as the filename.

.. code-block:: python

    npv = ens.get_df('npv.txt')  # A DataFrame is returned, with the columns 'REAL' and 'npv.txt'
    npv_values = npv['npv.txt']  # Need to say 'npv.txt' once more to get to the column values.


Reading tabular data from CSV files
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

CSV files are handled analogously to txt files, in that you read them
in by ``load_csv(filename)`` (where ``filename`` is the filename local
to each realization). The data will be stored with the filename as the
key, and you can get back the aggregated data set using
``get_df(filename)``.

In aggregations from ensembles, the first column will always be
``REAL`` which is the realization index. The next columns will be from
the CSV data you loaded.

In case you need to clean up imported files, it is possible to delete
columns and rows from internalized dataframes through the `drop()`
functionality. For an ensemble object called `ens` you may issue the
following:

.. code-block:: python

    ens.drop('parameters.txt', key='BOGUSDATA')
    ens.drop('parameters.txt', keys=['FOO1', 'FOO2', 'FOO3'])
    ens.drop('geo_gas_volumes.csv', rowcontains='Totals') # Deletes all rows with 'Totals' anywhere.
    ens.drop('geo_oil_volumes.csv', column='Giip')
    ens.drop('unsmry--monthly', rowcontains='2000-01-01') # Enter dates as strings

When called on `ScratchEnsemble` object the drops occur in each linked
realization object, while on virtual ensembles, it occurs directly in
its dataframe.

Reading simulation grid data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Simulation static and dynamic grid data can be read and aggregated from the
ensemble and returned as a DataFrame. The current implementation can be
slow for large grid model and/or ensembles with many realizations.

.. code-block:: python
    
    # Find of the report number corresponding to the date you are interested to extract from
    ens.get_unrst_report_dates()
    # Extract the mean of following properties at the report step 4
    ens.get_eclgrid(props=['PERMX', 'FLOWATI+', 'FLOWATJ+'], report=4, agg='mean')

When called, `get_eclgrid()` reads the grid (geometry) from one
realization. Then depending if the properties requested are static or
dynamic, the corresponding `*INIT` or `*UNRST` file will be read for all
successful realization in the ensemble. The user can specify how the
results should be aggregated. Currently the options supported are `mean` or
`std`.


Filtering realizations
^^^^^^^^^^^^^^^^^^^^^^

In an ensemble, realizations can be filtered out based on certain
properties. Filtering is relevant both for removing realizations that
have failed somewhere in the process, and it is also relevant for
extracting subsets with certain properties (by values).

Generally, fmu.ensemble is very permissive of realizations with close
to no data. It is the user responsibility to filter those out if
needed. The filtering function `filter()` can be used both do to
in-place filtering, but also return VirtualEnsemble objects containing
those realizations that matched the criterion.

Examples:

.. code-block:: python

    # Assuming an ensemble where yearly summary data is loaded,
    # throw away all realizations that did not reach a certain date
    ens.filter('unsmry--yearly', column='DATE',
               columncontains='2030-01-01')

    # Extract the subset for a specific sensitivity.
    vens = ens.filter('parameters.txt', key='DRAINAGE_STRATEGY',
                      value='Depletion', inplace=False)
    
    # Remove all realizations where a specific output file
    # (that we have tried to internalize) is missing
    ens.filter('geo_oil_1.csv')

Filtering with other comparators than equivalence is not implemented.
