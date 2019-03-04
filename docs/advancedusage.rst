Advanced usage
==============

Merging data
------------

The ``CSV_EXPORT1`` workflow built into ERT is performing one of the
specific tasks that can now be accomplished using this module. That
CSV export is just a merge of the dataframes coming from
``parameters.txt`` and the Eclipse summary data.

.. code-block:: python

    import pandas as pd
    smry = reekensemble.load_smry(time_index='monthly')
    params = reekensemble.parameters
    # Match the two tables where the value of REAL is identical:
    smry_params = pd.merge(smry, params)
    smry_params.to_csv('smry_params.csv', index=False)

For finer control, you can specify exactly which summary vectors you
want to include, the time resolution, and perhaps also a subset of the
parameters. For example, if you have computed any kind of scalar data
pr. realization and put that into ``outputs.txt``, you can merge with
``load_txt('outputs.txt')`` instead of ``params`` in the code above.


Statistics over ensembles
-------------------------

Statistics over ensembles can be computed by aggregating their data
and presenting them in realization-like objects. A "*mean*" of an
ensemble is possible to compute for all data an ensemble contains, and
the result is something that we treat like a *realization*. It is
important to realize that this result is **not** a realization, only
something we can treat in this way, for which the ``VirtualRealization``
object is used. This "mean realization" is not a physical realization
where all numbers make physically sense, it just gives you the mean of
all the data, over the realizations for scalars, for each point in
time for time-series.

As an example, oil, gas, and pressure profiles in a statistical
realization, will typically not be compatible, it that it can be
physically impossible for the reality to obtain these profiles. Use
and interpret with care!

Supported statistical aggregations are ``mean``, ``median``, ``min``,
``max``, ``std``, ``var`` and ``pXX`` where ``XX`` is a number between
``00`` and ``99`` being the percentile you want. You access this
functionality through the function ``agg()`` in the ensemble objects.

Pandas and Numpy is used under the hood for all computations, and for
percentiles/quantiles, the oil industry has an opposite meaning of
quantiles. When you ask for ``p10`` ("high case") it will be translated
to ``p90`` before beint sent to Pandas and Numpy for computation.

.. code-block:: python

    ens = ensemble.ScratchEnsemble('ref-ensemble',
             '/scratch/foo/r018-ref-case/realization-*/iter-3')

    mean = ens.agg('mean')

    # What is the mean value of "FWL" in parameters.txt?
    print(mean['parameters.txt']['FWL'])

    # What is the mean ultimate FOPT
    print(mean['unsmry--monthly']['FOPT'].iloc[-1])
    # (.iloc[-1] is here Pandas functionality for accessing the last
    # row)
  

Comparing realizations or ensembles
-----------------------------------

Any linear combinations of ensembles or realizations is possible to
compute, in a pointwise manner. This includes all common data in the
linear combination.

Computing the sum of two realizations is only a matter of adding them
in your Python interpreter. The end result is a object you can treat
similar to a realization, asking for its data using ``get_df()``, or
asking for the summary data using ``get_smry()``. Eclipse time-series
will be combined at point-wise in time, but only on shared
time-steps. It is therefore recommended to interpolate them to
e.g. monthly time interval prior to combination.

Ensembles can be linearly combined analogously to realizations, and
will be matched on realization index ``REAL``. 

.. code-block:: python

    refens = ensemble.ScratchEnsemble('ref-ensemble',
                '/scratch/foo/r018-ref-case/realization-*/iter-3')
    iorens = ensemble.ScratchEnsemble('ior-ensemble',
                '/scratch/foo/r018-ior-case/realization-*/pred')

    # Calculate the delta ensemble
    deltaens = iorens - refens

    # Obtain the field gain:
    fieldgain = deltaens.get_smry(column_keys=['FOPT'],
                                  time_index='monthly')

If the ensembles you want to combine cannot be compared realization by
realization, or does not even contain the same number of realizations,
you should first aggregate the ensembles (mean or anyone else), and
then construct delta objects of the statistical realizations.

Remember that this library does not help you in interpreting these
results correctly, it only gives you the opportunity to calculate them!


Working with observations
-------------------------

Observations for history matching can be loaded, and computations
(comparisons) of observed data versus simulated data can be performed.

The Observation object can be initizalized using YAML files or from
a Python dictionary.

If you are opting for simple usage, just being able to compare ``FOPT``
versus ``FOPTH`` in your ensemble, your observation config could look
like:

.. code-block:: yaml

    # Eclipse summary vectors compared with allocated summary vectors
    smryh:
      - key: FOPT
        histvec: FOPTH

This file can be loaded in Python:

.. code-block:: python

    # Assume the yaml above has been put in a file:
    obs = ensemble.Observations('fopt-obs.yml')

Alternatively, it is possible to initialize this directly without the filesystem:

.. code-block:: python

    obs = ensemble.Observations({'smryh': [{'key': 'FOPT',
            'histvec': 'FOPTH'}]})


.. code-block:: python

    # Load an ensemble we want to analyze
    ens = ensemble.ScratchEnsemble('hmcandidate',
            '/scratch/foo/something/realization-*/iter-3')

    # Perform calculation of misfit
    # A dataframe with computed mismatches is returned.
    # We only have one "observation" for each realization, so
    # only one row pr. realization is returned.
    misfit = obs.mismatch(ens)

    # Sort ascending by L1 (absolute error) and print the realization
    # indices of the first five:
    print(misfit.sort_values('L1').head()['REAL'].values)
    # Will return f.ex:
    #   [ 38  26 100  71  57]


For comparisons with single measured values (recommended for history
matching), use the YAML syntax:

.. code-block:: yaml
                
    smry:
      # Mandatory elements per entry: key and observations
    - key: WBP4:OP_1
        # This is a global comment regarding this set of observations
        comment: "Shut-in pressures converted from well head conditions"
        observations:
           # Mandatory elements per entry in ecl_vector observations: value, error, date
           - {value: 251, error: 4, date: 2001-01-01}
           - {value: 251, error: 10, date: 2002-01-01}
           - {value: 251, error: 10, date: 2003-01-01,
              comment: First measurement after sensor drift correction}


Representative realizations
---------------------------

It is possible to utilize the observation support for calculating
similarity between realizations. An example of this is to create a
"mean" realization by use of the aggregation functionality (or p10,
p90 etc.) and then rank the ensemble members by how similar they are
to this aggregated realization. It is possible to pick certain summary
data from the virtual realization as "observations", and calculate
mismatches. For this, a utility function ``load_smry()`` is provided
by the Observation object to load "virtual" observations from an
existing realization. If you then use the Observation object to
compute mismatches, and then rank realizations by the mismatch, you
can pick the realization that is closest to your statistics of choice.

.. code-block:: python

    # Load an ensemble we want to analyze
    ens = ensemble.ScratchEnsemble('hmensemble',
            '/scratch/foo/something/realization-*/iter-3')

    # Calculate a "mean" realization
    mean = ens.agg('mean')

    # Create an empty observation object
    obs = Observations({})

    # Load data from the mean realization as virtual observations:
    obs.load_smry(mean, 'FOPT', time_index='yearly')

    # Calculate the difference between the ensemble members and the
    # mean realization:
    mis = obs.mismatch(ens)

    # Group mismatch data by realization, and pick the realization
    # index with the smallest sum of squared errors ('L2')
    closest_to_mean = mis.groupby('REAL').sum()['L2']\
                                         .sort_values()\
                                         .index\
                                         .values[0]
