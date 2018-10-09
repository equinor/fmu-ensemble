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
something we can treat in this way, for which the `VirtualRealization`
object is used. This "mean realization" is not a physical realization
where all numbers make physically sense, it just gives you the mean of
all the data, over the realizations for scalars, for each point in
time for time-series.

As an example, oil, gas, and pressure profiles in a statistical
realization, will typically not be compatible, it that it can be
physically impossible for the reality to obtain these profiles. Use
and interpret with care!

Supported statistical aggregations are `mean`, `median`, `min`, `max`,
`std`, `var` and `pXX` where `XX` is a number between `00` and `99`
being the percentile you want. You access this functionality through
the function `agg()` in the ensemble objects.

Pandas and Numpy is used under the hood for all computations, and for
percentiles/quantiles, the oil industry has an opposite meaning of
quantiles. When you ask for `p10` ("high case") it will be translated
to "`p90`" before beint sent to Pandas and Numpy for computation.

.. code-block:: python

    ens = ensemble.ScratchRealization('ref-ensemble',
             '/scratch/foo/r018-ref-case/realization-*/iter-3')

    mean = ens.agg('mean')

    # What is the mean value of "FWL" in parameters.txt?
    print(mean['parameters.txt']['FWL'])

    # What is the mean ultimate FOPT
    print(mean['unsmry-monthly']['FOPT'].iloc[-1])
    # (.iloc[-1] is here Pandas functionality for accessing the last
    # row)
  

Comparing realizations or ensembles
-----------------------------------

Any linear combinations of ensembles or realizations is possible to
compute, in a pointwise manner. This includes all common data in the
linear combination.

Computing the sum of two realizations is only a matter of adding them
in your Python interpreter. The end result is a object you can treat
similar to a realization, asking for its data using `get_df()`, or
asking for the summary data using `get_smry()`. Eclipse time-series
will be combined at point-wise in time, but only on shared
time-steps. It is therefore recommended to interpolate them to
e.g. monthly time interval prior to combination.

Ensembles can be linearly combined analogously to realizations, and
will be matched on realization index `REAL`. 

.. code-block:: python

    refens = ensemble.ScratchRealization('ref-ensemble',
                '/scratch/foo/r018-ref-case/realization-*/iter-3')
    iorens = ensemble.ScratchRealization('ior-ensemble',
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
