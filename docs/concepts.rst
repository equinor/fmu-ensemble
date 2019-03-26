Concepts in fmu-ensemble
========================

There are some concepts in fmu-ensemble that one needs to understand
at some point

Realization
-----------

A realization is a single model run, where parameters are determined (but
can be drawn stochastically at the Ensemble level). It usually includes one
Eclipse run, but that is not a requirement. As long as you have a done some
computational job that has left (supported) files on the filesystem, it can
be possible to load in results using fmu-ensemble.


ScratchRealization
^^^^^^^^^^^^^^^^^^

The class `ScratchRealization` is a Python object that can load
realization results (and input) from the filesystem, typically located
on ``/scratch``. You can ask the object to load and thereby *internalize* data,
with the ``load_*()`` functions. The internalization is an important
concept. All data that you internalize will be stored in the object,
it can be easily reaccessed, statistics can be computed, and it will
be added to a corresponding VirtualRealization (see below).

Additionally, you may find ``get_*()`` functions that can access certain
datatypes. Common for these is that they will *not* modify the object,
it is a read-once operation. This is particularly relevant for Eclipse
summary data, where you at different times may ask for different
subsets and at different sampling frequencies, but do not want to
internalize all the data into the object.

VirtualRealization
^^^^^^^^^^^^^^^^^^

A `VirtualRealization` is typically a `ScratchRealization` that has lost
its knowledge of the original data on disk. The object only knows of
the data that was internalized, and the main access function is
``get_df()``. Another typical source for a virtual realization is a
calculated realization, either as a point-wise statistical aggregate
of a collection of realizations (ensemble), or maybe from a linear
combination of realizations (then coming from the object
``RealizationCombination``.

There are no 
Virtual realizations have features to write their internal data to
disk, the ``to_disk()`` feature. This can be used to store stripped down
versions of realizations, or to be able to store computed
realizations. You may both write to disk into a file structure that
would resemble the original realization (and you can edit the files if
you are bold enough). The virtual realization can later be
instantiated from the dumped disk structure by ``load_disk()``. Another
variant for storage is to use ``to_json()`` which will dump all data in
as a *json* datatype, for which there probably exists use cases.

RealizationCombination
^^^^^^^^^^^^^^^^^^^^^^

The object `RealizationCombination` is an object that the user will
not observe directly, but it will work under the hood every time
arithmetic operations on realizations are done.


Ensemble
--------

An ensemble is a collection of realizations, where the realizations
share some common features making it relevant to do statistical
aggregations over them. This does not necessarily exclude random
collections of realizations, but if the realizations do not share
anything, they can always be collected in simple Python lists as well.

A requirement is that each member of the ensembled are referred to by
an *integer*, and will always be present in the column ``REAL`` in
aggregated dataframes.

ScratchEnsemble
^^^^^^^^^^^^^^^

A `ScratchEnsemble` is an ensemble that is initialized from a
directory of realization-runs on the file system, typically on
`/scratch`. This object can do a full initialization of all
`ScratchRealization` in a specific directory, and collect them into a
ensemble object.


VirtualEnsemble
^^^^^^^^^^^^^^^

Analogous to the relationship between `ScratchRealization` and
`VirtualRealization`, a `VirtualEnsemble` is an ensemble with no
strings attached to the original filesystem. All data from its underlying
realizations is aggregated and full dataframes are stored. The object
is able to construct new `VirtualRealization` objects from its data, both
by picking by index, or from statistical aggregations.

You can ask a VirtualEnsemble for ``get_smry()`` in which it will try its
best to locate internalized Eclipse summary data, and then interpolate the
data to your chosen time index. This is opposed to a ScratchEnsemble which
in that case would go back to the original binary files and give the correct
answer.

EnsembleCombination
^^^^^^^^^^^^^^^^^^^

Whenever you try do add or substract ensembles, the objects you get in
return are of type `EnsembleCombination`. These objects act as
ensembles, but its data is always a combination of the data in two or
more ensembles (or a single ensemble scaled by a scalar).

Calculating a combination of ensembles can be computationally
expensive, depending on the amount of data requested and included. The
actual combination of numbers is *not* done until you actually ask for
it. That means that initialization of `EnsembleCombination` is fast,
but when you ask for its data, it might take time. If you want all
data to be evaluated in one go, you ask the object for a
`VirtualEnsemble` using the function `to_virtual()`, which means that
all internalized data is evaluated and returned to you for further
access and/or storage.

The implementation of the linear algebra over ensembles and
realizations is accomplished using a `Binary Expression Tree`_, with
`ScratchEnsemble` or `VirtualEnsemble` at the leaf nodes.


.. _Binary Expression Tree: https://en.wikipedia.org/wiki/Binary_expression_tree
