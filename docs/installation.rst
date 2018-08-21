.. highlight:: shell

============
Installation
============


Stable release
--------------

The stable release is distributed for all Equinor users through
``/project/res``. Eventually it will be distribued through Komodo,
with access to stable, testing and bleeding versions.

As of August 2018, you need to enable komodo testing prior to running
fmu-ensemble code.

From sources
------------

The sources for fmu-ensemble can be downloaded from the `Equinor Git repo`_.

You can either clone the public repository:

.. code-block:: console

    $ git clone git://git.equinor.com/fmu-utilities/fmu-ensemble

Once you have a copy of the source, and you have a `virtual environment`_,
you can install it with:

.. code-block:: console

    $ make install


.. _Equinor Git repo: https://git.equinor.com/fmu-utilities/fmu-ensemble
.. _virtual environment: http://docs.python-guide.org/en/latest/dev/virtualenvs/
