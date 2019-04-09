.. highlight:: shell

============
Contributing
============

Contributions are welcome, and they are greatly appreciated! Every
little bit helps, and credit will always be given.

You can contribute in many ways:

Types of Contributions
----------------------

Report Bugs
~~~~~~~~~~~

Report bugs at https://git.equinor.com/fmu-utilities/fmu-ensemble/issues.

If you are reporting a bug, please include:

* Your operating system name and version.
* Any details about your local setup that might be helpful in troubleshooting.
* Detailed steps to reproduce the bug.

Fix Bugs
~~~~~~~~

Look through the Git issues for bugs. Anything tagged with "bug"
and "help wanted" is open to whoever wants to implement it.

Implement Features
~~~~~~~~~~~~~~~~~~

Look through the Git issues for features. Anything tagged with "enhancement"
and "help wanted" is open to whoever wants to implement it.

Write Documentation
~~~~~~~~~~~~~~~~~~~

fmu-ensemble could always use more documentation, whether as part of the
official fmu-ensemble docs, in docstrings, or even on the web in blog posts,
articles, and such.

Submit Feedback
~~~~~~~~~~~~~~~

The best way to send feedback is to file an issue
at https://git.equinor.com/fmu-utilities/fmu-ensemble/issues.

If you are proposing a feature:

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.
* Remember that this is a volunteer-driven project, and that contributions
  are welcome :)


Code standards
--------------

It is very important to be complient to code standards. A summary:

PEP8 and PEP20
~~~~~~~~~~~~~~

* Use PEP8 standard (https://www.python.org/dev/peps/pep-0008/) and PEP20 philosophy.
  This implies:

  * Line width max 79

  * Naming: files_as_this, ClassesAsThis, ExceptionsAsThis, CONSTANTS,
    function_as_this, method_as_this

  * Use a single underscore to protect instance variables, other private
    variables and and private classes

  * 4 space indents (spaces, no tabs)

  * Single quotes to delimit strings, triple double quotes in docstrings.

  * One space before and after =, =, +, * etc

  * No space around  = in keword lists, e.g. my_function(value=27, default=None)

  * Avoid one or two letter variables, even for counters. And meaningful names, but don't
    overdo it.

  * See also: https://git.equinor.com/fmu-utilities/fmu-coding-practice/blob/master/python-style.md


In addition:
~~~~~~~~~~~~

* Start with documentation and tests. Think and communicate first!

* Docstrings shall start and end with """ and use Google style.

* Use pytest as testing engine

* Code shall be be Python 2.7.13 + and python 3.4 + compliant


Use flake8 and/or pylint to check
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  python -m flake8 mycode.py

  make flake   # for all

The pylint is rather strict and sometimes exceptions are needed... , but anyway quite useful!

  python -m pylint mycode.py

  make lint   # for all

Get Started!
------------

Ready to contribute? Here's how to set up `fmu-ensemble` for local development.

1. Clone your fork locally::

     $ git clone git@git.equinor.com:<your-user>/fmu-ensemble.git
     $ cd fmu-ensemble
     $ git remote add upstream git@git.equinor.com:fmu-utilities/fmu-ensemble.git

   This means your `origin` is now your personal fork, while the actual master
   is at `upstream`.

3. See the rest of recipe here:
   https://git.equinor.com/fmu-utilities/fmu-coding-practice/blob/master/developer-guide.md
