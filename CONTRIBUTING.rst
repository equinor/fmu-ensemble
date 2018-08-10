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

  * Use a single underscore to protect internal variables and classes

  * 4 space indents (spaces, no tabs)

  * One space before and after =, =, +, * etc

  * No space around  = in keword lists, e.g. my_function(value=27, default=None)

  * Avoid one or two letter variables, except for counters. And meaningful names, but don't
    overdo it.

  * See also: https://git.equinor.com/fmu-utilities/fmu-coding-practice


In addition:
~~~~~~~~~~~~

* Start with documentation and tests. Think first!

* Docstrings shall start and end with """ and use Google style.

* Use a single underscore to protect class properties, and use a single underscore
  in file names for submodules that should not be exposed in the documentation.

* Use single ticks for strings 'mystring'.

* Use pytest as testing engine

* Code shall be be Python 2.7.10 + and python 3.4 + compliant


Use flake8 and/or pylint to check
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  python -m flake8 mycode.py

  make flake (all)

The pylint command is much stricter and sometimes wrong... , but can be quite useful!

  python -m pylint mycode.py

  make lint (all)


Get Started!
------------

Ready to contribute? Here's how to set up `fmu-ensemble` for local development.

If you you komodo python, shift to komodo-testing, as this is more python complete.

1. You need `fmu-config` installed in *your virtual env*, as fmu-ensemble is dependent
   on the `fmu.config.etc` module. Clone this repo (as in step 3), enter the folder,
   and type `make develop`. That's all (but perhaps you need to pull regularly).

2. Fork the `fmu-ensemble` repo in web browser to a personal fork

3. Clone your fork locally::

     $ git clone git@git.equinor.com:<your-user>/fmu-ensemble.git
     $ git remote add upstream git@git.equinor.com:fmu-utilities/fmu-ensemble.git

   This means your `origin` is now your personal fork, while the actual master
   is at `upstream`.

4. Activate the virtual envirioment and go to your fork::

     $ <activate your virtual env>
     $ cd fmu-ensemble/
     $ pip install -r requirements_dev.txt  (needed once or rarely)
     $ make develop  (same as pip install -e .)
     $ make test  (see that test works)

5. Create a branch for local development::

     $ git checkout -b name-of-your-bugfix-or-feature

   Now you can make your changes locally.

6. When you're done making changes, check that your changes pass flake, lint and the tests,
   including testing and docs::

     $ make flake
     $ make lint
     $ make test
     $ make docs

7. Commit your changes and push your branch to GitHub::

     $ git commit -am "Your detailed description of your changes."
     $ git push origin name-of-your-bugfix-or-feature

8. Submit a pull request (merge request) through the Git website.

9. Then remove the current branch::

     $ git checkout master
     $ git fetch upstream
     $ git merge upstream/master

10. Alterantive in one go::

     $ git pull upstream master
     $ git push
     $ git remote  (watch which remotes)

11. Delete your previous branch and make a new feature branch::

      $ git branch -d name-of-your-bugfix-or-feature
      $ git checkout -b name-of-your-new-bugfix-or-feature


Pull Request Guidelines
-----------------------

Before you submit a pull request, check that it meets these guidelines:

1. The pull request should include tests.

2. If the pull request adds functionality, the docs should be updated. Put
   your new functionality into a function with a docstring, and add the
   feature to the list in README.rst.


Tips
----

To run a subset of tests::

  $ pytest tests/test_<feature>

Or use the Makefile to speed up things::

  $ make test
