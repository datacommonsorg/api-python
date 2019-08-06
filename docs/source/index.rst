.. Data Commons Python Client API documentation master file, created by
   sphinx-quickstart on Fri Aug  2 15:29:27 2019.

Data Commons Python Client API
==============================

.. toctree::
   :maxdepth: 1
   :hidden:

   Modules <modules>
   License <https://github.com/google/datacommons/blob/master/LICENSE>

Overview
--------

`Data Commons <http://datacommons.org/>`_ is an open knowledge graph that
connects data from different sources (US Census, NOAA, Bureau of Labor
Statistics, and more) under a single unified representation. It contains
statements about real world entities like

- Santa Clara County is contained in the State of California
- The latitude of Berkeley, CA is 37.8703
- The population of all persons in Maryland has a total count of 5,996,080.

It links references to the same entities (such as cities, counties,
organizations, etc.) across different datasets to nodes in the graph, so that
users can access data about a particular entity aggregated from different
sources.

The **Data Commons Python Client API** is a Python library that allows
developers to programmatically access nodes in the Data Commons graph. This
package allows users to explore the structure of the graph, integrate statistics
in the Data Commons graph into data analysis workflows and much more.

Getting Started
---------------

Begin by installing the :code:`datacommons` package through :code:`pip`.

.. code-block:: bash

    $ pip install datacommons

For more information about installing :code:`pip` and setting up other parts of
your Python development environment, please refer to the
`Python Development Environment Setup Guide <https://cloud.google.com/python/setup>`_
for Google Cloud Platform. To get started using the Python Client API, simply
import the :code:`datacommons` package.

.. code-block:: python

    import datacommons as dc

From here you can view our tutorials on how to use the API to perform certain
tasks, or see a full list of functions, classes and methods available for use
by navigating to `modules <modules.html>`_

Further Resources
-----------------

To learn more about Data Commons and get involved, please visit the following
resources.

- `Our homepage <http://datacommons.org/>`_
- `Our GitHub Page <https://github.com/google/datacommons/>`_
- `Our Issues Page <https://github.com/google/datacommons/issues>`_
- `The Client API PyPI Page <https://pypi.org/project/datacommons/>`_
