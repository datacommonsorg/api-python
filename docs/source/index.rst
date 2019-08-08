.. Data Commons Python Client API documentation master file, created by
   sphinx-quickstart on Fri Aug  2 15:29:27 2019.

Data Commons Python Client API
==============================

.. toctree::
   :maxdepth: 5
   :hidden:

   Getting Started <started>
   Tutorials <tutorials>
   Modules <modules>
   License <https://github.com/google/datacommons/blob/master/LICENSE>

Feedback Request
----------------

Thank you for using this experimental version of the Python Client API. Please
consider leaving us some feedback so that we can make this library more easy
to use and understand!

- Send an issue request to the
  `Data Commons Issues Page <https://github.com/google/datacommons/issues>`_.
  When creating an issue please mark the issue using the **api feedback** label!

**DISCLAIMER** This is an experimental version of the Python Client API. The
semantics and availability of this library is subject to change without
prior notice!

Getting Started
---------------

To get started using the Data Commons Python Client API, install the package
from `pip`.

.. code-block:: bash

    $ pip install git+https://github.com/google/datacommons.git@stable-1.x

Once the package is installed, you can import the `datacommons` package in
Python.

.. code-block:: python

    import datacommons as dc

You will additionally need to provision an API key to access the Data Commons
graph. Enable the `Data Commons API` on Google Cloud Platform to create an
API key. After receiving a key, you can configure the package to use it by
calling:

.. code-block:: python

    dc.set_api_key('YOUR-API-KEY')

You're now ready to start using Data Commons!

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

Further Resources
-----------------

To learn more about Data Commons and get involved, please visit the following
resources.

- `Our homepage <http://datacommons.org/>`_
- `Our GitHub Page <https://github.com/google/datacommons/>`_
- `Our Issues Page <https://github.com/google/datacommons/issues>`_
