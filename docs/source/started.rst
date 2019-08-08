Getting Started
===============

To get started, install the :code:`datacommons` package through :code:`pip`.

.. code-block:: bash

    $ pip install git+https://github.com/google/datacommons.git@stable-1.x

For more information about installing :code:`pip` and setting up other parts of
your Python development environment, please refer to the
`Python Development Environment Setup Guide <https://cloud.google.com/python/setup>`_
for Google Cloud Platform. Afterwards, you are ready to use the Data Commons
Python Client API! Simply import package.

.. code-block:: python

    import datacommons as dc

You will additionally need to provision an API key to access the Data Commons
graph. Enable the `Data Commons API` on Google Cloud Platform to create an
API key. After receiving a key, you can configure the package to use it by
calling:

.. code-block:: python

    dc.set_api_key('YOUR-API-KEY')

You're now ready to go! From here you can view our tutorials on how to use the
API to perform certain tasks, or see a full list of functions, classes and
methods available for use by navigating to `modules <modules.html>`_
