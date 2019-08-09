.. _getting_started:

Getting Started
===============

To get started using the Python Client API requires three steps.

- Instal the API using :code:`pip`
- Create an API key on `Google Cloud Platform <https://cloud.google.com/>`_ (GCP)
- Enable the API key with the Python Client API

Installing the Python Client API
--------------------------------

First, install the :code:`datacommons` package through :code:`pip`.

.. code-block:: bash

    $ pip install git+https://github.com/google/datacommons.git@stable-1.x

For more information about installing :code:`pip` and setting up other parts of
your Python development environment, please refer to the
`Python Development Environment Setup Guide <https://cloud.google.com/python/setup>`_
for Google Cloud Platform.

Creating an API Key
-------------------

Using the Data Commons Python API requires you to provision an API key on GCP.
Follow these steps to create your own key.

1.  Start by creating a `GCP Project`_ and `enabling billing`_ for that project.
2.  After creating your project, you can navigate to the GCP Console to create
    an API key.

    - Begin by navigating to **APIs & Services > Credentials** in the side
      bar or by clicking on
      `this link <https://console.developers.google.com/apis/credentials>`_
    - Click **Create Credentials** then select **API Key**

    Once you do this, you'll be presented a key that you can copy to your
    clipboard.

.. _`GCP Project`: https://console.developers.google.com/projectcreate
.. _`enabling billing`: https://cloud.google.com/billing/docs/how-to/modify-project#enable_billing_for_a_project

Using the Python Client API
---------------------------

Once you have the API key, you can get started using the Data Commons Python
Client API. There are two ways to enable your key in the API package.

1.  You can set the API key by calling :py:func:`datacommons.utils.set_api_key`.
    Start by importing :code:`datacommons`, then set the API key like so.

    .. code-block:: python

       import datacommons as dc

       dc.set_api_key('YOUR-API-KEY')

    This will create an environment variable in your Python runtime called
    :code:`DC_API_KEY` holding your key. Your key will then be used whenever
    the package sends a request to the Data Commons graph.

2.  You can export an environment variable in your shell like so.

    .. code-block:: bash

       export DC_API_KEY='YOUR-API-KEY'

    After you've exported the variable, you can start using the Data Commons
    package!

    .. code-block:: python

       import datacommons as dc

    This route is particularly useful if you are building applications that
    depend on this API, and are deploying them to hosting services.

You're now ready to go! From here you can view our tutorials on how to use the
API to perform certain tasks, or see a full list of functions, classes and
methods available for use by navigating to `modules <modules.html>`_
