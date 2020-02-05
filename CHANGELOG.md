# Changelog

## 1.0.9

**Date** - 02/04/2020

**Release Tag** - [v1.0.9](https://github.com/datacommonsorg/api-python/releases/tag/v1.0.9)

**Release Status** - Current head of branch [`master`](https://github.com/datacommonsorg/api-python/tree/master)

New features added to the Python Client API

-   Use six package for urllib.

## 1.0.7

**Date** - 02/04/2020

**Release Tag** - [v1.0.7](https://github.com/datacommonsorg/api-python/releases/tag/v1.0.7)

**Release Status** - Current head of branch [`master`](https://github.com/datacommonsorg/api-python/tree/master)

New features added to the Python Client API

-   Support python 2.7.

## 1.0.6

**Date** - 01/29/2020

**Release Tag** - [v1.0.6](https://github.com/datacommonsorg/api-python/releases/tag/v1.0.6)

**Release Status** - Current head of branch [`master`](https://github.com/datacommonsorg/api-python/tree/master)

New features added to the Python Client API

-   Add a new API for getting related places.



## 1.0.5

**Date** - 01/27/2020

**Release Tag** - [v1.0.5](https://github.com/datacommonsorg/api-python/releases/tag/v1.0.5)

**Release Status** - Current head of branch [`master`](https://github.com/datacommonsorg/api-python/tree/master)

New features added to the Python Client API

-   Remove the dependency on Pandas and Numpy in package dependency.
-   Replace requests with urllib.


## 1.0.2

**Date** - 11/6/2019

**Release Tag** - [v1.0.2](https://github.com/datacommonsorg/api-python/releases/tag/v1.0.2)

**Release Status** - Current head of branch [`master`](https://github.com/datacommonsorg/api-python/tree/master)

New features added to the Python Client API

-   Remove the dependency on Pandas.


## 1.0.1

**Date** - 10/2/2019

**Release Tag** - [v1.0.1](https://github.com/datacommonsorg/api-python/releases/tag/v1.0.1)

**Release Status** - Current head of branch [`stable-1.x`](https://github.com/datacommonsorg/api-python/tree/stable-1.x)

New features added to the Python Client API

-   Added two new functions `get_pop_obs` and `get_place_obs`
-   SPARQL query is now supported as a function `query` instead of a class.
-   Added documentation on how to provision an API key and provide it to the API

Bugs fixed in new release

-   Fixed various typos and formatting issues in the documentation.
-   If the index of the `pandas.Series` passed into functions such as `get_populations` and `get_observations` was not contiguous, then the assignment step would not properly align the values returned by calling the function. This is because the `pandas.Series` returned by the function would have a different index than the given series. This is fixed by assigning the index of the returned series to that of the given series.

## 1.0.0

**Date** - 8/9/2019

**Release Tag** - [v1.0.0](https://github.com/datacommonsorg/api-python/releases/tag/v1.0.0)

New release of the Python Client API.

-   New functions in the API built on top of the [Data Commons REST API](https://github.com/datacommonsorg/mixer).
    -   `get_property_labels`
    -   `get_property_values`
    -   `get_triples`
    -   `get_populations`
    -   `get_observations`
    -   `get_places_in`
-   New tests and examples checked into `datacommons/test` and `datacommons/examples`
-   Full documentation released on [readthedocs](https://datacommons.readthedocs.io/en/latest/)

## 0.4.3

**Date** - 8/13/2019

**Release Tag** - [v0.4.3](https://github.com/datacommonsorg/api-python/releases/tag/v0.4.3)

**Release Status** - Latest on [PyPI](https://pypi.org/project/datacommons/). Current head of branch [`stable-0.x`](https://github.com/datacommonsorg/api-python/tree/stable-0.x).

Patch release that fixes bugs in `datacommons.Client`.

-   Functions `get_cities` and `get_states` now provides `typeOf` constraints in their datalog queries.

## 0.x

Initial release of the Data Commons API.
