# Changelog

## 1.0.0

**Date** - 8/9/2019

**Release Tag** - [v1.0.0](https://github.com/datacommonsorg/api-python/releases/tag/v1.0.0)

**Release Status** - Current head of branch [`stable-1.x`](https://github.com/datacommonsorg/api-python/tree/stable-1.x)

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
