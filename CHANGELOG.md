# Changelog

## 1.0.0

Status of release:

-   Released at tag [v1.0.0](https://github.com/datacommonsorg/api-python/releases/tag/v1.0.0).
-   Current head of branch [`stable-1.x`](https://github.com/datacommonsorg/api-python/tree/stable-1.x)

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

Status of release:

-   Released at tag [v0.4.3](https://github.com/datacommonsorg/api-python/releases/tag/v0.4.3).
-   Current head of branch [`stable-0.x`](https://github.com/datacommonsorg/api-python/tree/stable-0.x).
-   Current release on [PyPI](https://pypi.org/project/datacommons/).

Patch release that fixes bugs in `datacommons.Client`.

-   Functions `get_cities` and `get_states` now provides `typeOf` constraints in their datalog queries.
