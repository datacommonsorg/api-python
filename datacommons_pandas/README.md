# Data Commons Pandas API

This is a Python library for creating pandas objects with data in the
Data Commons Graph.

To get started, install this package from pip.

    pip install datacommons_pandas

Once the package is installed, import `datacommons_pandas`.

    import datacommons_pandas as dcpd

For more detail on getting started with the API, please visit our
[API Overview](http://docs.datacommons.org/api/).

When you are ready to use the API, you can refer to `datacommons_pandas/examples` for
examples on how to use this package to perform various tasks. More tutorials and
documentation can be found on our [tutorials page](https://datacommons.org/colab)!

## About Data Commons

[Data Commons](https://datacommons.org/) is an open knowledge repository that
provides a unified view across multiple public data sets and statistics. You can
view what [datasets](https://datacommons.org/datasets) are currently ingested
and browse the graph using our [browser](https://browser.datacommons.org/).

## License

Apache 2.0

## Development

Please follow the [Development instructions](../README.md#development).

## Release to PyPI

- Update "VERSION" in [setup_datacommons_pandas.py](../setup_datacommons_pandas.py)
- Update [CHANGELOG.md](CHANGELOG.md) for a new version
- Upload a new package using steps for [generating distribution archives](https://packaging.python.org/tutorials/packaging-projects/#generating-distribution-archives) and [uploading the distribution archives](https://packaging.python.org/tutorials/packaging-projects/#uploading-the-distribution-archives)

### To test on TestPyPI

Similar to regular PyPI release, except:

1. Append "-USERNAME" to the package NAME. For example,
`NAME = 'datacommons_pandas-foobar'`.
1. Increment the version code to something that has not been used in your test
  project. This will not affect the production PyPI versioning.

Here are some helpful commands:
- Build the dist
  ```
  python3 -m pip install --user --upgrade setuptools wheel
  python3 ../setup_datacommons_pandas.py sdist bdist_wheel
  ```
- Release the dist to TestPyPI.
  ```
  python3 -m pip install --user --upgrade twine
  python3 -m twine upload --repository testpypi ../dist/*
  ```

## Support

For general questions or issues about the API, please open an issue on our
[issues](https://github.com/datacommonsorg/api-python/issues) page. For all other
questions, please send an email to `support@datacommons.org`.

**Note** - This is not an officially supported Google product.
