# Data Commons Python API

This is a Python library for accessing data in the Data Commons Graph.
To get started, install this package from pip.

    pip install datacommons

Once the package is installed, import `datacommons`.

    import datacommons as dc

If you would like to provide an API key, follow the steps in
[Setting up access to the Data Commons API](https://docs.datacommons.org/api/setup.html),
add the following line to your code:

    dc.set_api_key('YOUR-API-KEY')

Data Commons *does not charge* users, but uses the API key for
understanding API usage.

For more detail on getting started with the API, please visit our
[API Overview](http://docs.datacommons.org/api/).

When you are ready to use the API, you can refer to `datacommons/examples` for
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

The Python API currently supports `python>=2.7`.

To test, run:

```
$ ./run_tests_local.sh
```

To debug the continuous integration tests, run:

```
$ cloud-build-local --config=cloudbuild.yaml --dryrun=false .
```

Both commands will run the same set of tests.

To run the examples:

```
$ python -m datacommons.examples.XXX
```

where XXX is the module you want to run.

## Release to PyPI

- Update "VERSION" in setup.py
- Update CHANGELOG.md for a new version
- Upload a new package using steps for [generating distribution archives](https://packaging.python.org/tutorials/packaging-projects/#generating-distribution-archives) and [uploading the distribution archives](https://packaging.python.org/tutorials/packaging-projects/#uploading-the-distribution-archives)

## Support

For general questions or issues about the API, please open an issue on our
[issues](https://github.com/google/datacommons/issues) page. For all other
questions, please send an email to `support@datacommons.org`.

**Note** - This is not an officially supported Google product.
