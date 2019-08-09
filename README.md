# Data Commons Python Client API

This is a Python library for accessing data in the Data Commons knowledge graph.
To get started, install this package from pip.

    pip install git+https://github.com/google/datacommons.git@stable-1.x

Once the package is installed, import `datacommons`.

    import datacommons as dc

You will also need to provision an API key by enabling the Data Commons API on
GCP. Once you have the API key, simply add the following line to your code.

    dc.set_api_key('YOUR-API-KEY')

For more detail on getting started with the API, please visit
[Getting Started](https://datacommons.readthedocs.io/en/latest/started.html).

After you're ready to use the API, you can refer to `datacommons/examples` for
examples on how to use this package to perform various tasks. More tutorials and
documentation can be found at our
[readthedocs](https://datacommons.readthedocs.io/en/latest/)!

## About Data Commons

[Data Commons](https://datacommons.org/) is an Open Knowledge Graph that
provides a unified view across multiple public data sets and statistics. You can
view what [datasets](https://datacommons.org/datasets) are currently ingested
and browse the graph using our [browser](https://browser.datacommons.org/).

## License

Apache 2.0

## Development

The Python Client API currently supports `python>=3.6`. We use
[bazel](https://bazel.build/) as our build system. To test, first install
bazel then run the following:

```
$ bazel build //...
$ bazel test //...
```

## Support

For general questions or issues about the API, please open an issue on our
[issues](https://github.com/google/datacommons/issues) page. For all other
questions, please send an email to `support@datacommons.org`.

**Note** - This is not an officially supported Google product.
