# Data Commons Python Client API

This is a Python library for accessing data in the Data Commons knowledge graph.
To get started, install this package from pip.

    pip install git+https://github.com/google/datacommons.git@stable-1.x

Once the package is installed, import `datacommons` and you're ready to go!

    import datacommons as dc

Please take a look at `datacommons/examples` for examples on how to use this
package to access the Data Commons knowledge graph.

To access more tutorials and documentation on how to use our package, please
refer to our [readthedocs](https://datacommons.readthedocs.io/en/latest/).

## Licence

Apache 2.0

## Development

We use [bazel](https://bazel.build/) as our build system. To test, first install
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
