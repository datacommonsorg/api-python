# Data Commons Python API

This is a Python library for accessing data in the Data Commons Graph.

To get started, install this package from pip.

```bash
pip install datacommons-client
```

To get additional functionality to work with Pandas DataFrames, install the package
with the optional Pandas dependency.

```bash
pip install "datacommons-client[Pandas]"
```

Once the package is installed, import `datacommons_client`.

```python
import datacommons_client as dc
```

For more detail on getting started with the API, please visit our
[API Overview](https://docs.datacommons.org/api/).

**Note:** This package uses the V2 REST API and is in Beta. All of the existing Python and Pandas tutorials and samples use V1. We will be updating all the documentation during this Beta.

## About Data Commons

[Data Commons](https://datacommons.org/) is an open knowledge repository that
provides a unified view across multiple public data sets and statistics. You can
view what [datasets](https://datacommons.org/datasets) are currently ingested
and browse the graph using our [browser](https://datacommons.org/browser).

## License

Apache 2.0

## Support

For general questions or issues about the API, please open an issue on our
[issues](https://github.com/google/datacommons/issues) page. For all other
questions, please send an email to `support@datacommons.org`.
