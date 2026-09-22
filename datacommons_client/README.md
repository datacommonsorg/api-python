# Data Commons Python API

This is a Python library for accessing data in the Data Commons Graph via the V2 REST API (`node`, `observation`, `resolve`) and the SDMX 3.0 REST API (`data`, `availability`).

To get started, install this package from pip.

```bash
pip install datacommons-client
```

To get additional functionality to work with Pandas DataFrames, install the package
with the optional Pandas dependency.

```bash
pip install "datacommons-client[Pandas]"
```

Once the package is installed, import `datacommons_client` and initialize `DataCommonsClient`:

```python
from datacommons_client import DataCommonsClient

client = DataCommonsClient(api_key="YOUR_API_KEY")

# V2 Observation query
observations = client.observation.fetch(
    variable_dcids="Count_Person",
    entity_dcids=["country/USA"],
)

# SDMX 3.0 Data query (returns SDMX-CSV string, or use fetch_data_as_dataframe for a Pandas DataFrame)
csv_data = client.sdmx.fetch_data(
    variable="Count_Person",
    constraints={"observationAbout": "country/USA"},
)
df = client.sdmx.fetch_data_as_dataframe(
    variable="Count_Person",
    constraints={"observationAbout": "country/USA"},
)

# SDMX 3.0 Availability query (returns parsed SDMX-JSON, or use fetch_available_values for {component_id: [values]})
availability = client.sdmx.fetch_availability(
    component_id="provenance",
    variable="Count_Person",
)
available_values = client.sdmx.fetch_available_values(
    component_id="provenance",
    variable="Count_Person",
)
```

## Connecting to a Private Data Commons Platform Instance

To query a private [Data Commons Platform](https://github.com/datacommonsorg/datacommons) instance protected by IAM (such as an authenticated Cloud Run service), pass the instance URL along with your `Authorization` header and set `validate_instance=False`:

```python
from datacommons_client import DataCommonsClient

client = DataCommonsClient(
    url="https://your-dcp-service-url.a.run.app/core/api/v2",
    headers={"Authorization": f"Bearer {id_token}"},
    validate_instance=False,
)

# Query V2 or SDMX 3.0 endpoints on the private instance
node_data = client.node.fetch(node_dcids="FinancialTrade", expression="->name")
sdmx_csv = client.sdmx.fetch_data(
    variable="FinancialTrade",
    constraints={"sourceCountry": "country/FRA"},
)
```

For more detail on getting started with the API, please visit <https://docs.datacommons.org/api/python/v2/>.


