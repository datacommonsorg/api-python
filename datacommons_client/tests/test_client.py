from unittest.mock import MagicMock
from unittest.mock import patch

import pandas as pd
import pytest

from datacommons_client.client import DataCommonsClient
from datacommons_client.endpoints.base import API
from datacommons_client.endpoints.node import NodeEndpoint
from datacommons_client.endpoints.observation import ObservationEndpoint
from datacommons_client.endpoints.resolve import ResolveEndpoint
from datacommons_client.models.node import Name
from datacommons_client.utils.error_handling import NoDataForPropertyError


@pytest.fixture
def mock_client():
  """Fixture to create a DataCommonsClient instance with a mocked API."""
  with patch("datacommons_client.endpoints.base.API.post") as mock_post:
    client = DataCommonsClient(api_key="test_key")
    client.observation = MagicMock(spec=ObservationEndpoint)
    return client


@patch(
    "datacommons_client.endpoints.base.resolve_instance_url",
    return_value="https://datacommons.org",
)
@patch(
    "datacommons_client.utils.request_handling.check_instance_is_valid",
    return_value="https://datacommons.org",
)
@patch(
    "datacommons_client.utils.request_handling.build_headers",
    return_value={"X-API-Key": "test_key"},
)
def test_datacommons_client_initialization(mock_build_headers,
                                           mock_check_instance,
                                           mock_resolve_instance_url):
  """Tests that DataCommonsClient initializes correctly with API and endpoints, using a fake address."""
  client = DataCommonsClient(api_key="test_key", dc_instance="test_instance")

  assert isinstance(client.api, API)
  assert client.api.headers == {
      "Content-Type": "application/json",
      "X-API-Key": "test_key",
  }

  assert isinstance(client.node, NodeEndpoint)
  assert isinstance(client.observation, ObservationEndpoint)
  assert isinstance(client.resolve, ResolveEndpoint)

  assert client.node.api is client.api
  assert client.observation.api is client.api
  assert client.resolve.api is client.api


def test_datacommons_client_raises_error_when_both_url_and_instance_are_provided(
):
  """Tests that DataCommonsClient raises a ValueError when both `dc_instance` and `url` are given."""
  with pytest.raises(ValueError,
                     match="Cannot provide both `dc_instance` and `url`."):
    DataCommonsClient(api_key="test_key",
                      dc_instance="test_instance",
                      url="https://test.url")


def test_observations_dataframe_raises_error_when_entities_all_but_no_entity_type(
    mock_client,):
  """Tests that ValueError is raised if 'entities' is 'all' but 'entity_type' is not specified."""
  with pytest.raises(
      ValueError,
      match="When 'entity_dcids' is 'all', 'entity_type' must be specified.",
  ):
    mock_client.observations_dataframe(variable_dcids="var1",
                                       date="2024",
                                       entity_dcids="all")


def test_observations_dataframe_raises_error_when_invalid_entity_type_usage(
    mock_client,):
  """Tests that ValueError is raised if 'entity_type' or 'parent_entity' is specified with specific entities."""
  with pytest.raises(
      ValueError,
      match="Specify 'entity_type' and 'parent_entity'"
      " only when 'entity_dcids' is 'all'.",
  ):
    mock_client.observations_dataframe(
        variable_dcids="var1",
        date="2024",
        entity_dcids=["entity1"],
        entity_type="Country",
    )


def test_observations_dataframe_calls_fetch_observations_by_entity_type(
    mock_client):
  """Tests that fetch_observations_by_entity_type is called with correct parameters."""
  mock_client.observation.fetch_observations_by_entity_type.return_value.to_observation_records.return_value = (
      [])

  df = mock_client.observations_dataframe(
      variable_dcids=["var1", "var2"],
      date="2024",
      entity_dcids="all",
      entity_type="Country",
      parent_entity="Earth",
  )

  mock_client.observation.fetch_observations_by_entity_type.assert_called_once_with(
      date="2024",
      variable_dcids=["var1", "var2"],
      entity_type="Country",
      parent_entity="Earth",
      filter_facet_ids=None,
  )

  assert isinstance(df, pd.DataFrame)
  assert df.empty


def test_observations_dataframe_calls_fetch_observations_by_entity(mock_client):
  """Tests that fetch_observations_by_entity is called with correct parameters."""

  mock_client.observation.fetch_observations_by_entity_dcid.return_value.to_observation_records.return_value = (
      [])

  df = mock_client.observations_dataframe(variable_dcids="var1",
                                          date="latest",
                                          entity_dcids=["entity1", "entity2"])

  mock_client.observation.fetch_observations_by_entity_dcid.assert_called_once_with(
      date="latest",
      entity_dcids=["entity1", "entity2"],
      variable_dcids="var1",
      filter_facet_ids=None,
  )

  assert isinstance(df, pd.DataFrame)
  assert df.empty


def test_observations_dataframe_returns_dataframe_with_expected_columns(
    mock_client):
  """Tests that the method returns a DataFrame with expected columns."""
  mock_client.observation.fetch_observations_by_entity_dcid.return_value.to_observation_records.return_value = [
      {
          "date": "2024",
          "entity": "entity1",
          "variable": "var1",
          "value": 100,
          "unit": "unit1",
      },
      {
          "date": "2024",
          "entity": "entity2",
          "variable": "var2",
          "value": 200,
          "unit": "unit2",
      },
  ]

  # Mock entity name lookup to prevent API calls
  mock_client.node.fetch_entity_names = MagicMock(return_value={
      "entity1": Name(value="Entity One", language="en", property="name"),
      "entity2": Name(value="Entity Two", language="en", property="name"),
      "var1": Name(value="Variable One", language="en", property="name"),
      "var2": Name(value="Variable Two", language="en", property="name"),
  },)

  df = mock_client.observations_dataframe(variable_dcids="var1",
                                          date="2024",
                                          entity_dcids=["entity1", "entity2"])

  assert isinstance(df, pd.DataFrame)
  assert set(df.columns) == {
      "date", "entity", "entity_name", "variable", "variable_name", "value",
      "unit"
  }
  assert len(df) == 2
  assert df.iloc[0]["entity"] == "entity1"
  assert df.iloc[0]["entity_name"] == "Entity One"
  assert df.iloc[1]["entity"] == "entity2"
  assert df.iloc[1]["entity_name"] == "Entity Two"
  assert df.iloc[0]["variable"] == "var1"
  assert df.iloc[0]["variable_name"] == "Variable One"
  assert df.iloc[0]["value"] == 100
  assert df.iloc[0]["unit"] == "unit1"
  assert df.iloc[1]["variable"] == "var2"
  assert df.iloc[1]["variable_name"] == "Variable Two"
  assert df.iloc[1]["value"] == 200
  assert df.iloc[1]["unit"] == "unit2"


@patch(
    "datacommons_client.endpoints.base.check_instance_is_valid",
    return_value="https://test.url",
)
def test_dc_instance_is_ignored_when_url_is_provided(mock_check_instance):
  """Tests that dc_instance is ignored when a fully resolved URL is provided."""

  client = DataCommonsClient(api_key="test_key", url="https://test.url")

  # Check that the API base_url is set to the fully resolved url
  assert client.api.base_url == "https://test.url"


def test_find_filter_facet_ids_returns_none_when_no_filters(mock_client):
  """Tests that _find_filter_facet_ids returns None when no filters are provided."""
  result = mock_client._find_filter_facet_ids(fetch_by="entity",
                                              date="2024",
                                              variable_dcids="var1",
                                              property_filters=None)
  assert result is None


def test_find_filter_facet_ids_returns_facet_ids(mock_client):
  """Tests that _find_filter_facet_ids correctly returns facet IDs when filters are provided."""
  mock_client.observation.fetch_observations_by_entity_dcid.return_value.find_matching_facet_id.side_effect = [
      ["213"], ["3243"]
  ]

  result = mock_client._find_filter_facet_ids(
      fetch_by="entity",
      date="2024",
      variable_dcids="var1",
      property_filters={
          "measurementMethod": "Census",
          "unit": "USD"
      },
  )

  assert set(result) == {"213", "3243"}


def test_observations_dataframe_filters_by_facet_ids(mock_client):
  """Tests that observations_dataframe includes facet filtering when property_filters are used."""
  mock_client._find_filter_facet_ids = MagicMock(
      return_value=["facet_1", "facet_2"])

  mock_client.observation.fetch_observations_by_entity_dcid.return_value.to_observation_records.return_value = (
      [])

  df = mock_client.observations_dataframe(
      variable_dcids="var1",
      date="2024",
      entity_dcids=["entity1"],
      property_filters={"measurementMethod": "Census"},
  )

  mock_client.observation.fetch_observations_by_entity_dcid.assert_called_once_with(
      variable_dcids="var1",
      date="2024",
      entity_dcids=["entity1"],
      filter_facet_ids=["facet_1", "facet_2"],
  )
  assert isinstance(df, pd.DataFrame)


def test_observations_dataframe_raises_error_when_no_facet_match(mock_client):
  """Tests that observations_dataframe raises NoDataForPropertyError when no facets match the filters."""
  mock_client._find_filter_facet_ids = MagicMock(return_value=None)

  with pytest.raises(NoDataForPropertyError):
    mock_client.observations_dataframe(
        variable_dcids="var1",
        date="2024",
        entity_dcids=["entity1"],
        property_filters={"measurementMethod": "Nonexistent"},
    )

  mock_client._find_filter_facet_ids = MagicMock(return_value=[])

  with pytest.raises(NoDataForPropertyError):
    mock_client.observations_dataframe(
        variable_dcids="var2",
        date="2024",
        entity_dcids=["entity1"],
        property_filters={"measurementMethodX": "Nonexistent"},
    )
