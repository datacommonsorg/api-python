from unittest.mock import MagicMock

from datacommons_client.endpoints.base import API
from datacommons_client.endpoints.observation import ObservationEndpoint
from datacommons_client.endpoints.response import ObservationResponse
from datacommons_client.models.observation import ByVariable
from datacommons_client.models.observation import ObservationDate
from datacommons_client.models.observation import ObservationSelect


def test_fetch():
  """Tests the fetch method of ObservationEndpoint."""
  api_mock = MagicMock(spec=API)
  api_mock.post.return_value = {"byVariable": {}}
  endpoint = ObservationEndpoint(api=api_mock)

  response = endpoint.fetch(variable_dcids="dcid/variableID",
                            date=ObservationDate.LATEST,
                            select=["date", "variable", "entity", "value"],
                            entity_dcids="dc/EntityID",
                            filter_facet_domains="domain1",
                            filter_facet_ids="facet1")

  # Check the response
  assert isinstance(response, ObservationResponse)

  # Check the post request
  api_mock.post.assert_called_once_with(payload={
      "date": ObservationDate.LATEST,
      "variable": {
          "dcids": ["dcid/variableID"]
      },
      "entity": {
          "dcids": ["dc/EntityID"],
      },
      "select": ["date", "variable", "entity", "value"],
      "filter": {
          "domains": ["domain1"],
          "facet_ids": ["facet1"]
      }
  },
                                        endpoint="observation",
                                        all_pages=True,
                                        next_token=None)


def test_fetch_observations_by_entity_type():
  """Tests the fetch_observations_by_entity_type method."""
  api_mock = MagicMock(spec=API)
  api_mock.post.return_value = {"byVariable": {}}
  endpoint = ObservationEndpoint(api=api_mock)

  response = endpoint.fetch_observations_by_entity_type(
      date="2023",
      parent_entity="Earth",
      entity_type="Country",
      select=["variable", "entity", "facet"],
      variable_dcids="dc/VariableID")

  # Check the response
  assert isinstance(response, ObservationResponse)

  # Check the post request
  api_mock.post.assert_called_once_with(payload={
      "date": "2023",
      "variable": {
          "dcids": ["dc/VariableID"]
      },
      "entity": {
          "expression": "Earth<-containedInPlace+{typeOf:Country}"
      },
      "select": ["variable", "entity", "facet"],
  },
                                        endpoint="observation",
                                        all_pages=True,
                                        next_token=None)


def test_fetch_observations_facets_by_entity_type():
  """Tests the fetch_observations_by_entity_type method."""
  api_mock = MagicMock(spec=API)
  api_mock.post.return_value = {"byVariable": {}}
  endpoint = ObservationEndpoint(api=api_mock)

  response = endpoint.fetch_observations_by_entity_type(
      date="2023",
      parent_entity="Earth",
      entity_type="Country",
      variable_dcids="dc/VariableID",
      select=["variable", "entity", "facet"],
  )

  # Check the response
  assert isinstance(response, ObservationResponse)

  # Check the post request
  api_mock.post.assert_called_once_with(payload={
      "date": "2023",
      "variable": {
          "dcids": ["dc/VariableID"]
      },
      "entity": {
          "expression": "Earth<-containedInPlace+{typeOf:Country}"
      },
      "select": ["variable", "entity", "facet"],
  },
                                        endpoint="observation",
                                        all_pages=True,
                                        next_token=None)


def test_fetch_available_statistical_variables_single_entity():
  """Test fetching variables for a single entity."""
  mock_data = {
      "var1": ["ent1"],
      "var2": ["ent1"],
  }

  # Mock the fetch method on the ObservationEndpoint instance
  endpoint = ObservationEndpoint(api=MagicMock())
  endpoint.fetch = MagicMock()
  endpoint.fetch.return_value.get_data_by_entity = MagicMock(
      return_value=mock_data)

  result = endpoint.fetch_available_statistical_variables("ent1")

  expected = {
      "ent1": ["var1", "var2"],
  }
  assert result == expected

  endpoint.fetch.assert_called_once_with(
      entity_dcids="ent1",
      select=[ObservationSelect.VARIABLE, ObservationSelect.ENTITY],
      variable_dcids=[])


def test_fetch_available_statistical_variables_multiple_entities():
  """Test fetching variables for multiple entities."""
  mock_data = {
      "var1": ["ent1", "ent2"],
      "var2": ["ent2"],
  }

  endpoint = ObservationEndpoint(api=MagicMock())
  endpoint.fetch = MagicMock()
  endpoint.fetch.return_value.get_data_by_entity = MagicMock(
      return_value=mock_data)

  result = endpoint.fetch_available_statistical_variables(["ent1", "ent2"])

  expected = {
      "ent1": ["var1"],
      "ent2": ["var1", "var2"],
  }
  assert result == expected
