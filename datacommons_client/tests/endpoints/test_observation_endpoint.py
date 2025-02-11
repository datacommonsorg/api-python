from unittest.mock import MagicMock

from datacommons_client.endpoints.base import API
from datacommons_client.endpoints.observation import ObservationEndpoint
from datacommons_client.endpoints.payloads import ObservationDate
from datacommons_client.endpoints.response import ObservationResponse


def test_fetch():
  """Tests the fetch method of ObservationEndpoint."""
  api_mock = MagicMock(spec=API)
  endpoint = ObservationEndpoint(api=api_mock)

  response = endpoint.fetch(
      variable_dcids="dcid/variableID",
      date=ObservationDate.LATEST,
      select=["date", "variable", "entity", "value"],
      entity_dcids="dc/EntityID",
  )

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
  },
                                        endpoint="observation",
                                        all_pages=True,
                                        next_token=None)


def test_fetch_latest_observation():
  """Tests the fetch_latest_observation method."""
  api_mock = MagicMock(spec=API)
  endpoint = ObservationEndpoint(api=api_mock)

  response = endpoint.fetch_latest_observations(
      variable_dcids=["dc/Variable1", "dc/Variable2"],
      entity_dcids="dc/EntityID")

  # Check the response
  assert isinstance(response, ObservationResponse)

  # Check the post request
  api_mock.post.assert_called_once_with(
      payload={
          "variable": {
              "dcids": ["dc/Variable1", "dc/Variable2"]
          },
          "date": ObservationDate.LATEST,
          "entity": {
              "dcids": ["dc/EntityID"]
          },
          "select": ["date", "variable", "entity", "value"],  # Default select
      },
      endpoint="observation",
      all_pages=True,
      next_token=None)


def test_fetch_latest_observations_by_entity():
  """Tests the fetch_latest_observations_by_entity method."""
  api_mock = MagicMock(spec=API)
  endpoint = ObservationEndpoint(api=api_mock)

  response = endpoint.fetch_latest_observations_by_entity(
      variable_dcids="dc/VariableID",
      entity_dcids=["dc/Entity1", "dc/Entity2"],
  )

  # Check the response
  assert isinstance(response, ObservationResponse)

  # Check the post request
  api_mock.post.assert_called_once_with(payload={
      "variable": {
          "dcids": ["dc/VariableID"]
      },
      "date": ObservationDate.LATEST,
      "entity": {
          "dcids": ["dc/Entity1", "dc/Entity2"]
      },
      "select": ["date", "variable", "entity", "value"],
  },
                                        endpoint="observation",
                                        all_pages=True,
                                        next_token=None)


def test_fetch_observations_by_entity_type():
  """Tests the fetch_observations_by_entity_type method."""
  api_mock = MagicMock(spec=API)
  endpoint = ObservationEndpoint(api=api_mock)

  response = endpoint.fetch_observations_by_entity_type(
      date="2023",
      parent_entity="Earth",
      entity_type="Country",
      variable_dcids="dc/VariableID",
  )

  # Check the response
  assert isinstance(response, ObservationResponse)

  # Check the post request
  api_mock.post.assert_called_once_with(payload={
      "variable": {
          "dcids": ["dc/VariableID"]
      },
      "date": "2023",
      "entity": {
          "expression": "Earth<-containedInPlace+{typeOf:Country}"
      },
      "select": ["date", "variable", "entity", "value"],
  },
                                        endpoint="observation",
                                        all_pages=True,
                                        next_token=None)
