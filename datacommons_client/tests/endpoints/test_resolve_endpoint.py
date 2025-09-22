from unittest.mock import MagicMock

from datacommons_client.endpoints.base import API
from datacommons_client.endpoints.resolve import _resolve_correspondence_expression
from datacommons_client.endpoints.resolve import ResolveEndpoint
from datacommons_client.endpoints.response import ResolveResponse
from datacommons_client.models.resolve import Candidate
from datacommons_client.models.resolve import Entity


def test_fetch():
  """Tests the fetch method of ResolveEndpoint."""
  api_mock = MagicMock(spec=API)
  api_mock.post = MagicMock(return_value={})
  endpoint = ResolveEndpoint(api=api_mock)

  response = endpoint.fetch(node_ids="Node1", expression="some_expression")

  # Check the response
  assert isinstance(response, ResolveResponse)

  # Check the post request
  api_mock.post.assert_called_once_with(payload={
      "nodes": ["Node1"],
      "property": "some_expression",
  },
                                        endpoint="resolve",
                                        all_pages=True,
                                        next_token=None)


def test_fetch_dcid_by_name():
  """Tests the fetch_dcid_by_name method."""
  api_mock = MagicMock(spec=API)
  api_mock.post = MagicMock(return_value={})
  endpoint = ResolveEndpoint(api=api_mock)

  response = endpoint.fetch_dcids_by_name(names=["Entity1"],
                                          entity_type="Place")

  # Check the response
  assert isinstance(response, ResolveResponse)

  # Check the post request
  api_mock.post.assert_called_once_with(payload={
      "nodes": ["Entity1"],
      "property": "<-description{typeOf:Place}->dcid"
  },
                                        endpoint="resolve",
                                        all_pages=True,
                                        next_token=None)


def test_fetch_dcid_by_wikidata_id():
  """Tests the fetch_dcid_by_wikidata_id method."""
  api_mock = MagicMock(spec=API)
  api_mock.post = MagicMock(return_value={})
  endpoint = ResolveEndpoint(api=api_mock)

  response = endpoint.fetch_dcids_by_wikidata_id(wikidata_ids="Q12345",
                                                 entity_type="Country")

  # Check the response
  assert isinstance(response, ResolveResponse)

  # Check the post request
  api_mock.post.assert_called_once_with(payload={
      "nodes": ["Q12345"],
      "property": "<-wikidataId{typeOf:Country}->dcid",
  },
                                        endpoint="resolve",
                                        all_pages=True,
                                        next_token=None)


def test_fetch_dcids_list_by_wikidata_id():
  """Tests the fetch_dcid_by_wikidata_id method."""
  api_mock = MagicMock(spec=API)
  api_mock.post = MagicMock(return_value={})
  endpoint = ResolveEndpoint(api=api_mock)

  response = endpoint.fetch_dcids_by_wikidata_id(
      wikidata_ids=["Q12345", "Q695660"])

  # Check the response
  assert isinstance(response, ResolveResponse)

  # Check the post request
  api_mock.post.assert_called_once_with(payload={
      "nodes": ["Q12345", "Q695660"],
      "property": "<-wikidataId->dcid",
  },
                                        endpoint="resolve",
                                        all_pages=True,
                                        next_token=None)


def test_fetch_dcid_by_coordinates():
  """Tests the fetch_dcid_by_coordinates method."""
  api_mock = MagicMock(spec=API)
  api_mock.post = MagicMock(return_value={})
  endpoint = ResolveEndpoint(api=api_mock)

  response = endpoint.fetch_dcid_by_coordinates(latitude="37.7749",
                                                longitude="-122.4194",
                                                entity_type="City")

  # Check the response
  assert isinstance(response, ResolveResponse)

  # Check the post request
  api_mock.post.assert_called_once_with(payload={
      "nodes": ["37.7749#-122.4194"],
      "property": "<-geoCoordinate{typeOf:City}->dcid",
  },
                                        endpoint="resolve",
                                        all_pages=True,
                                        next_token=None)


def test_resolve_correspondence_expression():
  """Tests the resolve_correspondence_expression function."""
  expression = _resolve_correspondence_expression(from_type="description",
                                                  to_type="dcid",
                                                  entity_type="Place")
  assert expression == "<-description{typeOf:Place}->dcid"

  expression_no_entity_type = _resolve_correspondence_expression(
      from_type="description", to_type="dcid")
  assert expression_no_entity_type == "<-description->dcid"


def test_flatten_resolve_response():
  """Tests the flatten_resolve_response function."""
  # Mock ResolveResponse with multiple entities
  mock_data = ResolveResponse(entities=[
      Entity(node="Node1", candidates=[Candidate(dcid="Candidate1")]),
      Entity(node="Node2",
             candidates=[
                 Candidate(dcid="Candidate2"),
                 Candidate(dcid="Candidate3")
             ]),
      Entity(node="Node3", candidates=[])  # No candidates
  ])

  # Call the function
  result = mock_data.to_flat_dict()

  # Expected output
  expected = {
      "Node1": "Candidate1",  # Single candidate
      "Node2": ["Candidate2", "Candidate3"],  # Multiple candidates
      "Node3": [],  # No candidates
  }

  # Assertions
  assert result == expected
