from unittest.mock import MagicMock
from unittest.mock import patch

from datacommons_client.endpoints.base import API
from datacommons_client.endpoints.resolve import flatten_resolve_response
from datacommons_client.endpoints.resolve import \
    resolve_correspondence_expression
from datacommons_client.endpoints.resolve import ResolveEndpoint
from datacommons_client.endpoints.response import ResolveResponse


@patch(
    "datacommons_client.endpoints.base.check_instance_is_valid",
    return_value="https://custom.api/v2",
)
@patch(
    "datacommons_client.endpoints.base.post_request",
    return_value={"entities": []},
)
def test_fetch(mock_post_request, mock_check_instance_is_valid):
  """Tests the fetch method of ResolveEndpoint."""
  api = API(url="https://custom.api/v2")
  endpoint = ResolveEndpoint(api=api)

  response = endpoint.fetch(node_dcids="Node1", expression="some_expression")

  # Check the response
  assert isinstance(response, ResolveResponse)

  # Check the post request
  mock_post_request.assert_called_once_with(
      url="https://custom.api/v2/resolve",
      payload={
          "nodes": ["Node1"],
          "property": "some_expression",
      },
      headers=api.headers,
      max_pages=None,
  )


@patch(
    "datacommons_client.endpoints.base.check_instance_is_valid",
    return_value="https://custom.api/v2",
)
@patch(
    "datacommons_client.endpoints.base.post_request",
    return_value={"entities": []},
)
def test_fetch_dcid_by_name(mock_post_request, mock_check_instance_is_valid):
  """Tests the fetch_dcid_by_name method."""
  api = API(url="https://custom.api/v2")
  endpoint = ResolveEndpoint(api=api)

  response = endpoint.fetch_dcid_by_name(names=["Entity1"], entity_type="Place")

  # Check the response
  assert isinstance(response, ResolveResponse)

  # Check the post request
  mock_post_request.assert_called_once_with(
      url="https://custom.api/v2/resolve",
      payload={
          "nodes": ["Entity1"],
          "property": "<-description{typeOf:Place}->dcid",
      },
      headers=api.headers,
      max_pages=None,
  )


@patch(
    "datacommons_client.endpoints.base.check_instance_is_valid",
    return_value="https://custom.api/v2",
)
@patch(
    "datacommons_client.endpoints.base.post_request",
    return_value={"entities": []},
)
def test_fetch_dcid_by_wikidata_id(mock_post_request,
                                   mock_check_instance_is_valid):
  """Tests the fetch_dcid_by_wikidata_id method."""
  api = API(url="https://custom.api/v2")
  endpoint = ResolveEndpoint(api=api)

  response = endpoint.fetch_dcid_by_wikidata_id(wikidata_id="Q12345",
                                                entity_type="Country")

  # Check the response
  assert isinstance(response, ResolveResponse)

  # Check the post request
  mock_post_request.assert_called_once_with(
      url="https://custom.api/v2/resolve",
      payload={
          "nodes": ["Q12345"],
          "property": "<-wikidataId{typeOf:Country}->dcid",
      },
      headers=api.headers,
      max_pages=None,
  )


@patch(
    "datacommons_client.endpoints.base.check_instance_is_valid",
    return_value="https://custom.api/v2",
)
@patch(
    "datacommons_client.endpoints.base.post_request",
    return_value={"entities": []},
)
def test_fetch_dcid_by_coordinates(mock_post_request,
                                   mock_check_instance_is_valid):
  """Tests the fetch_dcid_by_coordinates method."""
  api = API(url="https://custom.api/v2")
  endpoint = ResolveEndpoint(api=api)

  response = endpoint.fetch_dcid_by_coordinates(latitude="37.7749",
                                                longitude="-122.4194",
                                                entity_type="City")

  # Check the response
  assert isinstance(response, ResolveResponse)

  # Check the post request
  mock_post_request.assert_called_once_with(
      url="https://custom.api/v2/resolve",
      payload={
          "nodes": ["37.7749#-122.4194"],
          "property": "<-geoCoordinate{typeOf:City}->dcid",
      },
      headers=api.headers,
      max_pages=None,
  )


@patch(
    "datacommons_client.endpoints.base.check_instance_is_valid",
    return_value="https://custom.api/v2",
)
@patch(
    "datacommons_client.endpoints.base.post_request",
    return_value={"entities": []},
)
def test_fetch_from_type_to_type(mock_post_request,
                                 mock_check_instance_is_valid):
  """Tests the fetch_from_type_to_type method."""
  api = API(url="https://custom.api/v2")
  endpoint = ResolveEndpoint(api=api)

  response = endpoint.fetch_from_type_to_type(
      entities="Node1",
      from_type="type1",
      to_type="type2",
      entity_type="Place",
  )

  # Check the response
  assert isinstance(response, ResolveResponse)

  # Check the post request
  mock_post_request.assert_called_once_with(
      url="https://custom.api/v2/resolve",
      payload={
          "nodes": ["Node1"],
          "property": "<-type1{typeOf:Place}->type2",
      },
      headers=api.headers,
      max_pages=None,
  )


def test_resolve_correspondence_expression():
  """Tests the resolve_correspondence_expression function."""
  expression = resolve_correspondence_expression(from_type="description",
                                                 to_type="dcid",
                                                 entity_type="Place")
  assert expression == "<-description{typeOf:Place}->dcid"

  expression_no_entity_type = resolve_correspondence_expression(
      from_type="description", to_type="dcid")
  assert expression_no_entity_type == "<-description->dcid"


def test_flatten_resolve_response():
  """Tests the flatten_resolve_response function."""
  # Mock ResolveResponse with multiple entities
  mock_data = ResolveResponse(entities=[
      MagicMock(node="Node1", candidates=[MagicMock(dcid="Candidate1")]),
      MagicMock(
          node="Node2",
          candidates=[
              MagicMock(dcid="Candidate2"),
              MagicMock(dcid="Candidate3"),
          ],
      ),
      MagicMock(node="Node3", candidates=[]),  # No candidates
  ])

  # Call the function
  result = flatten_resolve_response(mock_data)

  # Expected output
  expected = {
      "Node1": "Candidate1",  # Single candidate
      "Node2": ["Candidate2", "Candidate3"],  # Multiple candidates
      "Node3": [],  # No candidates
  }

  # Assertions
  assert result == expected
