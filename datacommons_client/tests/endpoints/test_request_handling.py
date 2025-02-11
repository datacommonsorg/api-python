from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
import requests

from datacommons_client.utils.error_handling import APIError
from datacommons_client.utils.error_handling import DCAuthenticationError
from datacommons_client.utils.error_handling import DCConnectionError
from datacommons_client.utils.error_handling import DCStatusError
from datacommons_client.utils.error_handling import InvalidDCInstanceError
from datacommons_client.utils.request_handling import _fetch_with_pagination
from datacommons_client.utils.request_handling import _merge_values
from datacommons_client.utils.request_handling import _recursively_merge_dicts
from datacommons_client.utils.request_handling import _send_post_request
from datacommons_client.utils.request_handling import build_headers
from datacommons_client.utils.request_handling import check_instance_is_valid
from datacommons_client.utils.request_handling import post_request
from datacommons_client.utils.request_handling import resolve_instance_url


def test_resolve_instance_url_default():
  """Tests resolving the default Data Commons instance."""
  assert (resolve_instance_url("datacommons.org") ==
          "https://api.datacommons.org/v2")


@patch("requests.get")
def test_check_instance_is_valid_request_exception(mock_get):
  """Tests that a RequestException raises InvalidDCInstanceError."""
  mock_get.side_effect = requests.exceptions.RequestException("Request failed")
  with pytest.raises(InvalidDCInstanceError):
    check_instance_is_valid("https://invalid-instance")


@patch("requests.post")
def test_send_post_request_connection_error(mock_post):
  """Tests that a ConnectionError raises DCConnectionError."""
  mock_post.side_effect = requests.exceptions.ConnectionError(
      "Connection failed")
  with pytest.raises(DCConnectionError):
    _send_post_request("https://api.test.com", {}, {})


@patch("datacommons_client.utils.request_handling.check_instance_is_valid")
def test_resolve_instance_url_custom(mock_check_instance_is_valid):
  """Tests resolving a custom Data Commons instance."""
  mock_check_instance_is_valid.return_value = (
      "https://custom-instance/core/api/v2")

  assert (resolve_instance_url("custom-instance") ==
          "https://custom-instance/core/api/v2")
  mock_check_instance_is_valid.assert_called_once_with(
      "https://custom-instance/core/api/v2")


@patch("requests.get")
def test_check_instance_is_valid_valid(mock_get):
  """Tests that a valid instance URL is correctly validated."""

  # Create a mock response object with the expected JSON data and status code
  mock_response = MagicMock()
  mock_response.json.return_value = {"data": {"country/GTM": {}}}
  mock_response.status_code = 200
  mock_get.return_value = mock_response

  # Mock the instance URL to test
  instance_url = "https://valid-instance"

  # Assert that the instance URL is returned if it is valid
  assert check_instance_is_valid(instance_url) == instance_url
  mock_get.assert_called_once_with(
      f"{instance_url}/node?nodes=country%2FGTM&property=->name")


@patch("requests.get")
def test_check_instance_is_valid_invalid(mock_get):
  """Tests that an invalid instance URL raises the appropriate exception."""
  mock_response = MagicMock()
  mock_response.json.return_value = {"error": "Not Found"}
  mock_response.status_code = 404
  mock_get.return_value = mock_response

  with pytest.raises(InvalidDCInstanceError):
    check_instance_is_valid("https://invalid-instance")


@patch("requests.post")
def test_send_post_request_500_status_error(mock_post):
  """Tests that a 500-level HTTP error raises DCStatusError."""
  mock_response = MagicMock()
  mock_response.status_code = 500
  mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
      response=mock_response)
  mock_post.return_value = mock_response

  with pytest.raises(DCStatusError):
    _send_post_request("https://api.test.com", {}, {})


@patch("requests.post")
def test_send_post_request_other_http_error(mock_post):
  """Tests that non-500 HTTP errors raise APIError."""
  mock_response = MagicMock()
  mock_response.status_code = 404
  mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
      response=mock_response)
  mock_post.return_value = mock_response

  with pytest.raises(APIError):
    _send_post_request("https://api.test.com", {}, {})


def test_build_headers_with_api_key():
  """Tests building headers with an API key."""
  headers = build_headers("test-api-key")
  assert headers["Content-Type"] == "application/json"
  assert headers["X-API-Key"] == "test-api-key"


def test_build_headers_without_api_key():
  """Tests building headers without an API key."""
  headers = build_headers()
  assert headers["Content-Type"] == "application/json"
  assert "X-API-Key" not in headers


@patch("requests.post")
def test_send_post_request_success(mock_post):
  """Tests a successful POST request."""

  # Create a mock response object with the expected JSON data and status code
  mock_response = MagicMock()
  mock_response.status_code = 200
  mock_response.json.return_value = {"success": True}
  mock_post.return_value = mock_response

  # Mock the POST request
  url = "https://api.test.com"
  payload = {"key": "value"}
  headers = {"Content-Type": "application/json"}

  response = _send_post_request(url, payload, headers)
  assert response.status_code == 200
  assert response.json() == {"success": True}


@patch("requests.post")
def test_send_post_request_http_error(mock_post):
  """Tests handling an HTTP error during a POST request."""
  # Create a mock response object with a 401 status code
  mock_response = MagicMock()
  mock_response.status_code = 401
  mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
      response=mock_response)
  mock_post.return_value = mock_response

  # Mock the POST request and assert that a DCAuthenticationError is raised
  with pytest.raises(DCAuthenticationError):
    _send_post_request("https://api.test.com", {}, {})


def test_recursively_merge_dicts():
  """Tests recursive merging of dictionaries."""
  # Test merging two dictionaries with nested dictionaries and lists
  base = {"a": {"b": 1}, "c": [1, 2]}
  new = {"a": {"d": 2}, "c": [3], "e": 5}
  result = _recursively_merge_dicts(base, new)

  # Assert that the dictionaries are merged correctly
  assert result == {"a": {"b": 1, "d": 2}, "c": [1, 2, 3], "e": 5}


def test_merge_values_dicts():
  """Tests merging of dictionary values."""
  base = {"a": 1}
  new = {"b": 2}
  result = _merge_values(base, new)
  assert result == {"a": 1, "b": 2}


def test_merge_values_lists():
  """Tests merging of list values."""
  base = [1, 2]
  new = [3, 4]
  result = _merge_values(base, new)
  assert result == [1, 2, 3, 4]


def test_merge_values_other():
  """Tests merging non-dict, non-list values."""
  base = "value1"
  new = "value2"
  result = _merge_values(base, new)
  assert result == ["value1", "value2"]


def test_merge_values_complex_conflict():
  """Tests merging deeply nested, repeated objects."""

  # Nested but simple base
  base = {
      "key1": {
          "nested1": {
              "subkey1": "value1",
              "subkey2": [1, 2],
          },
          "nested2": "value2",
      },
      "key2": [1, 2, 3],
      "key3": "conflict1",
  }

  # Nested but complex new
  new = {
      "key1": {
          "nested1": {
              "subkey1": "new_value1",  # Already in base
              "subkey3": "new_value2",  # New key
          },
          "nested2": ["new_value3"],  # Conflicts with base (type change)
      },
      "key2": [4, 5],  # Should merge lists
      "key3": "conflict2",  # Should create a list due to conflict
      "key4": {
          "new_nested": "new_value4"
      },  # New key
  }

  # Expected result which keeps all data
  expected_result = {
      "key1": {
          "nested1": {
              "subkey1": ["value1", "new_value1"],
              "subkey2": [1, 2],
              "subkey3": "new_value2",
          },
          "nested2": ["value2", ["new_value3"]],
      },
      "key2": [1, 2, 3, 4, 5],
      "key3": ["conflict1", "conflict2"],
      "key4": {
          "new_nested": "new_value4"
      },
  }

  result = _merge_values(base, new)
  assert result == expected_result


@patch("datacommons_client.utils.request_handling._send_post_request")
def test_fetch_with_pagination(mock_send_post_request):
  """Tests fetching and merging paginated API responses."""

  # Mock the response JSON data for two pages
  mock_response = MagicMock()
  mock_response.json.side_effect = [
      {
          "data": {
              "page1": True
          },
          "nextToken": "token1"
      },
      {
          "data": {
              "page2": True
          }
      },
  ]
  mock_send_post_request.side_effect = [mock_response, mock_response]

  # Mock the POST request and assert that the results are merged correctly
  url = "https://api.test.com"
  payload = {}
  headers = {}

  result = _fetch_with_pagination(url, payload, headers)
  assert result == {"data": {"page1": True, "page2": True}, 'nextToken': None}


@patch("datacommons_client.utils.request_handling._send_post_request")
def test_fetch_with_pagination_next_token(mock_send_post_request):
  """Tests fetching and merging paginated API responses, including a next_token"""

  # Mock the response JSON data for two pages
  mock_response = MagicMock()

  # Should stop at first page given that all_pages is set to False
  mock_response.json.side_effect = [
      {
          "data": {
              "page1": True
          },
          "nextToken": "token2"
      },
  ]
  mock_send_post_request.side_effect = [mock_response, mock_response]

  # Mock the POST request and assert that the results are merged correctly
  url = "https://api.test.com"
  payload = {}
  headers = {}

  result = _fetch_with_pagination(url,
                                  payload,
                                  headers,
                                  all_pages=False,
                                  next_token="token1")
  assert result == {"data": {"page1": True}, 'nextToken': "token2"}


@patch("datacommons_client.utils.request_handling._send_post_request")
def test_fetch_with_pagination_next_token_all_pages(mock_send_post_request):
  """Tests fetching and merging paginated API responses, including a next_token"""

  # Mock the response JSON data for two pages
  mock_response = MagicMock()

  mock_response.json.side_effect = [
      {
          "data": {
              "page1": True
          },
          "nextToken": "token1"
      },
      {
          "data": {
              "page2": True
          },
          "nextToken": "token2",
      },
      {
          "data": {
              "page3": True
          },
          "nextToken": None,
      },
  ]
  mock_send_post_request.side_effect = [
      mock_response, mock_response, mock_response
  ]

  # Mock the POST request and assert that the results are merged correctly
  url = "https://api.test.com"
  payload = {}
  headers = {}

  result = _fetch_with_pagination(url,
                                  payload,
                                  headers,
                                  all_pages=True,
                                  next_token="token1")
  assert result == {
      "data": {
          "page1": True,
          "page2": True,
          "page3": True
      },
      'nextToken': None
  }


@patch("datacommons_client.utils.request_handling._send_post_request")
def test_fetch_with_pagination_invalid_json(mock_send_post_request):
  """Tests that invalid JSON response raises APIError."""
  mock_response = MagicMock()
  mock_response.json.side_effect = ValueError("Invalid JSON")
  mock_send_post_request.return_value = mock_response

  with pytest.raises(APIError):
    _fetch_with_pagination("https://api.test.com", {}, {})


@patch("datacommons_client.utils.request_handling._fetch_with_pagination")
def test_post_request(mock_fetch_with_pagination):
  """Tests the `post_request` function with mock pagination."""
  mock_fetch_with_pagination.return_value = {"result": "data"}
  result = post_request("https://api.test.com", {}, {})
  assert result == {"result": "data"}


@patch("datacommons_client.utils.request_handling._fetch_with_pagination")
def test_post_request_next_token(mock_fetch_with_pagination):
  """Tests the `post_request` function including a check for next_token"""
  mock_fetch_with_pagination.return_value = {"result": "data"}
  result = post_request("https://api.test.com", {}, {}, next_token="token123")

  mock_fetch_with_pagination.assert_called_once_with(url="https://api.test.com",
                                                     payload={},
                                                     headers={},
                                                     all_pages=True,
                                                     next_token="token123")


def test_post_request_invalid_payload():
  """Tests that a non-dictionary payload raises a ValueError."""
  with pytest.raises(ValueError):
    post_request("https://api.test.com", ["not", "a", "dict"], {})
