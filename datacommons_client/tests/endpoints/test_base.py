from unittest.mock import patch

import pytest

from datacommons_client.endpoints.base import API
from datacommons_client.endpoints.base import Endpoint


@patch(
    "datacommons_client.endpoints.base.build_headers",
    return_value={"Content-Type": "application/json"},
)
@patch(
    "datacommons_client.endpoints.base.resolve_instance_url",
    return_value="https://api.datacommons.org/v2",
)
@patch(
    "datacommons_client.endpoints.base.check_instance_is_valid",
    return_value="https://api.datacommons.org/v2",
)
def test_api_initialization_default(mock_check_instance, mock_resolve_instance,
                                    mock_build_headers):
  """Tests default API initialization with `datacommons.org` instance."""
  api = API()

  assert api.base_url == "https://api.datacommons.org/v2"
  assert api.headers == {"Content-Type": "application/json"}
  mock_resolve_instance.assert_called_once_with("datacommons.org")
  mock_build_headers.assert_called_once_with(None)


@patch(
    "datacommons_client.endpoints.base.build_headers",
    return_value={"Content-Type": "application/json"},
)
@patch(
    "datacommons_client.endpoints.base.check_instance_is_valid",
    return_value="https://custom_instance.api/v2",
)
def test_api_initialization_with_url(mock_check_instance, mock_build_headers):
  """Tests API initialization with a fully qualified URL."""
  api = API(url="https://custom_instance.api/v2")
  assert api.base_url == "https://custom_instance.api/v2"
  assert api.headers == {"Content-Type": "application/json"}
  mock_check_instance.assert_called_once_with("https://custom_instance.api/v2")


@patch(
    "datacommons_client.endpoints.base.resolve_instance_url",
    return_value="https://custom-instance/api/v2",
)
@patch(
    "datacommons_client.endpoints.base.build_headers",
    return_value={"Content-Type": "application/json"},
)
def test_api_initialization_with_dc_instance(mock_build_headers,
                                             mock_resolve_instance_url):
  """Tests API initialization with a custom Data Commons instance."""
  api = API(dc_instance="custom-instance")

  assert api.base_url == "https://custom-instance/api/v2"
  assert api.headers == {"Content-Type": "application/json"}
  mock_resolve_instance_url.assert_called_once_with("custom-instance")


def test_api_initialization_invalid_args():
  """Tests API initialization with both `dc_instance` and `url` raises a ValueError."""
  with pytest.raises(ValueError):
    API(dc_instance="custom-instance", url="https://custom.api/v2")


@patch(
    "datacommons_client.endpoints.base.post_request",
    return_value={"success": True},
)
@patch(
    "datacommons_client.endpoints.base.check_instance_is_valid",
    return_value="https://custom_instance.api/v2",
)
def test_api_post_request(mock_check_instance, mock_post_request):
  """Tests making a POST request using the API object."""
  api = API(url="https://custom_instance.api/v2")
  payload = {"key": "value"}

  response = api.post(payload=payload, endpoint="test-endpoint", all_pages=True)
  assert response == {"success": True}
  mock_post_request.assert_called_once_with(
      url="https://custom_instance.api/v2/test-endpoint",
      payload=payload,
      headers=api.headers,
      all_pages=True,
      next_token=True)


@patch(
    "datacommons_client.endpoints.base.post_request",
    return_value={"success": True},
)
@patch(
    "datacommons_client.endpoints.base.check_instance_is_valid",
    return_value="https://custom.api/v2",
)
def test_endpoint_post_request(mock_check_instance, mock_post_request):
  """Tests making a POST request using the Endpoint object."""
  api = API(url="https://custom.api/v2")
  endpoint = Endpoint(endpoint="node", api=api)
  payload = {"key": "value"}

  response = endpoint.post(payload=payload, all_pages=True)
  assert response == {"success": True}
  mock_post_request.assert_called_once_with(url="https://custom.api/v2/node",
                                            payload=payload,
                                            headers=api.headers,
                                            all_pages=True,
                                            next_token=None)


@patch(
    "datacommons_client.endpoints.base.check_instance_is_valid",
    return_value="https://custom_instance.api/v2",
)
def test_api_post_request_invalid_payload(mock_check_instance):
  """Tests that an invalid payload raises a ValueError."""
  api = API(url="https://custom_instance.api/v2")

  with pytest.raises(ValueError):
    api.post(payload=["invalid", "payload"], endpoint="test-endpoint")


@patch(
    "datacommons_client.endpoints.base.check_instance_is_valid",
    return_value="https://custom_instance.api/v2",
)
def test_endpoint_initialization(mock_check_instance):
  """Tests initializing an Endpoint with a valid API instance."""
  api = API(url="https://custom_instance.api/v2")
  endpoint = Endpoint(endpoint="node", api=api)

  assert endpoint.endpoint == "node"
  assert endpoint.api is api


@patch(
    "datacommons_client.endpoints.base.check_instance_is_valid",
    return_value="https://custom.api/v2",
)
def test_endpoint_repr(mock_check_instance):
  """Tests the string representation of the Endpoint object."""
  api = API(url="https://custom.api/v2")
  endpoint = Endpoint(endpoint="node", api=api)

  assert (
      repr(endpoint) == "<Node Endpoint using <API at https://custom.api/v2>>")


@patch(
    "datacommons_client.endpoints.base.post_request",
    return_value={"success": True},
)
@patch(
    "datacommons_client.endpoints.base.check_instance_is_valid",
    return_value="https://custom.api/v2",
)
def test_endpoint_post_request(mock_check_instance, mock_post_request):
  """Tests making a POST request using the Endpoint object."""
  api = API(url="https://custom.api/v2")
  endpoint = Endpoint(endpoint="node", api=api)
  payload = {"key": "value"}

  response = endpoint.post(payload=payload, all_pages=True)
  assert response == {"success": True}
  mock_post_request.assert_called_once_with(url="https://custom.api/v2/node",
                                            payload=payload,
                                            headers=api.headers,
                                            all_pages=True,
                                            next_token=None)


@pytest.mark.parametrize("all_pages", [True, False])
@pytest.mark.parametrize("next_token", ["token123", None])
@patch(
    "datacommons_client.endpoints.base.post_request",
    return_value={"success": True},
)
@patch(
    "datacommons_client.endpoints.base.check_instance_is_valid",
    return_value="https://custom_instance.api/v2",
)
def test_api_post_request(mock_check_instance, mock_post_request, all_pages,
                          next_token):
  """Tests making a POST request using the API object with and without max_pages."""
  api = API(url="https://custom_instance.api/v2")
  payload = {"key": "value"}

  response = api.post(payload=payload,
                      endpoint="test-endpoint",
                      all_pages=all_pages,
                      next_token=next_token)
  assert response == {"success": True}
  mock_post_request.assert_called_once_with(
      url="https://custom_instance.api/v2/test-endpoint",
      payload=payload,
      headers=api.headers,
      all_pages=all_pages,
      next_token=next_token)


@patch(
    "datacommons_client.endpoints.base.check_instance_is_valid",
    return_value="https://custom.api/v2",
)
def test_endpoint_post_request_invalid_payload(mock_check_instance):
  """Tests that an invalid payload raises a ValueError in the Endpoint post method."""
  api = API(url="https://custom.api/v2")
  endpoint = Endpoint(endpoint="node", api=api)

  with pytest.raises(ValueError):
    endpoint.post(payload=["invalid", "payload"])


@patch(
    "datacommons_client.endpoints.base.build_headers",
    side_effect=lambda api_key: {"X-API-Key": api_key} if api_key else {},
)
@patch(
    "datacommons_client.endpoints.base.check_instance_is_valid",
    return_value="https://custom.api/v2",
)
def test_api_repr(mock_check_instance, mock_build_headers):
  """Tests the __repr__ method of the API class."""
  # Without API key
  api = API(url="https://custom.api/v2")
  assert repr(api) == "<API at https://custom.api/v2>"

  # With API key
  api_with_key = API(url="https://custom.api/v2", api_key="test_key")
  assert (
      repr(api_with_key) == "<API at https://custom.api/v2 (Authenticated)>")

  mock_build_headers.assert_any_call(None)
  mock_build_headers.assert_any_call("test_key")


@patch(
    "datacommons_client.endpoints.base.build_headers",
    return_value={"Content-Type": "application/json"},
)
@patch(
    "datacommons_client.endpoints.base.check_instance_is_valid",
    return_value="https://custom.api/v2",
)
def test_endpoint_repr(mock_check_instance, mock_build_headers):
  """Tests the __repr__ method of the Endpoint class."""
  api = API(url="https://custom.api/v2")
  endpoint = Endpoint(endpoint="node", api=api)

  expected_repr = "<Node Endpoint using <API at https://custom.api/v2>>"
  assert repr(endpoint) == expected_repr
