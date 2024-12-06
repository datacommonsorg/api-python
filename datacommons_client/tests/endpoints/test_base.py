from unittest.mock import patch

import pytest

from datacommons_client.endpoints.base import API
from datacommons_client.endpoints.base import EndPoint


@patch("datacommons_client.endpoints.base.build_headers")
@patch("datacommons_client.endpoints.base.resolve_instance_url")
def test_api_initialization_default(
    mock_resolve_instance_url, mock_build_headers
):
    """Tests default API initialization with `datacommons.org` instance."""
    mock_resolve_instance_url.return_value = "https://api.datacommons.org/v2"
    mock_build_headers.return_value = {"Content-Type": "application/json"}

    api = API()

    assert api.base_url == "https://api.datacommons.org/v2"
    assert api.headers == {"Content-Type": "application/json"}
    mock_resolve_instance_url.assert_called_once_with("datacommons.org")
    mock_build_headers.assert_called_once_with(None)


@patch("datacommons_client.endpoints.base.build_headers")
def test_api_initialization_with_url(mock_build_headers):
    """Tests API initialization with a fully qualified URL."""
    mock_build_headers.return_value = {"Content-Type": "application/json"}

    api = API(url="https://custom_instance.api/v2")
    assert api.base_url == "https://custom_instance.api/v2"
    assert api.headers == {"Content-Type": "application/json"}


@patch("datacommons_client.endpoints.base.build_headers")
@patch("datacommons_client.endpoints.base.resolve_instance_url")
def test_api_initialization_with_dc_instance(
    mock_resolve_instance_url, mock_build_headers
):
    """Tests API initialization with a custom Data Commons instance."""
    mock_resolve_instance_url.return_value = "https://custom-instance/api/v2"
    mock_build_headers.return_value = {"Content-Type": "application/json"}

    api = API(dc_instance="custom-instance")

    assert api.base_url == "https://custom-instance/api/v2"
    assert api.headers == {"Content-Type": "application/json"}
    mock_resolve_instance_url.assert_called_once_with("custom-instance")


def test_api_initialization_invalid_args():
    """Tests API initialization with both `dc_instance` and `url` raises a ValueError."""
    with pytest.raises(ValueError):
        API(dc_instance="custom-instance", url="https://custom.api/v2")


def test_api_repr():
    """Tests the string representation of the API object."""
    api = API(url="https://custom_instance.api/v2", api_key="test-key")
    assert (
        repr(api) == "<API at https://custom_instance.api/v2 (Authenticated)>"
    )

    api = API(url="https://custom_instance.api/v2")
    assert repr(api) == "<API at https://custom_instance.api/v2>"


@patch("datacommons_client.endpoints.base.post_request")
def test_api_post_request(mock_post_request):
    """Tests making a POST request using the API object."""
    mock_post_request.return_value = {"success": True}

    api = API(url="https://custom_instance.api/v2")
    payload = {"key": "value"}

    response = api.post(payload=payload, endpoint="test-endpoint")
    assert response == {"success": True}
    mock_post_request.assert_called_once_with(
        url="https://custom_instance.api/v2/test-endpoint",
        payload=payload,
        headers=api.headers,
    )


def test_api_post_request_invalid_payload():
    """Tests that an invalid payload raises a ValueError."""
    api = API(url="https://custom_instance.api/v2")

    with pytest.raises(ValueError):
        api.post(payload=["invalid", "payload"], endpoint="test-endpoint")


def test_endpoint_initialization():
    """Tests initializing an EndPoint with a valid API instance."""
    api = API(url="https://custom_instance.api/v2")
    endpoint = EndPoint(endpoint="node", api=api)

    assert endpoint.endpoint == "node"
    assert endpoint.api is api


def test_endpoint_repr():
    """Tests the string representation of the EndPoint object."""
    api = API(url="https://custom.api/v2")
    endpoint = EndPoint(endpoint="node", api=api)

    assert (
        repr(endpoint) == "<Node Endpoint using <API at https://custom.api/v2>>"
    )


@patch("datacommons_client.endpoints.base.post_request")
def test_endpoint_post_request(mock_post_request):
    """Tests making a POST request using the EndPoint object."""
    mock_post_request.return_value = {"success": True}

    api = API(url="https://custom.api/v2")
    endpoint = EndPoint(endpoint="node", api=api)
    payload = {"key": "value"}

    response = endpoint.post(payload=payload)
    assert response == {"success": True}
    mock_post_request.assert_called_once_with(
        url="https://custom.api/v2/node",
        payload=payload,
        headers=api.headers,
    )


def test_endpoint_post_request_invalid_payload():
    """Tests that an invalid payload raises a ValueError in the EndPoint post method."""
    api = API(url="https://custom.api/v2")
    endpoint = EndPoint(endpoint="node", api=api)

    with pytest.raises(ValueError):
        endpoint.post(payload=["invalid", "payload"])
