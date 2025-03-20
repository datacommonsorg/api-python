from requests import Request
from requests import Response

from datacommons_client.utils.error_handling import APIError
from datacommons_client.utils.error_handling import DataCommonsError
from datacommons_client.utils.error_handling import DCAuthenticationError
from datacommons_client.utils.error_handling import DCConnectionError
from datacommons_client.utils.error_handling import DCStatusError
from datacommons_client.utils.error_handling import InvalidDCInstanceError
from datacommons_client.utils.error_handling import NoDataForPropertyError


def test_data_commons_error_default_message():
  """Tests that DataCommonsError uses the default message."""
  error = DataCommonsError()
  assert str(error) == DataCommonsError.default_message


def test_data_commons_error_custom_message():
  """Tests that DataCommonsError uses a custom message when provided."""
  error = DataCommonsError("Custom message")
  assert str(error) == "Custom message"


def test_api_error_without_response():
  """Tests APIError initialization without a Response object."""
  error = APIError()
  assert str(error) == f"\n{APIError.default_message}"


def test_api_error_with_response():
  """Tests APIError initialization with a mocked Response object.

    Verifies that the string representation includes status code,
    request URL, and response text.
    """
  mock_request = Request("GET", "http://example.com").prepare()
  mock_response = Response()
  mock_response.request = mock_request
  mock_response.status_code = 404
  mock_response._content = b"Not Found"

  error = APIError(response=mock_response)
  assert "Status Code: 404" in str(error)
  assert "Request URL: http://example.com" in str(error)
  assert "Not Found" in str(error)


def test_subclass_default_messages():
  """Tests that subclasses use their default messages."""
  connection_error = DCConnectionError()
  assert DCConnectionError.default_message in str(connection_error)

  status_error = DCStatusError()
  assert DCStatusError.default_message in str(status_error)

  auth_error = DCAuthenticationError()
  assert DCAuthenticationError.default_message in str(auth_error)

  instance_error = InvalidDCInstanceError()
  assert InvalidDCInstanceError.default_message in str(instance_error)

  filter_error = NoDataForPropertyError()
  assert NoDataForPropertyError.default_message in str(filter_error)


def test_subclass_custom_message():
  """Tests that subclasses use custom messages when provided."""
  error = DCAuthenticationError(response=Response(),
                                message="Custom auth error")
  assert str(error) == "\nCustom auth error"
