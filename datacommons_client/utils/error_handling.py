from typing import Optional

from requests import Response


class DataCommonsError(Exception):
  """Base exception for all Data Commons-related errors."""

  default_message = "An error occurred getting data from Data Commons API."

  def __init__(self, message: Optional[str] = None):
    """Initializes a DataCommonsError with a default or custom message."""
    super().__init__(message or self.default_message)


class APIError(DataCommonsError):
  """Represents an error interacting with Data Commons API."""

  default_message = "An API error occurred."

  def __init__(
      self,
      response: Optional[Response] = None,
      message: Optional[str] = None,
  ):
    """Initializes an APIError.

        Args:
            response (Optional[Response]): The response, if available.
            message (Optional[str]): A descriptive error message.
        """
    super().__init__(message or self.default_message)
    self.response = response
    self.request = getattr(response, "request", None)
    self.status_code = getattr(response, "status_code", None)

  def __str__(self) -> str:
    """Returns a detailed string representation of the error.

        Returns:
            str: A string describing the error, including the request URL if available.
        """

    details = f"\n{self.args[0]}"
    if self.status_code:
      details += f"\nStatus Code: {self.status_code}"
    if getattr(self.request, "url", None):
      details += f"\nRequest URL: {self.request.url}"
    if getattr(self.response, "text", None):
      details += f"\nResponse: {self.response.text}"

    return details


class DCConnectionError(APIError):
  """Raised for network-related errors in the Data Commons API."""

  default_message = (
      "A network error occurred while connecting to the Data Commons API.")


class DCStatusError(APIError):
  """Raised for non-2xx HTTP status code errors in the Data Commons API."""

  default_message = "The Data Commons API returned a non-2xx status code."


class DCAuthenticationError(APIError):
  """Raised for 401 Unauthorized errors in the Data Commons API."""

  default_message = "Authentication failed. Please check your API key."


class InvalidDCInstanceError(DataCommonsError):
  """Raised when an invalid Data Commons instance is provided."""

  default_message = "The specified Data Commons instance is invalid."


class InvalidObservationSelectError(DataCommonsError):
  """Raised when an invalid ObservationSelect field is provided."""

  default_message = "The ObservationSelect field is invalid."


class NoDataForPropertyError(DataCommonsError):
  """Raised when there is no data that meets the specified property filters."""

  default_message = "No available data for the specified property filters."


class SdmxClientError(APIError):
  """Base exception for SDMX client operations."""

  default_message = "An error occurred while querying the SDMX API."

  def __init__(
      self,
      message: Optional[str] = None,
      response: Optional[Response] = None,
  ) -> None:
    resolved_message = message or self.default_message
    super().__init__(response=response, message=resolved_message)
    self.message = resolved_message

  def __str__(self) -> str:
    return str(self.args[0])


class SdmxAPIError(SdmxClientError):
  """Raised when an SDMX endpoint returns an HTTP error status."""

  def __init__(
      self,
      status_code: int,
      message: str,
      response: Optional[Response] = None,
  ) -> None:
    super().__init__(
        message=f"SDMX API returned HTTP {status_code}: {message}",
        response=response,
    )
    self.status_code = status_code
    self.message = message

  def __reduce__(self):
    return (self.__class__, (self.status_code, self.message, self.response))
