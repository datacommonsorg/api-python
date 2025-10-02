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


VALID_SURFACE_HEADER_VALUES = ["mcp", r"mcp-[\d\.]+", "datagemma"]


class InvalidSurfaceHeaderValueError(DataCommonsError):
  """ 
    The surface header value must be a surface known to the Data Commons team.
    This value is used in the DC usage logs in Mixer.
  """
  default_message = "The surface header value should only to indicate a call made from Data Commons surfaces like the MCP server or DataGemma."
