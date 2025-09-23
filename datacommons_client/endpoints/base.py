import re
from typing import Any, Dict, Optional

from datacommons_client.utils.error_handling import InvalidSurfaceHeaderValueError
from datacommons_client.utils.error_handling import VALID_SURFACE_HEADER_VALUES
from datacommons_client.utils.request_handling import check_instance_is_valid
from datacommons_client.utils.request_handling import post_request
from datacommons_client.utils.request_handling import resolve_instance_url


class API:
  """Represents a configured API interface to the Data Commons API.

  This class handles environment setup, resolving the base URL, building headers,
  or optionally using a fully qualified URL directly. It can be used standalone
  to interact with the API or in combination with Endpoint classes.
  """

  def __init__(
      self,
      api_key: Optional[str] = None,
      dc_instance: Optional[str] = None,
      url: Optional[str] = None,
      surface_header_value: Optional[str] = None,
  ):
    """
    Initializes the API instance.

    Args:
        api_key: The API key for authentication. Defaults to None.
        dc_instance: The Data Commons instance domain. Ignored if `url` is provided.
                     Defaults to 'datacommons.org' if both `url` and `dc_instance` are None.
        url: A fully qualified URL for the base API. This may be useful if more granular control
            of the API is required (for local development, for example). If provided, dc_instance`
             should not be provided.
        surface_header_value: indicates which DC surface (MCP server, etc.) makes a call to the python library.
            If the call originated internally, this is null and we pass in "clientlib-python" as the surface header 

    Raises:
        ValueError: If both `dc_instance` and `url` are provided.
    """
    if dc_instance and url:
      raise ValueError("Cannot provide both `dc_instance` and `url`.")

    if not dc_instance and not url:
      dc_instance = "datacommons.org"

    if url is not None:
      # Use the given URL directly (strip trailing slash)
      self.base_url = check_instance_is_valid(url.rstrip("/"))
    else:
      # Resolve from dc_instance
      self.base_url = resolve_instance_url(dc_instance)

    # if this call originates from another DC product (MCP server, DataGemma, etc.), we indicate that to Mixer
    # otherwise, the 'x-surface' header is 'clientlib-python'
    if surface_header_value:
      # use patterns to support tags like mcp-{VERSION}
      if not any(
          re.fullmatch(pattern, surface_header_value)
          for pattern in VALID_SURFACE_HEADER_VALUES):
        raise InvalidSurfaceHeaderValueError

    self.headers = self.build_headers(surface_header_value=surface_header_value,
                                      api_key=api_key)

  def __repr__(self) -> str:
    """Returns a readable representation of the API object.

    Indicates the base URL and if it's authenticated.

    Returns:
        str: A string representation of the API object.
    """
    has_auth = " (Authenticated)" if "X-API-Key" in self.headers else ""
    return f"<API at {self.base_url}{has_auth}>"

  def post(self,
           payload: dict[str, Any],
           endpoint: Optional[str] = None,
           *,
           all_pages: bool = True,
           next_token: Optional[str] = None) -> Dict[str, Any]:
    """Makes a POST request using the configured API environment.

    If `endpoint` is provided, it will be appended to the base_url. Otherwise,
    it will just POST to the base URL.

    Args:
        payload: The JSON payload for the POST request.
        endpoint: An optional endpoint path to append to the base URL.
        all_pages: If True, fetch all pages of the response. If False, fetch only the first page.
            Defaults to True. Set to False to only fetch the first page. In that case, a
            `next_token` key in the response will indicate if more pages are available.
            That token can be used to fetch the next page.

    Returns:
        A dictionary containing the merged response data.

    Raises:
        ValueError: If the payload is not a valid dictionary.
    """
    if not isinstance(payload, dict):
      raise ValueError("Payload must be a dictionary.")

    url = (self.base_url if endpoint is None else f"{self.base_url}/{endpoint}")

    return post_request(url=url,
                        payload=payload,
                        headers=self.headers,
                        all_pages=all_pages,
                        next_token=next_token)

  def build_headers(self,
                    surface_header_value: Optional[str],
                    api_key: Optional[str] = None) -> dict[str, str]:
    """Build request headers for API requests.

    Includes JSON content type. If an API key is provided, add it as `X-API-Key`.

    Args:
        self: the API, which includes API key and surface header if available

    Returns:
        A dictionary of headers for the request.
    """
    headers = {
        "Content-Type": "application/json",
        "x-surface": "clientlib-python"
    }
    if api_key:
      headers["X-API-Key"] = api_key

    if surface_header_value:
      headers["x-surface"] = surface_header_value

    return headers


class Endpoint:
  """Represents a specific endpoint within the Data Commons API.

  This class leverages an API instance to make requests. It does not
  handle instance resolution or headers directly; that is delegated to the API instance.

  Attributes:
      endpoint (str): The endpoint path (e.g., 'node').
      api (API): The API instance providing configuration and the `post` method.
  """

  def __init__(self, endpoint: str, api: API):
    """
    Initializes the Endpoint instance.

    Args:
        endpoint: The endpoint path (e.g., 'node').
        api: An API instance that provides the environment configuration.
    """
    self.endpoint = endpoint
    self.api = api

  def __repr__(self) -> str:
    """Returns a readable representation of the Endpoint object.

    Shows the endpoint and underlying API configuration.

    Returns:
        str: A string representation of the Endpoint object.
    """
    return f"<{self.endpoint.title()} Endpoint using {repr(self.api)}>"

  def post(self,
           payload: dict[str, Any],
           all_pages: bool = True,
           next_token: Optional[str] = None) -> Dict[str, Any]:
    """Makes a POST request to the specified endpoint using the API instance.

    Args:
        payload: The JSON payload for the POST request.
        all_pages: If True, fetch all pages of the response. If False, fetch only the first page.
            Defaults to True. Set to False to only fetch the first page. In that case, a
            `next_token` key in the response will indicate if more pages are available.
            That token can be used to fetch the next page.
        next_token: Optionally, the token to fetch the next page of results. Defaults to None.

    Returns:
        A dictionary with the merged API response data.

    Raises:
        ValueError: If the payload is not a valid dictionary.
    """
    return self.api.post(payload=payload,
                         endpoint=self.endpoint,
                         all_pages=all_pages,
                         next_token=next_token)
