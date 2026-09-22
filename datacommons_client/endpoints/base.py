from collections.abc import Mapping
import re
from typing import Any, Dict, Optional

from datacommons_client.utils.context import _API_KEY_CONTEXT_VAR
from datacommons_client.utils.request_handling import check_instance_is_valid
from datacommons_client.utils.request_handling import CUSTOM_DC_V2
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
      *,
      headers: Optional[Mapping[str, str]] = None,
      validate_instance: bool = True,
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
        headers: Optional additional HTTP headers (e.g. Authorization bearer tokens) to include in requests.
        validate_instance: Whether to probe the instance URL during initialization. Defaults to True.

    Raises:
        ValueError: If both `dc_instance` and `url` are provided.
    """
    if dc_instance and url:
      raise ValueError("Cannot provide both `dc_instance` and `url`.")

    if not dc_instance and not url:
      dc_instance = "datacommons.org"

    if url is not None:
      clean_url = url.rstrip("/")
      if not validate_instance:
        self.base_url = clean_url
      elif headers:
        self.base_url = check_instance_is_valid(clean_url,
                                                api_key=api_key,
                                                headers=dict(headers))
      else:
        self.base_url = check_instance_is_valid(clean_url, api_key=api_key)
    else:
      clean_dc = (dc_instance.replace("https://", "").replace("http://",
                                                              "").rstrip("/"))
      if not validate_instance:
        if clean_dc == "datacommons.org":
          self.base_url = resolve_instance_url("datacommons.org")
        else:
          self.base_url = f"https://{clean_dc}{CUSTOM_DC_V2}"
      elif headers:
        self.base_url = resolve_instance_url(dc_instance,
                                             api_key=api_key,
                                             headers=dict(headers))
      else:
        self.base_url = resolve_instance_url(dc_instance, api_key=api_key)

    self.headers = self.build_headers(surface_header_value=surface_header_value,
                                      api_key=api_key,
                                      custom_headers=headers)

  def __repr__(self) -> str:
    """Returns a readable representation of the API object.

    Indicates the base URL and if it's authenticated.

    Returns:
        str: A string representation of the API object.
    """
    has_auth = (" (Authenticated)" if any(
        k.lower() in ("x-api-key", "authorization") for k in self.headers) else
                "")
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

    headers = self.headers
    ctx_api_key = _API_KEY_CONTEXT_VAR.get()
    if ctx_api_key:
      # Copy headers to avoid mutating the shared client state
      headers = self.headers.copy()
      headers["X-API-Key"] = ctx_api_key

    return post_request(url=url,
                        payload=payload,
                        headers=headers,
                        all_pages=all_pages,
                        next_token=next_token)

  def build_headers(
      self,
      surface_header_value: Optional[str],
      api_key: Optional[str] = None,
      custom_headers: Optional[Mapping[str, str]] = None,
  ) -> dict[str, str]:
    """Build request headers for API requests.

    Includes JSON content type. If an API key is provided, add it as `X-API-Key`.

    Args:
        self: the API, which includes API key and surface header if available
        surface_header_value: Optional surface header identifier.
        api_key: Optional API key for X-API-Key header.
        custom_headers: Optional custom headers to merge into the request headers.

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

    if custom_headers:
      headers.update(custom_headers)

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
