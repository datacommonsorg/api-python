from typing import Any, Dict, Optional

import requests
from requests import exceptions
from requests import Response

from datacommons_client.utils.error_handling import APIError
from datacommons_client.utils.error_handling import DCAuthenticationError
from datacommons_client.utils.error_handling import DCConnectionError
from datacommons_client.utils.error_handling import DCStatusError
from datacommons_client.utils.error_handling import InvalidDCInstanceError

BASE_DC_V2: str = "https://api.datacommons.org/v2"
CUSTOM_DC_V2: str = "/core/api/v2"


def check_instance_is_valid(instance_url: str) -> str:
  """Check that the given instance URL points to a valid Data Commons instance.

    This function attempts a GET request against a known node in Data Commons to
    validate the given instance URL. If the node is found and the response has the
    expected data, the URL is considered valid.

    Args:
        instance_url: The Data Commons instance URL to validate.

    Returns:
        The validated instance URL.

    Raises:
        InvalidDCInstanceError: If the instance URL does not seem to be a valid
            Data Commons instance.
    """
  # Test URL for a known node in Data Commons
  test_url = f"{instance_url}/node?nodes=country%2FGTM&property=->name"

  try:
    response = requests.get(test_url)
    response.raise_for_status()
  except requests.exceptions.RequestException as exc:
    raise InvalidDCInstanceError(exc.response) from exc

  data = response.json()
  if "data" not in data or "country/GTM" not in data["data"]:
    raise InvalidDCInstanceError(
        f"{instance_url} is not a valid Data Commons instance.")

  return instance_url


def resolve_instance_url(dc_instance: str) -> str:
  """Resolve the base API URL for a given Data Commons instance.

    If the instance is `datacommons.org`, the default URL is returned. Otherwise,
    the custom URL is validated via `_check_instance_is_valid`.

    Args:
        dc_instance: The identifier or domain of the Data Commons instance.

    Returns:
        The resolved base API URL.
    """
  # if https or http included in the string, remove it
  dc_instance = dc_instance.replace("https://", "").replace("http://", "")

  # If the instance is the default, return the base URL
  if dc_instance == "datacommons.org":
    return BASE_DC_V2

  # Otherwise, validate the custom instance URL
  url = f"https://{dc_instance}{CUSTOM_DC_V2}"
  return check_instance_is_valid(url)


def build_headers(api_key: str | None = None) -> dict[str, str]:
  """Build request headers for API requests.

    Includes JSON content type. If an API key is provided, add it as `X-API-Key`.

    Args:
        api_key: Optional API key string for authenticated requests.

    Returns:
        A dictionary of headers for the request.
    """
  headers = {"Content-Type": "application/json"}
  if api_key:
    headers["X-API-Key"] = api_key
  return headers


def _send_post_request(url: str, payload: dict[str, Any],
                       headers: dict[str, str]) -> Response:
  """Send a POST request and handle common HTTP errors with custom exceptions.

    Args:
        url: The target endpoint URL.
        payload: The JSON payload to send with the request.
        headers: The request headers, including authentication.

    Returns:
        The successful Response object.

    Raises:
        DCConnectionError: If a network-related error occurs.
        DCAuthenticationError: If a 401 Unauthorized error is received.
        DCStatusError: for 500-level HTTP errors.
        APIError: For other HTTP errors.
    """
  try:
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response

  # if ConnectionError
  except exceptions.ConnectionError as exc:
    raise DCConnectionError(exc.response) from exc

  # if HTTPError
  except exceptions.HTTPError as exc:
    status_code = (exc.response.status_code
                   if exc.response is not None else None)

    # if 401 Unauthorized
    if status_code == 401:
      raise DCAuthenticationError(exc.response) from exc

    # if 500-level HTTP errors
    if status_code >= 500:
      raise DCStatusError(exc.response) from exc

    # for other HTTP errors
    raise APIError(exc.response) from exc


def _recursively_merge_dicts(
    base: dict[str, Any],
    new: dict[str, Any],
    keys_to_skip: Optional[set[str]] = None,
) -> dict[str, Any]:
  """Recursively merge two dictionaries, skipping specified keys.

    Uses `_merge_values` to handle nested structures and different types.

    Args:
        base: The base dictionary.
        new: The dictionary to merge into `base`.
        keys_to_skip: A set of keys to skip during merging.

    Returns:
        A new dictionary that is the result of merging `new` into `base`.
    """
  if keys_to_skip is None:
    keys_to_skip = {"nextToken"}

  result = dict(base)
  for k, v in new.items():
    # Skip keys that should be excluded from merging
    if k in keys_to_skip:
      continue
    # If the key is already present, merge the values
    if k in result:
      result[k] = _merge_values(result[k], v)
    # Otherwise, add the new key-value pair
    else:
      result[k] = v
  return result


def _merge_values(base: Any, new: Any) -> Any:
  """Merge two values based on their structure using pattern matching.

    - If both are dicts, recursively merge them.
    - If both are lists, concatenate them.
    - Otherwise, if they differ, combine into a list. If they are equal, return one of them.

    Args:
        base: Existing value in the data structure.
        new: New value to merge in.

    Returns:
        The merged value.
    """
  match base, new:
    case dict(), dict():
      return _recursively_merge_dicts(base, new)
    case list(), list():
      # Merge two lists by concatenation
      return base + new
    case _:
      # If they are the same, return one. Otherwise, combine into a list.
      return base if base == new else [base, new]


def _fetch_with_pagination(
    url: str,
    payload: dict[str, Any],
    headers: dict[str, str],
    all_pages: bool = True,
    next_token: Optional[str] = None,
) -> dict[str, Any]:
  """Fetch and (if necessary) merge paginated results from an API.

    Continues fetching pages until `nextToken` is not found or `max_pages` is reached.

    Args:
        url: The API endpoint URL.
        payload: The request payload (JSON-serializable).
        headers: The request headers.
        all_pages: Whether to fetch all available pages. Defaults to True.
        next_token: Optionally, the token to fetch the next page of results. Defaults to None.

    Returns:
        A dictionary containing all merged results from all fetched pages.
    """
  combined_results: dict[str, Any] = {}

  # If a next token is provided, use it to fetch the next page
  if next_token:
    payload["nextToken"] = next_token

  while True:
    # Send a POST request and parse the JSON response
    response = _send_post_request(url, payload, headers)

    try:
      page_data = response.json()
    except ValueError:
      raise APIError(response)

    # Merge current page data into combined results
    combined_results = _recursively_merge_dicts(combined_results, page_data)

    # Update the payload with the next token
    next_token = page_data.get("nextToken")

    # Update the payload with the next token
    payload["nextToken"] = next_token

    # Stop if the user only wants one page or if there is no next token
    if not all_pages or not next_token:
      break

  # Add the final next token to the response
  combined_results["nextToken"] = next_token

  return combined_results


def post_request(
    url: str,
    payload: dict[str, Any],
    headers: dict[str, str],
    *,
    all_pages: bool = True,
    next_token: Optional[str] = None,
) -> Dict[str, Any]:
  """Send a POST request with optional pagination support and return a DCResponse.

    Args:
        url: The target endpoint URL.
        payload: The payload to send with the request.
        headers: The request headers, including authentication.
        all_pages: Fetch all available pages automatically. Defaults to True. Set to
            False to only fetch the first page. In that case, a `nextToken` key in the
            response will indicate if more pages are available. That token can be used
            to fetch the next page.
        next_token: Optionally, the token to fetch the next page of results. Defaults to None.
            If combined with `all_pages=False`, this token will be used to fetch only the next page.
            If combined with `all_pages=True`, all remaining pages will be fetched starting from this token.


    Returns:
        A dictionary containing the aggregated API response data.

    Raises:
        ValueError: If `json` is not a dictionary.
    """
  if not isinstance(payload, dict):
    raise ValueError("Payload must be a dictionary.")

  # Fetch and merge paginated results
  combined_results = _fetch_with_pagination(url=url,
                                            payload=payload,
                                            headers=headers,
                                            all_pages=all_pages,
                                            next_token=next_token)

  # Return the combined results as a dictionary
  return combined_results
