# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Endpoint client for the Data Commons SDMX 3.0 Data and Availability REST APIs."""

from collections.abc import Mapping, Sequence
from enum import Enum
from http import HTTPStatus
import io
from typing import Any, Dict, Optional

import requests

from datacommons_client.endpoints.base import API
from datacommons_client.endpoints.base import Endpoint
from datacommons_client.utils.context import _API_KEY_CONTEXT_VAR
from datacommons_client.utils.decorators import requires_pandas
from datacommons_client.utils.error_handling import SdmxAPIError
from datacommons_client.utils.error_handling import SdmxClientError

try:
  import pandas as pd
except ImportError:
  pd = None

# The SDMX context, agency, resource and version are fixed for Data Commons;
# the key is always the `*` wildcard.
DATAFLOW: str = "DC/DF_OBS/1.0.0/*"

DEFAULT_TIMEOUT_SECONDS: int = 60


class ApiLayout(Enum):
  """How a Data Commons deployment lays out its REST API paths.

  The public Data Commons API serves each API at the root of the host, while
  a self-hosted DCP instance serves them beneath a `/core/api` prefix.
  """

  ROOT = "root"
  CORE_API = "core_api"


# Path beneath which each deployment flavor serves the SDMX API.
_API_ROOTS = {
    ApiLayout.ROOT: "sdmx/v3",
    ApiLayout.CORE_API: "core/api/sdmx/v3",
}


def parse_filters(filters: Sequence[str]) -> dict[str, list[str]]:
  """Parses `key=value` filter strings into a constraint mapping.

  Repeating a key accumulates its values, which the API combines with OR.
  """
  if isinstance(filters, str):
    raise TypeError(
        "filters must be a sequence of 'key=value' strings, not a str.")

  constraints: dict[str, list[str]] = {}
  for item in filters:
    key, separator, value = item.partition("=")
    if not separator or not key.strip() or not value.strip():
      raise ValueError(f"Invalid filter '{item}'. Expected key=value "
                       "(for example: observationAbout=country/FRA).")
    constraints.setdefault(key.strip(), []).append(value.strip())
  return constraints


def build_query_params(
    variable: str,
    constraints: Optional[Mapping[str, Any]] = None,
) -> dict[str, str]:
  """Builds the `c[<component>]=<values>` query parameters for an SDMX request."""
  if not variable or not variable.strip():
    raise ValueError("variable must not be empty.")

  params: dict[str, str] = {}
  for component, value in (constraints or {}).items():
    if value is None:
      continue
    if isinstance(value, str):
      values = [value]
    elif isinstance(value, (Sequence, set)):
      values = sorted(value) if isinstance(value, set) else list(value)
    else:
      values = [str(value)]
    cleaned = [str(v).strip() for v in values if str(v).strip()]
    if cleaned:
      params[f"c[{component.strip()}]"] = ",".join(cleaned)

  params["c[variableMeasured]"] = variable.strip()
  return params


def extract_availability_values(
    payload: Mapping[str, Any],) -> dict[str, list[str]]:
  """Extracts `{component_id: [values]}` from an SDMX-JSON Availability response.

  Unpacks the nested `data.dataConstraints[*].cubeRegions[*].keyValues[*]`
  structure returned by the SDMX 3.0 Availability endpoint into a flat mapping
  of component IDs to their available string values.
  """
  result: dict[str, list[str]] = {}
  data = payload.get("data") if isinstance(payload, Mapping) else None
  if not isinstance(data, Mapping):
    return result

  for constraint in data.get("dataConstraints") or ():
    if not isinstance(constraint, Mapping):
      continue
    for region in constraint.get("cubeRegions") or ():
      if not isinstance(region, Mapping) or not region.get("include", True):
        continue
      for item in region.get("keyValues") or region.get("components") or ():
        if not isinstance(item, Mapping) or not item.get("include", True):
          continue
        comp_id = item.get("id")
        if not isinstance(comp_id, str) or not comp_id:
          continue
        values_bucket = result.setdefault(comp_id, [])
        for val in item.get("values") or ():
          v = val.get("value") if isinstance(val, Mapping) else val
          if v is not None and str(v) not in values_bucket:
            values_bucket.append(str(v))
  return result


def _resolve_sdmx_host_and_layout(
    raw_base_url: str,
    preferred_layout: Optional[ApiLayout] = None,
    headers: Optional[Mapping[str, str]] = None,
) -> tuple[str, ApiLayout]:
  """Resolves the host origin URL and initial SDMX layout from an API base URL.

  `API.base_url` typically ends with `/v2` (public Data Commons) or
  `/core/api/v2` (custom DCP instance), whereas SDMX endpoints are served at
  `/sdmx/v3` and `/core/api/sdmx/v3` respectively.
  """
  base = raw_base_url.rstrip("/")
  if base.endswith("/core/api/sdmx/v3"):
    host = base[:-len("/core/api/sdmx/v3")].rstrip("/")
    inferred = ApiLayout.CORE_API
  elif base.endswith("/sdmx/v3"):
    host = base[:-len("/sdmx/v3")].rstrip("/")
    inferred = ApiLayout.ROOT
  elif base.endswith("/core/api/v2"):
    host = base[:-len("/core/api/v2")].rstrip("/")
    inferred = ApiLayout.CORE_API
  elif base.endswith("/core/api"):
    host = base[:-len("/core/api")].rstrip("/")
    inferred = ApiLayout.CORE_API
  elif base.endswith("/v2"):
    host = base[:-len("/v2")].rstrip("/")
    inferred = ApiLayout.ROOT
  else:
    host = base
    has_bearer_auth = bool(headers and
                           any(k.lower() == "authorization" for k in headers))
    inferred = ApiLayout.CORE_API if has_bearer_auth else ApiLayout.ROOT

  return host, (preferred_layout or inferred)


class SdmxEndpoint(Endpoint):
  """Queries the SDMX 3.0 Data and Availability APIs of a Data Commons endpoint."""

  def __init__(
      self,
      api: API,
      *,
      session: Optional[requests.Session] = None,
      timeout: int = DEFAULT_TIMEOUT_SECONDS,
      preferred_layout: Optional[ApiLayout] = None,
  ) -> None:
    """Initializes the SdmxEndpoint instance.

    Args:
        api: An API instance providing the environment and authentication configuration.
        session: Optional pre-configured `requests.Session` (used in tests).
        timeout: Per-request timeout in seconds.
        preferred_layout: Optional layout hint (`ApiLayout.ROOT` or `ApiLayout.CORE_API`).
    """
    super().__init__(endpoint="sdmx/v3", api=api)
    self._session = session or requests.Session()
    self._timeout = timeout
    self._preferred_layout = preferred_layout
    self._api_root: Optional[str] = None

  def __repr__(self) -> str:
    """Returns a readable representation of the SdmxEndpoint object."""
    return f"<Sdmx Endpoint using {repr(self.api)}>"

  def post(self,
           payload: dict[str, Any],
           all_pages: bool = True,
           next_token: Optional[str] = None) -> Dict[str, Any]:
    """SDMX endpoints are queried via GET rather than POST."""
    raise NotImplementedError(
        "SDMX endpoints only support GET requests via fetch_data() and "
        "fetch_availability().")

  @property
  def base_url(self) -> str:
    """Returns the host base URL of the endpoint being queried."""
    host, _ = _resolve_sdmx_host_and_layout(
        self.api.base_url,
        self._preferred_layout,
        self.api.headers,
    )
    return host

  def fetch_data(
      self,
      variable: str,
      constraints: Optional[Mapping[str, str | Sequence[str]]] = None,
      *,
      response_format: str = "csv",
      log: bool = True,
      multi_entity: bool = True,
      accept: Optional[str] = None,
  ) -> str:
    """Fetches statistical observations for a variable via the SDMX 3.0 Data API.

    Args:
        variable: The statistical variable measured (e.g. `Count_Person`).
        constraints: Dimension and attribute filters to apply.
        response_format: SDMX response format; defaults to SDMX-CSV (`"csv"`).
        log: Request server-side SDMX execution logs (`X-Log-SDMX` header).
        multi_entity: Query across multi-entity schemas (`X-Use-Multi-Entity-Schema` header).
        accept: Optional `Accept` header override.

    Returns:
        The raw response body, SDMX-CSV by default.
    """
    params = build_query_params(variable, constraints)
    if response_format:
      params["format"] = response_format

    response = self._get(
        f"data/dataflow/{DATAFLOW}",
        params,
        log=log,
        multi_entity=multi_entity,
        accept=accept,
    )
    return response.text

  get_data = fetch_data

  @requires_pandas
  def fetch_data_as_dataframe(
      self,
      variable: str,
      constraints: Optional[Mapping[str, str | Sequence[str]]] = None,
      *,
      log: bool = True,
      multi_entity: bool = True,
      accept: Optional[str] = None,
  ) -> "pd.DataFrame":
    """Fetches statistical observations via SDMX-CSV and returns a pandas DataFrame.

    Args:
        variable: The statistical variable measured (e.g. `Count_Person`).
        constraints: Dimension and attribute filters to apply.
        log: Request server-side SDMX execution logs (`X-Log-SDMX` header).
        multi_entity: Query across multi-entity schemas (`X-Use-Multi-Entity-Schema` header).
        accept: Optional `Accept` header override.

    Returns:
        A `pandas.DataFrame` containing the SDMX-CSV rows and columns.
    """
    csv_text = self.fetch_data(
        variable,
        constraints,
        response_format="csv",
        log=log,
        multi_entity=multi_entity,
        accept=accept,
    )
    if not csv_text or not csv_text.strip():
      return pd.DataFrame()
    return pd.read_csv(io.StringIO(csv_text))

  def fetch_availability(
      self,
      component_id: str,
      variable: str,
      constraints: Optional[Mapping[str, str | Sequence[str]]] = None,
      *,
      log: bool = True,
      multi_entity: bool = True,
      accept: Optional[str] = None,
  ) -> dict[str, Any] | str:
    """Queries the values available for a dimension or attribute via the SDMX 3.0 Availability API.

    Args:
        component_id: The component to inspect (e.g. `provenance`, `unit`).
        variable: The statistical variable measured.
        constraints: Dimension and attribute filters to apply.
        log: Request server-side SDMX execution logs (`X-Log-SDMX` header).
        multi_entity: Query across multi-entity schemas (`X-Use-Multi-Entity-Schema` header).
        accept: Optional `Accept` header override.

    Returns:
        The parsed SDMX-JSON structure, or the raw body if it is not JSON.
    """
    if not component_id or not component_id.strip():
      raise ValueError("component_id must not be empty.")

    response = self._get(
        f"availability/dataflow/{DATAFLOW}/{component_id.strip()}",
        build_query_params(variable, constraints),
        log=log,
        multi_entity=multi_entity,
        accept=accept,
    )

    if "json" not in response.headers.get("Content-Type", ""):
      return response.text
    try:
      return response.json()
    except ValueError:
      return response.text

  get_availability = fetch_availability

  def fetch_available_values(
      self,
      component_id: str,
      variable: str,
      constraints: Optional[Mapping[str, str | Sequence[str]]] = None,
      *,
      log: bool = True,
      multi_entity: bool = True,
      accept: Optional[str] = None,
  ) -> dict[str, list[str]]:
    """Queries available dimension/attribute values and returns `{component_id: [values]}`.

    Convenience wrapper around `fetch_availability()` that unpacks the nested
    SDMX-JSON `dataConstraints[*].cubeRegions[*].keyValues[*]` payload.

    Args:
        component_id: The component to inspect (e.g. `provenance`, `unit`, or `*`).
        variable: The statistical variable measured.
        constraints: Dimension and attribute filters to apply.
        log: Request server-side SDMX execution logs (`X-Log-SDMX` header).
        multi_entity: Query across multi-entity schemas (`X-Use-Multi-Entity-Schema` header).
        accept: Optional `Accept` header override.

    Returns:
        A dictionary mapping each returned component ID to its list of available values.
    """
    payload = self.fetch_availability(
        component_id,
        variable,
        constraints,
        log=log,
        multi_entity=multi_entity,
        accept=accept,
    )
    if not isinstance(payload, Mapping):
      return {}
    return extract_availability_values(payload)

  def _get(
      self,
      resource: str,
      params: Mapping[str, str],
      *,
      log: bool,
      multi_entity: bool,
      accept: Optional[str],
  ) -> requests.Response:
    """Fetches an SDMX resource, discovering which API root the endpoint uses."""
    base_headers = {
        k: v for k, v in self.api.headers.items() if k.lower() != "content-type"
    }
    ctx_api_key = _API_KEY_CONTEXT_VAR.get()
    if ctx_api_key:
      base_headers["X-API-Key"] = ctx_api_key

    headers = {
        "X-Log-SDMX": str(log).lower(),
        "X-Use-Multi-Entity-Schema": str(multi_entity).lower(),
        **base_headers,
    }
    if accept:
      headers["Accept"] = accept

    response = None
    for api_root in self._candidate_api_roots():
      response = self._send(f"{api_root}/{resource}", params, headers)
      if response.status_code != HTTPStatus.NOT_FOUND:
        if response.ok:
          self._api_root = api_root
        break

    return self._checked(response)

  def _candidate_api_roots(self) -> tuple[str, ...]:
    """Returns the API roots to try, most likely first.

    A DCP instance serves the SDMX API under `/core/api` while the public
    Data Commons API serves it at the root. The endpoint's preferred layout
    is only a hint, so the other root is retained as a fallback: a 404 from
    the first root transparently retries against the second. A valid SDMX
    query never returns 404, so the status unambiguously signals that the
    endpoint uses the other layout.
    """
    if self._api_root:
      return (self._api_root,)

    _, layout = _resolve_sdmx_host_and_layout(
        self.api.base_url,
        self._preferred_layout,
        self.api.headers,
    )
    preferred = _API_ROOTS[layout]
    fallbacks = (root for root in _API_ROOTS.values() if root != preferred)
    return (preferred, *fallbacks)

  def _send(
      self,
      path: str,
      params: Mapping[str, str],
      headers: Mapping[str, str],
  ) -> requests.Response:
    """Performs a single authenticated GET request."""
    url = f"{self.base_url}/{path}"
    try:
      return self._session.get(url,
                               params=params,
                               headers=headers,
                               timeout=self._timeout)
    except requests.RequestException as e:
      raise SdmxClientError(
          f"Could not reach the Data Commons endpoint at {url}: {e}") from e

  @staticmethod
  def _checked(response: requests.Response) -> requests.Response:
    """Returns the response, raising `SdmxAPIError` for error statuses."""
    if response.ok:
      return response
    raise SdmxAPIError(response.status_code,
                       _error_message(response),
                       response=response)


SdmxClient = SdmxEndpoint


def _error_message(response: requests.Response) -> str:
  """Extracts the most informative error message from a failed response.

  Falls back to the HTTP reason phrase so that responses with blank or
  uninformative bodies still describe the failure.
  """
  message = ""
  try:
    payload = response.json()
  except ValueError:
    body = response.text.strip()
    # HTML bodies, such as a proxy or load balancer error page, are too
    # noisy to surface verbatim; leave them to the reason phrase fallback.
    if not body.startswith("<"):
      message = body
  else:
    if isinstance(payload, dict):
      err_field = payload.get("error")
      if isinstance(err_field, dict):
        message = str(
            payload.get("message") or err_field.get("message") or
            err_field.get("status") or "")
      else:
        message = str(payload.get("message") or err_field or "")
      # Surface the raw payload when it has content but no known field.
      if not message.strip() and payload:
        message = response.text
    elif payload:
      message = str(payload)

  return message.strip() or response.reason or "Unknown error"
