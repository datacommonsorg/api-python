import copy
from http import HTTPStatus
import json
import pickle
from typing import Any
from unittest.mock import MagicMock

import pytest
import requests

from datacommons_client import DataCommonsClient
from datacommons_client import use_api_key
from datacommons_client.endpoints.base import API
from datacommons_client.endpoints.sdmx import ApiLayout
from datacommons_client.endpoints.sdmx import build_query_params
from datacommons_client.endpoints.sdmx import parse_filters
from datacommons_client.endpoints.sdmx import SdmxAPIError
from datacommons_client.endpoints.sdmx import SdmxClient
from datacommons_client.endpoints.sdmx import SdmxClientError
from datacommons_client.endpoints.sdmx import SdmxEndpoint
from datacommons_client.utils.error_handling import APIError
from datacommons_client.utils.error_handling import DataCommonsError

_CSV = "STRUCTURE,OBS_VALUE\ndataflow,100\n"
_AVAILABILITY = {"data": {"dataConstraints": [{"id": "DF_OBS_AVAILABILITY"}]}}

_ROOT_DATA_URL = "https://api.datacommons.org/sdmx/v3/data/dataflow/DC/DF_OBS/1.0.0/*"
_CORE_DATA_URL = (
    "https://dc-service-xyz.run.app/core/api/sdmx/v3/data/dataflow/DC/DF_OBS/1.0.0/*"
)


@pytest.fixture
def public_api() -> API:
  return API(api_key="test-key")


@pytest.fixture
def instance_api() -> API:
  return API(
      url="https://dc-service-xyz.run.app/core/api/v2",
      headers={"Authorization": "Bearer test-token"},
      validate_instance=False,
  )


@pytest.fixture
def make_response():
  """Builds a mock `requests.Response`."""

  def _make(
      status_code: int = 200,
      text: str = "",
      *,
      json_body: Any = None,
      content_type: str = "text/csv",
      reason: str = "",
  ) -> MagicMock:
    response = MagicMock(spec=requests.Response)
    response.status_code = status_code
    response.ok = status_code < HTTPStatus.BAD_REQUEST
    response.reason = reason or HTTPStatus(status_code).phrase
    response.headers = {"Content-Type": content_type}
    if json_body is not None:
      response.text = json.dumps(json_body)
      response.json.return_value = json_body
    else:
      response.text = text
      response.json.side_effect = ValueError("not JSON")
    return response

  return _make


@pytest.fixture
def make_session():
  """Builds a mock `requests.Session` returning `responses` in order."""

  def _make(*responses: MagicMock) -> MagicMock:
    session = MagicMock(spec=requests.Session)
    session.get.side_effect = list(responses)
    return session

  return _make


class TestParseFilters:

  def test_parses_key_value_pairs(self):
    assert parse_filters(["a=1", "b=2"]) == {"a": ["1"], "b": ["2"]}

  def test_groups_repeated_keys(self):
    assert parse_filters(["a=1", "a=2"]) == {"a": ["1", "2"]}

  def test_strips_surrounding_whitespace(self):
    assert parse_filters([" a = 1 "]) == {"a": ["1"]}

  def test_keeps_equals_signs_in_values(self):
    assert parse_filters(["a=x=y"]) == {"a": ["x=y"]}

  def test_rejects_bare_string_input(self):
    with pytest.raises(TypeError, match="must be a sequence"):
      parse_filters("a=1")

  @pytest.mark.parametrize("bad", ["novalue", "=1", " =1", "a=", "a=   "])
  def test_rejects_malformed_filters(self, bad):
    with pytest.raises(ValueError, match="Invalid filter"):
      parse_filters([bad])


class TestBuildQueryParams:

  def test_includes_the_variable(self):
    assert build_query_params("Count_Person") == {
        "c[variableMeasured]": "Count_Person"
    }

  def test_rejects_empty_variable(self):
    with pytest.raises(ValueError, match="variable must not be empty"):
      build_query_params("   ")

  def test_renders_constraints_as_component_params(self):
    params = build_query_params("V", {"observationAbout": "country/FRA"})
    assert params["c[observationAbout]"] == "country/FRA"

  def test_joins_multiple_values_with_commas(self):
    params = build_query_params("V", {"provenance": ["a", "b"]})
    assert params["c[provenance]"] == "a,b"

  def test_joins_set_values_with_commas(self):
    params = build_query_params("V", {"provenance": {"b", "a"}})
    assert params["c[provenance]"] == "a,b"

  def test_ignores_empty_or_none_constraints(self):
    params = build_query_params("V", {"unit": [], "scalingFactor": None})
    assert params == {"c[variableMeasured]": "V"}


class TestSdmxEndpoint:

  def test_sdmx_client_is_alias_for_sdmx_endpoint(self):
    assert SdmxClient is SdmxEndpoint

  def test_errors_inherit_from_api_and_datacommons_error(self):
    err = SdmxAPIError(404, "Not found")
    assert isinstance(err, SdmxClientError)
    assert isinstance(err, APIError)
    assert isinstance(err, DataCommonsError)
    assert pickle.loads(pickle.dumps(err)).status_code == 404
    assert copy.deepcopy(err).message == "Not found"

  def test_repr_and_post_not_implemented(self, public_api):
    endpoint = SdmxEndpoint(public_api)
    assert repr(endpoint) == (
        "<Sdmx Endpoint using <API at https://api.datacommons.org/v2"
        " (Authenticated)>>")
    with pytest.raises(NotImplementedError):
      endpoint.post(payload={})

  def test_fetch_data_returns_the_response_body(self, public_api, make_response,
                                                make_session):
    session = make_session(make_response(text=_CSV))
    endpoint = SdmxEndpoint(public_api, session=session)

    assert endpoint.fetch_data("Count_Person") == _CSV

  def test_get_data_alias_returns_the_response_body(self, public_api,
                                                    make_response,
                                                    make_session):
    session = make_session(make_response(text=_CSV))
    endpoint = SdmxEndpoint(public_api, session=session)

    assert endpoint.get_data("Count_Person") == _CSV

  def test_fetch_data_sends_auth_headers_and_params(self, public_api,
                                                    make_response,
                                                    make_session):
    session = make_session(make_response(text=_CSV))

    SdmxEndpoint(public_api, session=session).fetch_data(
        "Count_Person", {"observationAbout": "country/USA"})

    url, kwargs = session.get.call_args.args[0], session.get.call_args.kwargs
    assert url == _ROOT_DATA_URL
    assert kwargs["headers"]["X-API-Key"] == "test-key"
    assert kwargs["headers"]["x-surface"] == "clientlib-python"
    assert "Content-Type" not in kwargs["headers"]
    assert kwargs["headers"]["X-Log-SDMX"] == "true"
    assert kwargs["headers"]["X-Use-Multi-Entity-Schema"] == "true"
    assert kwargs["params"]["c[observationAbout]"] == "country/USA"
    assert kwargs["params"]["format"] == "csv"

  def test_use_api_key_context_override(self, public_api, make_response,
                                        make_session):
    session = make_session(make_response(text=_CSV))
    endpoint = SdmxEndpoint(public_api, session=session)

    with use_api_key("override-key"):
      endpoint.fetch_data("Count_Person")

    headers = session.get.call_args.kwargs["headers"]
    assert headers["X-API-Key"] == "override-key"

  def test_header_flags_can_be_disabled(self, public_api, make_response,
                                        make_session):
    session = make_session(make_response(text=_CSV))

    SdmxEndpoint(public_api,
                 session=session).fetch_data("V",
                                             log=False,
                                             multi_entity=False,
                                             accept="application/json")

    headers = session.get.call_args.kwargs["headers"]
    assert headers["X-Log-SDMX"] == "false"
    assert headers["X-Use-Multi-Entity-Schema"] == "false"
    assert headers["Accept"] == "application/json"

  def test_fetch_availability_parses_json(self, public_api, make_response,
                                          make_session):
    session = make_session(
        make_response(json_body=_AVAILABILITY, content_type="application/json"))
    endpoint = SdmxEndpoint(public_api, session=session)

    assert endpoint.fetch_availability("provenance",
                                       "Count_Person") == _AVAILABILITY

  def test_fetch_availability_rejects_empty_component(self, public_api):
    endpoint = SdmxEndpoint(public_api)
    with pytest.raises(ValueError, match="component_id must not be empty"):
      endpoint.fetch_availability("   ", "Count_Person")

  def test_fetch_availability_falls_back_to_raw_text(self, public_api,
                                                     make_response,
                                                     make_session):
    session = make_session(
        make_response(text="not json", content_type="text/plain"))
    endpoint = SdmxEndpoint(public_api, session=session)

    assert endpoint.get_availability("provenance", "V") == "not json"

  def test_fetch_available_values_unpacks_cube_regions(self, public_api,
                                                       make_response,
                                                       make_session):
    availability_payload = {
        "data": {
            "dataConstraints": [{
                "id":
                    "DF_OBS_AVAILABILITY",
                "cubeRegions": [{
                    "include":
                        True,
                    "keyValues": [
                        {
                            "id": "provenance",
                            "include": True,
                            "values": ["dc/base/CensusPEP", "dc/base/WHO"],
                        },
                        {
                            "id": "TIME_PERIOD",
                            "include": True,
                            "values": ["2020", "2021"],
                        },
                    ],
                }],
            }]
        }
    }
    session = make_session(
        make_response(json_body=availability_payload,
                      content_type="application/json"))
    endpoint = SdmxEndpoint(public_api, session=session)

    values = endpoint.fetch_available_values("*", "Count_Person")
    assert values == {
        "provenance": ["dc/base/CensusPEP", "dc/base/WHO"],
        "TIME_PERIOD": ["2020", "2021"],
    }

  def test_fetch_data_as_dataframe(self, public_api, make_response,
                                   make_session):
    session = make_session(make_response(text=_CSV))
    endpoint = SdmxEndpoint(public_api, session=session)

    df = endpoint.fetch_data_as_dataframe("Count_Person")
    assert list(df.columns) == ["STRUCTURE", "OBS_VALUE"]
    assert len(df) == 1
    assert df.iloc[0]["OBS_VALUE"] == 100

  def test_instance_api_prefers_the_core_api_root(self, instance_api,
                                                  make_response, make_session):
    session = make_session(make_response(text=_CSV))

    SdmxEndpoint(instance_api, session=session).fetch_data("V")

    assert session.get.call_args.args[0] == _CORE_DATA_URL
    assert (session.get.call_args.kwargs["headers"]["Authorization"] ==
            "Bearer test-token")

  def test_bare_host_with_authorization_header_prefers_core_api(
      self, make_response, make_session):
    api = API(
        url="https://dc-service-xyz.run.app",
        headers={"Authorization": "Bearer test-token"},
        validate_instance=False,
    )
    session = make_session(make_response(text=_CSV))

    SdmxEndpoint(api, session=session).fetch_data("V")

    assert session.get.call_args.args[0] == _CORE_DATA_URL

  def test_bare_host_with_preferred_layout_core_api(self, make_response,
                                                    make_session):
    api = API(url="https://dc-service-xyz.run.app", validate_instance=False)
    session = make_session(make_response(text=_CSV))

    SdmxEndpoint(api, session=session,
                 preferred_layout=ApiLayout.CORE_API).fetch_data("V")

    assert session.get.call_args.args[0] == _CORE_DATA_URL

  def test_a_404_retries_against_the_other_api_root(self, instance_api,
                                                    make_response,
                                                    make_session):
    session = make_session(make_response(404), make_response(text=_CSV))
    endpoint = SdmxEndpoint(instance_api, session=session)

    assert endpoint.fetch_data("V") == _CSV

    attempted = [call.args[0] for call in session.get.call_args_list]
    assert attempted == [
        _CORE_DATA_URL,
        "https://dc-service-xyz.run.app/sdmx/v3/data/dataflow/DC/DF_OBS/1.0.0/*",
    ]

  def test_the_discovered_api_root_is_reused(self, instance_api, make_response,
                                             make_session):
    session = make_session(
        make_response(404),
        make_response(text=_CSV),
        make_response(text=_CSV),
    )
    endpoint = SdmxEndpoint(instance_api, session=session)

    endpoint.fetch_data("V")
    endpoint.fetch_data("V")

    # Only the first query pays for the discovery attempt.
    assert session.get.call_count == 3

  def test_transient_500_does_not_cache_api_root(self, instance_api,
                                                 make_response, make_session):
    session = make_session(
        make_response(500, reason="Server Error"),
        make_response(404),
        make_response(text=_CSV),
    )
    endpoint = SdmxEndpoint(instance_api, session=session)

    with pytest.raises(SdmxAPIError):
      endpoint.fetch_data("V")

    assert endpoint._api_root is None
    assert endpoint.fetch_data("V") == _CSV
    assert endpoint._api_root == "sdmx/v3"

  def test_a_404_from_every_root_is_reported(self, public_api, make_response,
                                             make_session):
    session = make_session(make_response(404, reason="Not Found"),
                           make_response(404))
    endpoint = SdmxEndpoint(public_api, session=session)

    with pytest.raises(SdmxAPIError) as excinfo:
      endpoint.fetch_data("V")

    assert excinfo.value.status_code == 404

  def test_api_errors_surface_the_server_message(self, public_api,
                                                 make_response, make_session):
    session = make_session(
        make_response(401, json_body={"message": "API key not valid"}))
    endpoint = SdmxEndpoint(public_api, session=session)

    with pytest.raises(SdmxAPIError, match="API key not valid") as excinfo:
      endpoint.fetch_data("V")

    assert excinfo.value.status_code == 401

  def test_api_errors_extract_nested_error_message(self, public_api,
                                                   make_response, make_session):
    session = make_session(
        make_response(
            401,
            json_body={
                "error": {
                    "code": 401,
                    "message": "Nested auth failure",
                }
            },
        ))
    endpoint = SdmxEndpoint(public_api, session=session)

    with pytest.raises(SdmxAPIError, match="Nested auth failure") as excinfo:
      endpoint.fetch_data("V")

    assert excinfo.value.message == "Nested auth failure"

  def test_api_errors_fall_back_to_the_reason_phrase(self, public_api,
                                                     make_response,
                                                     make_session):
    session = make_session(make_response(500, text="", reason="Server Error"))
    endpoint = SdmxEndpoint(public_api, session=session)

    with pytest.raises(SdmxAPIError, match="Server Error"):
      endpoint.fetch_data("V")

  def test_html_error_bodies_are_not_echoed(self, public_api, make_response,
                                            make_session):
    session = make_session(
        make_response(502, text="<html>gateway</html>", reason="Bad Gateway"))
    endpoint = SdmxEndpoint(public_api, session=session)

    with pytest.raises(SdmxAPIError, match="Bad Gateway"):
      endpoint.fetch_data("V")

  def test_network_failures_are_wrapped(self, public_api, make_session):
    session = make_session()
    session.get.side_effect = requests.ConnectionError("refused")
    endpoint = SdmxEndpoint(public_api, session=session)

    with pytest.raises(SdmxClientError, match="Could not reach"):
      endpoint.fetch_data("V")

  def test_datacommons_client_exposes_sdmx_endpoint(self):
    client = DataCommonsClient(api_key="test-key")
    assert isinstance(client.sdmx, SdmxEndpoint)
    assert client.sdmx.api is client.api
    assert client.sdmx.base_url == "https://api.datacommons.org"
