from unittest.mock import patch

from datacommons_client.endpoints.base import API
from datacommons_client.endpoints.observation import ObservationEndpoint
from datacommons_client.endpoints.payloads import ObservationDate
from datacommons_client.endpoints.response import ObservationResponse


@patch(
    "datacommons_client.endpoints.base.check_instance_is_valid",
    return_value="https://custom.api/v2",
)
@patch(
    "datacommons_client.endpoints.base.post_request",
    return_value={"data": "mock_response"},
)
def test_fetch(mock_post_request, mock_check_instance_is_valid):
    """Tests the fetch method of ObservationEndpoint."""
    api = API(url="https://custom.api/v2")
    endpoint = ObservationEndpoint(api=api)

    response = endpoint.fetch(
        variable_dcids="dc/VariableID",
        date=ObservationDate.LATEST,
        select=["date", "variable", "entity", "value"],
        entity_dcids="dc/EntityID",
    )

    # Check the response
    assert isinstance(response, ObservationResponse)

    # Check the post request
    mock_post_request.assert_called_once_with(
        url="https://custom.api/v2/observation",
        payload={
            "variable": {"dcids": ["dc/VariableID"]},
            "date": ObservationDate.LATEST,
            "entity": {"dcids": ["dc/EntityID"]},
            "select": ["date", "variable", "entity", "value"],
        },
        headers=api.headers,
        max_pages=None,
    )


@patch(
    "datacommons_client.endpoints.base.check_instance_is_valid",
    return_value="https://custom.api/v2",
)
@patch(
    "datacommons_client.endpoints.base.post_request",
    return_value={"data": "mock_response"},
)
def test_fetch_latest_observation(
    mock_post_request, mock_check_instance_is_valid
):
    """Tests the fetch_latest_observation method."""
    api = API(url="https://custom.api/v2")
    endpoint = ObservationEndpoint(api=api)

    response = endpoint.fetch_latest_observation(
        variable_dcids=["dc/Variable1", "dc/Variable2"],
        entity_dcids="dc/EntityID",
    )

    # Check the response
    assert isinstance(response, ObservationResponse)

    # Check the post request
    mock_post_request.assert_called_once_with(
        url="https://custom.api/v2/observation",
        payload={
            "variable": {"dcids": ["dc/Variable1", "dc/Variable2"]},
            "date": ObservationDate.LATEST,
            "entity": {"dcids": ["dc/EntityID"]},
            "select": ["date", "variable", "entity", "value"],  # Default select
        },
        headers=api.headers,
        max_pages=None,
    )


@patch(
    "datacommons_client.endpoints.base.check_instance_is_valid",
    return_value="https://custom.api/v2",
)
@patch(
    "datacommons_client.endpoints.base.post_request",
    return_value={"data": "mock_response"},
)
def test_fetch_latest_observations_by_entity(
    mock_post_request, mock_check_instance_is_valid
):
    """Tests the fetch_latest_observations_by_entity method."""
    api = API(url="https://custom.api/v2")
    endpoint = ObservationEndpoint(api=api)

    response = endpoint.fetch_latest_observations_by_entity(
        variable_dcids="dc/VariableID",
        entity_dcids=["dc/Entity1", "dc/Entity2"],
    )

    # Check the response
    assert isinstance(response, ObservationResponse)

    # Check the post request
    mock_post_request.assert_called_once_with(
        url="https://custom.api/v2/observation",
        payload={
            "variable": {"dcids": ["dc/VariableID"]},
            "date": ObservationDate.LATEST,
            "entity": {"dcids": ["dc/Entity1", "dc/Entity2"]},
            "select": ["date", "variable", "entity", "value"],
        },
        headers=api.headers,
        max_pages=None,
    )


@patch(
    "datacommons_client.endpoints.base.check_instance_is_valid",
    return_value="https://custom.api/v2",
)
@patch(
    "datacommons_client.endpoints.base.post_request",
    return_value={"data": "mock_response"},
)
def test_fetch_observations_by_entity_type(
    mock_post_request, mock_check_instance_is_valid
):
    """Tests the fetch_observations_by_entity_type method."""
    api = API(url="https://custom.api/v2")
    endpoint = ObservationEndpoint(api=api)

    response = endpoint.fetch_observations_by_entity_type(
        date="2023",
        parent_entity="Earth",
        entity_type="Country",
        variable_dcids="dc/VariableID",
    )

    # Check the response
    assert isinstance(response, ObservationResponse)

    # Check the post request
    mock_post_request.assert_called_once_with(
        url="https://custom.api/v2/observation",
        payload={
            "variable": {"dcids": ["dc/VariableID"]},
            "date": "2023",
            "entity": {
                "expression": "Earth<-containedInPlace+{typeOf:Country}"
            },
            "select": ["date", "variable", "entity", "value"],
        },
        headers=api.headers,
        max_pages=None,
    )
