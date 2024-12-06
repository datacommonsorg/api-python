import pytest

from datacommons_client.endpoints.payloads import NodePayload
from datacommons_client.endpoints.payloads import ObservationDate
from datacommons_client.endpoints.payloads import ObservationPayload
from datacommons_client.endpoints.payloads import ResolvePayload
from datacommons_client.endpoints.payloads import SparqlPayload


def test_node_payload_normalize():
    """Tests that NodePayload correctly normalizes single and multiple nodes."""
    payload = NodePayload(nodes="node1", prop="prop1")
    assert payload.nodes == ["node1"]

    payload = NodePayload(nodes=["node1", "node2"], prop="prop1")
    assert payload.nodes == ["node1", "node2"]


def test_node_payload_validate():
    """Tests that NodePayload validates its inputs correctly."""
    with pytest.raises(ValueError):
        NodePayload(nodes="node1", prop=123)  # `prop` must be a string


def test_node_payload_to_dict():
    """Tests NodePayload conversion to dictionary."""
    payload = NodePayload(nodes="node1", prop="prop1")
    assert payload.to_dict == {"nodes": ["node1"], "property": "prop1"}


def test_observation_payload_normalize():
    """Tests that ObservationPayload normalizes inputs correctly."""
    payload = ObservationPayload(
        date="LATEST",
        variable_dcids="var1",
        select=["variable", "entity"],
        entity_dcids="ent1",
    )
    assert payload.variable_dcids == ["var1"]
    assert payload.entity_dcids == ["ent1"]
    assert payload.date == ObservationDate.LATEST

    payload = ObservationPayload(
        date="all",
        variable_dcids=["var1"],
        select=["variable", "entity"],
        entity_dcids=["ent1"],
    )
    assert payload.date == ObservationDate.ALL
    assert payload.variable_dcids == ["var1"]
    assert payload.entity_dcids == ["ent1"]


def test_observation_payload_validate():
    """Tests that ObservationPayload validates its inputs."""
    with pytest.raises(ValueError):
        ObservationPayload(
            date="LATEST",
            variable_dcids="var1",
            select=["variable"],
            entity_dcids=None,
            entity_expression=None,
        )  # Requires either `entity_dcids` or `entity_expression`

    with pytest.raises(ValueError):
        ObservationPayload(
            date="LATEST",
            variable_dcids="var1",
            select=["value"],  # Missing required "variable" and "entity"
            entity_expression="expression",
        )

    with pytest.raises(ValueError):
        ObservationPayload(
            date="LATEST",
            variable_dcids="var1",
            select=["variable", "entity"],
            entity_dcids="ent1",
            entity_expression="expression",  # Both `entity_dcids` and `entity_expression` set
        )


def test_observation_payload_to_dict():
    """Tests ObservationPayload conversion to dictionary."""
    payload = ObservationPayload(
        date="LATEST",
        variable_dcids="var1",
        select=["variable", "entity"],
        entity_dcids="ent1",
    )
    assert payload.to_dict == {
        "date": ObservationDate.LATEST,
        "variable": {"dcids": ["var1"]},
        "entity": {"dcids": ["ent1"]},
        "select": ["variable", "entity"],
    }


def test_resolve_payload_normalize():
    """Tests that ResolvePayload normalizes single and multiple nodes."""
    payload = ResolvePayload(nodes="node1", expression="expr1")
    assert payload.nodes == ["node1"]

    payload = ResolvePayload(nodes=["node1", "node2"], expression="expr1")
    assert payload.nodes == ["node1", "node2"]


def test_resolve_payload_validate():
    """Tests that ResolvePayload validates its inputs correctly."""
    with pytest.raises(ValueError):
        ResolvePayload(
            nodes="node1", expression=123
        )  # `expression` must be a string


def test_resolve_payload_to_dict():
    """Tests ResolvePayload conversion to dictionary."""
    payload = ResolvePayload(nodes="node1", expression="expr1")
    assert payload.to_dict == {"nodes": ["node1"], "property": "expr1"}


def test_sparql_payload_validate():
    """Tests that SparqlPayload validates its inputs correctly."""
    with pytest.raises(ValueError):
        SparqlPayload(query="")  # Query cannot be empty

    with pytest.raises(ValueError):
        SparqlPayload(query=None)  # Query must be a non-empty string


def test_sparql_payload_to_dict():
    """Tests SparqlPayload conversion to dictionary."""
    payload = SparqlPayload(query="SELECT * WHERE {?s ?p ?o}")
    assert payload.to_dict == {"query": "SELECT * WHERE {?s ?p ?o}"}
