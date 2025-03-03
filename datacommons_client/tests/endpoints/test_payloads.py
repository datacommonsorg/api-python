import pytest

from datacommons_client.endpoints.payloads import NodeRequestPayload
from datacommons_client.endpoints.payloads import ObservationDate
from datacommons_client.endpoints.payloads import ObservationRequestPayload
from datacommons_client.endpoints.payloads import ObservationSelect
from datacommons_client.endpoints.payloads import ResolveRequestPayload
from datacommons_client.utils.error_handling import InvalidObservationSelectError


def test_node_payload_normalize():
  """Tests that NodeRequestPayload correctly normalizes single and multiple node_dcids."""
  payload = NodeRequestPayload(node_dcids="node1", expression="prop1")
  assert payload.node_dcids == ["node1"]

  payload = NodeRequestPayload(node_dcids=["node1", "node2"],
                               expression="prop1")
  assert payload.node_dcids == ["node1", "node2"]


def test_node_payload_validate():
  """Tests that NodeRequestPayload validates its inputs correctly."""
  with pytest.raises(ValueError):
    NodeRequestPayload(node_dcids="node1",
                       expression=123)  # `expression` must be a string


def test_node_payload_to_dict():
  """Tests NodeRequestPayload conversion to dictionary."""
  payload = NodeRequestPayload(node_dcids="node1", expression="prop1")
  assert payload.to_dict == {"nodes": ["node1"], "property": "prop1"}


def test_observation_payload_normalize():
  """Tests that ObservationRequestPayload normalizes inputs correctly."""
  payload = ObservationRequestPayload(
      date="LATEST",
      variable_dcids="var1",
      select=["variable", "entity"],
      entity_dcids="ent1",
  )
  assert payload.variable_dcids == ["var1"]
  assert payload.entity_dcids == ["ent1"]
  assert payload.date == ObservationDate.LATEST

  payload = ObservationRequestPayload(
      date="all",
      variable_dcids=["var1"],
      select=["variable", "entity"],
      entity_dcids=["ent1"],
  )
  assert payload.date == ObservationDate.ALL
  assert payload.variable_dcids == ["var1"]
  assert payload.entity_dcids == ["ent1"]


def test_observation_select_invalid_value():
  """Tests that an invalid ObservationSelect value raises InvalidObservationSelectError."""
  with pytest.raises(
      InvalidObservationSelectError,
      match=
      r"Invalid `select` field: 'invalid'. Only date, variable, entity, value are allowed.",
  ):
    ObservationSelect("invalid")


def test_observation_payload_validate():
  """Tests that ObservationRequestPayload validates its inputs."""
  with pytest.raises(ValueError):
    ObservationRequestPayload(
        date="LATEST",
        variable_dcids="var1",
        select=["variable"],
        entity_dcids=None,
        entity_expression=None,
    )  # Requires either `entity_dcids` or `entity_expression`

  with pytest.raises(ValueError):
    ObservationRequestPayload(
        date="LATEST",
        variable_dcids="var1",
        select=["value"],  # Missing required "variable" and "entity"
        entity_expression="expression",
    )

  with pytest.raises(ValueError):
    ObservationRequestPayload(
        date="LATEST",
        variable_dcids="var1",
        select=["variable", "entity"],
        entity_dcids="ent1",
        entity_expression=
        "expression",  # Both `entity_dcids` and `entity_expression` set
    )


def test_observation_payload_to_dict():
  """Tests ObservationRequestPayload conversion to dictionary."""
  payload = ObservationRequestPayload(
      date="LATEST",
      variable_dcids="var1",
      select=["variable", "entity"],
      entity_dcids="ent1",
  )
  assert payload.to_dict == {
      "date": ObservationDate.LATEST,
      "variable": {
          "dcids": ["var1"]
      },
      "entity": {
          "dcids": ["ent1"]
      },
      "select": ["variable", "entity"],
  }


def test_resolve_payload_normalize():
  """Tests that ResolveRequestPayload normalizes single and multiple node_dcids."""
  payload = ResolveRequestPayload(node_dcids="node1", expression="expr1")
  assert payload.node_dcids == ["node1"]

  payload = ResolveRequestPayload(node_dcids=["node1", "node2"],
                                  expression="expr1")
  assert payload.node_dcids == ["node1", "node2"]


def test_resolve_payload_validate():
  """Tests that ResolveRequestPayload validates its inputs correctly."""
  with pytest.raises(ValueError):
    ResolveRequestPayload(node_dcids="node1",
                          expression=123)  # `expression` must be a string


def test_resolve_payload_to_dict():
  """Tests ResolveRequestPayload conversion to dictionary."""
  payload = ResolveRequestPayload(node_dcids="node1", expression="expr1")
  assert payload.to_dict == {"nodes": ["node1"], "property": "expr1"}
