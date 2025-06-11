from datacommons_client.models.node import Arcs
from datacommons_client.models.node import Node
from datacommons_client.models.node import NodeGroup
from datacommons_client.models.node import Properties


def test_node_model_validation():
  """Test that Node.model_validate parses data correctly."""
  json_data = {
      "dcid": "node123",
      "name": "Test Node",
      "provenanceId": "prov123",
      "types": ["TypeA", "TypeB"],
      "value": "42",
  }
  node = Node.model_validate(json_data)
  assert node.dcid == "node123"
  assert node.name == "Test Node"
  assert node.provenanceId == "prov123"
  assert node.types == ["TypeA", "TypeB"]
  assert node.value == "42"


def test_node_model_validation_partial():
  """Test Node.model_validate with partial data."""
  json_data = {
      "dcid": "node123",
  }
  node = Node.model_validate(json_data)
  assert node.dcid == "node123"
  assert node.name is None
  assert node.provenanceId is None
  assert node.types is None
  assert node.value is None


def test_nodegroup_model_validation():
  """Test that NodeGroup.model_validate parses data correctly."""
  json_data = {
      "nodes": [
          {
              "dcid": "node1",
              "name": "Node 1"
          },
          {
              "dcid": "node2",
              "name": "Node 2"
          },
      ]
  }
  node_group = NodeGroup.model_validate(json_data)
  assert len(node_group.nodes) == 2
  assert node_group.nodes[0].dcid == "node1"
  assert node_group.nodes[1].name == "Node 2"


def test_nodegroup_model_validation_empty():
  """Test NodeGroup.model_validate with empty data."""
  json_data = {}
  node_group = NodeGroup.model_validate(json_data)
  assert len(node_group.nodes) == 0


def test_arcs_model_validation():
  """Test that Arcs.model_validate parses data correctly."""
  json_data = {
      "arcs": {
          "label1": {
              "nodes": [{
                  "dcid": "node1"
              }, {
                  "dcid": "node2"
              }]
          },
          "label2": {
              "nodes": [{
                  "dcid": "node3"
              }]
          },
      }
  }
  arcs = Arcs.model_validate(json_data)
  assert len(arcs.arcs) == 2
  assert "label1" in arcs.arcs
  assert len(arcs.arcs["label1"].nodes) == 2
  assert arcs.arcs["label1"].nodes[0].dcid == "node1"
  assert len(arcs.arcs["label2"].nodes) == 1
  assert arcs.arcs["label2"].nodes[0].dcid == "node3"


def test_arcs_model_validation_empty():
  """Test Arcs.model_validate with empty data."""
  json_data = {}
  arcs = Arcs.model_validate(json_data)
  assert len(arcs.arcs) == 0


def test_properties_model_validation():
  """Test that Properties.model_validate parses data correctly."""
  json_data = {"properties": ["prop1", "prop2", "prop3"]}
  properties = Properties.model_validate(json_data)
  assert len(properties.properties) == 3
  assert properties.properties == ["prop1", "prop2", "prop3"]


def test_properties_model_validation_empty():
  """Test Properties.model_validate with empty data."""
  json_data = {}
  properties = Properties.model_validate(json_data)
  assert properties.properties is None
