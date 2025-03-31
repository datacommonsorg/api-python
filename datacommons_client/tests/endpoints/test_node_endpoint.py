from unittest.mock import MagicMock
from unittest.mock import patch

from datacommons_client.endpoints.base import API
from datacommons_client.endpoints.node import NodeEndpoint
from datacommons_client.endpoints.response import NodeResponse
from datacommons_client.models.graph import Parent
from datacommons_client.models.node import Node


def test_node_endpoint_initialization():
  """Test if the NodeEndpoint initializes correctly."""
  api_mock = MagicMock(spec=API)
  endpoint = NodeEndpoint(api=api_mock)

  assert endpoint.endpoint == "node"
  assert endpoint.api == api_mock


def test_node_endpoint_fetch():
  """Test the fetch method of NodeEndpoint."""
  api_mock = MagicMock(spec=API)
  api_mock.post.return_value = {
      "data": {
          "test_node": {
              "properties": {
                  "name": "Test"
              }
          }
      }
  }

  endpoint = NodeEndpoint(api=api_mock)
  response = endpoint.fetch(node_dcids="test_node", expression="name")

  api_mock.post.assert_called_once_with(
      payload={
          "nodes": ["test_node"],
          "property": "name"
      },
      endpoint="node",
      all_pages=True,
      next_token=None,
  )
  assert isinstance(response, NodeResponse)
  assert "test_node" in response.data


def test_node_endpoint_fetch_property_labels():
  """Test fetch_property_labels method."""
  api_mock = MagicMock(spec=API)
  endpoint = NodeEndpoint(api=api_mock)
  endpoint.fetch = MagicMock(return_value=NodeResponse(
      data={"node1": {
          "properties": {}
      }}))

  response = endpoint.fetch_property_labels(node_dcids="node1", out=False)
  endpoint.fetch.assert_called_once_with(node_dcids="node1",
                                         expression="<-",
                                         all_pages=True,
                                         next_token=None)
  assert isinstance(response, NodeResponse)
  assert "node1" in response.data


def test_node_endpoint_fetch_property_values_out():
  """Test fetch_property_values method with constraints and direction (out)"""

  api_mock = MagicMock(spec=API)
  api_mock.post.return_value = {
      "data": {
          "node1": {
              "properties": {
                  "name": "Test"
              }
          }
      }
  }

  endpoint = NodeEndpoint(api=api_mock)
  response = endpoint.fetch_property_values(node_dcids="node1",
                                            properties="name",
                                            constraints="typeOf:City",
                                            out=True)

  expected_expression = "->name{typeOf:City}"
  api_mock.post.assert_called_once_with(
      payload={
          "nodes": ["node1"],
          "property": expected_expression
      },
      endpoint="node",
      all_pages=True,
      next_token=None,
  )
  assert isinstance(response, NodeResponse)
  assert "node1" in response.data


def test_node_endpoint_fetch_property_values_in():
  """Test fetch_property_values method with constraints and direction (in)"""

  api_mock = MagicMock(spec=API)
  api_mock.post.return_value = {
      "data": {
          "node1": {
              "properties": {
                  "name": "Test"
              }
          }
      }
  }

  endpoint = NodeEndpoint(api=api_mock)
  response = endpoint.fetch_property_values(node_dcids="node1",
                                            properties="name",
                                            constraints="typeOf:City",
                                            out=False)

  expected_expression = "<-name{typeOf:City}"
  api_mock.post.assert_called_once_with(
      payload={
          "nodes": ["node1"],
          "property": expected_expression
      },
      endpoint="node",
      all_pages=True,
      next_token=None,
  )
  assert isinstance(response, NodeResponse)
  assert "node1" in response.data


def test_node_endpoint_fetch_all_classes():
  """Test fetch_all_classes method."""
  api_mock = MagicMock(spec=API)
  endpoint = NodeEndpoint(api=api_mock)
  endpoint.fetch_property_values = MagicMock(return_value=NodeResponse(
      data={"Class": {
          "arcs": {}
      }}))

  response = endpoint.fetch_all_classes()
  endpoint.fetch_property_values.assert_called_once_with(
      node_dcids="Class",
      properties="typeOf",
      out=False,
      all_pages=True,
      next_token=None,
  )
  assert isinstance(response, NodeResponse)
  assert "Class" in response.data


def test_node_endpoint_fetch_property_values_string_vs_list():
  """Test fetch_property_values with string and list expressions."""
  api_mock = MagicMock(spec=API)
  api_mock.post.return_value = {
      "data": {
          "node1": {
              "properties": {
                  "name": "Test"
              }
          }
      }
  }

  endpoint = NodeEndpoint(api=api_mock)

  # String input
  response = endpoint.fetch_property_values(node_dcids="node1",
                                            properties="name",
                                            constraints=None,
                                            out=True)
  api_mock.post.assert_called_with(
      payload={
          "nodes": ["node1"],
          "property": "->name"
      },
      endpoint="node",
      all_pages=True,
      next_token=None,
  )

  # List input
  response = endpoint.fetch_property_values(node_dcids="node1",
                                            properties=["name", "typeOf"],
                                            constraints=None,
                                            out=True)
  api_mock.post.assert_called_with(
      payload={
          "nodes": ["node1"],
          "property": "->[name, typeOf]"
      },
      endpoint="node",
      all_pages=True,
      next_token=None,
  )


@patch("datacommons_client.utils.graph.build_parents_dictionary")
def test_fetch_entity_parents_as_dict(mock_build_parents_dict):
  """Test fetch_entity_parents with dictionary output."""
  api_mock = MagicMock()
  endpoint = NodeEndpoint(api=api_mock)

  api_mock.post.return_value = {
      "data": {
          "X": {
              "properties": {
                  "containedInPlace": []
              }
          }
      }
  }
  endpoint.fetch_property_values = MagicMock()
  endpoint.fetch_property_values.return_value.get_properties.return_value = {
      "X": Node("X", "X name", types=["Country"])
  }

  result = endpoint.fetch_entity_parents("X", as_dict=True)
  assert result == {"X": [{"dcid": "X", "name": "X name", "type": "Country"}]}

  endpoint.fetch_property_values.assert_called_once_with(
      node_dcids="X", properties="containedInPlace")


@patch("datacommons_client.utils.graph.build_parents_dictionary")
def test_fetch_entity_parents_as_objects(mock_build_parents_dict):
  """Test fetch_entity_parents with raw Parent object output."""
  api_mock = MagicMock()
  endpoint = NodeEndpoint(api=api_mock)

  # Simulate what fetch_property_values().get_properties() would return
  endpoint.fetch_property_values = MagicMock()
  endpoint.fetch_property_values.return_value.get_properties.return_value = {
      "X": Node("X", "X name", types=["Country"])
  }

  # Mock output of build_parents_dictionary
  parent_obj = Node("X", "X name", types=["Country"])
  mock_build_parents_dict.return_value = {"X": [parent_obj]}

  result = endpoint.fetch_entity_parents("X", as_dict=False)

  assert isinstance(result, dict)
  assert "X" in result
  assert isinstance(result["X"][0], Parent)

  endpoint.fetch_property_values.assert_called_once_with(
      node_dcids="X", properties="containedInPlace")


@patch("datacommons_client.endpoints.node.fetch_parents_lru")
def test_fetch_parents_cached_delegates_to_lru(mock_fetch_lru):
  mock_fetch_lru.return_value = (Parent("B", "B name", "Region"),)
  endpoint = NodeEndpoint(api=MagicMock())

  result = endpoint._fetch_parents_cached("X")

  assert isinstance(result, tuple)
  assert result[0].dcid == "B"
  mock_fetch_lru.assert_called_once_with(endpoint, "X")


@patch("datacommons_client.endpoints.node.flatten_ancestry")
@patch("datacommons_client.endpoints.node.build_ancestry_map")
def test_fetch_entity_ancestry_flat(mock_build_map, mock_flatten):
  """Test fetch_entity_ancestry with flat structure (as_tree=False)."""
  mock_build_map.return_value = (
      "X",
      {
          "X": [Parent("A", "A name", "Country")],
          "A": [],
      },
  )
  mock_flatten.return_value = [{
      "dcid": "A",
      "name": "A name",
      "type": "Country"
  }]

  endpoint = NodeEndpoint(api=MagicMock())
  result = endpoint.fetch_entity_ancestry("X", as_tree=False)

  assert result == {"X": [{"dcid": "A", "name": "A name", "type": "Country"}]}
  mock_build_map.assert_called_once()
  mock_flatten.assert_called_once()


@patch("datacommons_client.endpoints.node.build_ancestry_tree")
@patch("datacommons_client.endpoints.node.build_ancestry_map")
def test_fetch_entity_ancestry_tree(mock_build_map, mock_build_tree):
  """Test fetch_entity_ancestry with tree structure (as_tree=True)."""
  mock_build_map.return_value = (
      "Y",
      {
          "Y": [Parent("Z", "Z name", "Region")],
          "Z": [],
      },
  )
  mock_build_tree.return_value = {
      "dcid":
          "Y",
      "name":
          None,
      "type":
          None,
      "parents": [{
          "dcid": "Z",
          "name": "Z name",
          "type": "Region",
          "parents": []
      }],
  }

  endpoint = NodeEndpoint(api=MagicMock())
  result = endpoint.fetch_entity_ancestry("Y", as_tree=True)

  assert "Y" in result
  assert result["Y"]["dcid"] == "Y"
  assert result["Y"]["parents"][0]["dcid"] == "Z"
  mock_build_map.assert_called_once()
  mock_build_tree.assert_called_once_with("Y", mock_build_map.return_value[1])
