from unittest.mock import MagicMock

from datacommons_client.endpoints.base import API
from datacommons_client.endpoints.node import NodeEndpoint
from datacommons_client.endpoints.response import NodeResponse


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

  api_mock.post.assert_called_once_with(payload={
      "nodes": ["test_node"],
      "property": "name"
  },
                                        endpoint="node",
                                        all_pages=True,
                                        next_token=None)
  assert isinstance(response, NodeResponse)
  assert "test_node" in response.data


def test_node_endpoint_fetch_list_input():
  """Test the fetch method with list inputs for node_dcids and expression."""
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
  response = endpoint.fetch(node_dcids=["test_node1", "test_node2"],
                            expression=["name", "typeOf"])

  api_mock.post.assert_called_once_with(payload={
      "nodes": ["test_node1", "test_node2"],
      "property": "[name, typeOf]",
  },
                                        endpoint="node",
                                        all_pages=True,
                                        next_token=None)
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
  api_mock.post.assert_called_once_with(payload={
      "nodes": ["node1"],
      "property": expected_expression
  },
                                        endpoint="node",
                                        all_pages=True,
                                        next_token=None)
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
  api_mock.post.assert_called_once_with(payload={
      "nodes": ["node1"],
      "property": expected_expression
  },
                                        endpoint="node",
                                        all_pages=True,
                                        next_token=None)
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
  endpoint.fetch_property_values.assert_called_once_with(node_dcids="Class",
                                                         properties="typeOf",
                                                         out=False,
                                                         all_pages=True,
                                                         next_token=None)
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
  api_mock.post.assert_called_with(payload={
      "nodes": ["node1"],
      "property": "->name"
  },
                                   endpoint="node",
                                   all_pages=True,
                                   next_token=None)

  # List input
  response = endpoint.fetch_property_values(node_dcids="node1",
                                            properties=["name", "typeOf"],
                                            constraints=None,
                                            out=True)
  api_mock.post.assert_called_with(payload={
      "nodes": ["node1"],
      "property": "->[name, typeOf]"
  },
                                   endpoint="node",
                                   all_pages=True,
                                   next_token=None)
