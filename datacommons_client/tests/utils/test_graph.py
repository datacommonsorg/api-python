from collections import defaultdict
from unittest.mock import MagicMock

from datacommons_client.models.node import Node
from datacommons_client.utils.graph import _assemble_tree
from datacommons_client.utils.graph import _fetch_relationship_uncached
from datacommons_client.utils.graph import _postorder_nodes
from datacommons_client.utils.graph import build_graph_map
from datacommons_client.utils.graph import build_relationship_tree
from datacommons_client.utils.graph import fetch_relationship_lru
from datacommons_client.utils.graph import flatten_relationship


def test_fetch_parents_uncached_returns_data():
  """Test _fetch_parents_uncached delegates to endpoint correctly."""
  endpoint = MagicMock()
  endpoint.fetch_place_parents.return_value.get.return_value = [
      Node(dcid="parent1", name="Parent 1", types=["Country"])
  ]

  result = _fetch_relationship_uncached(endpoint,
                                        "test_dcid",
                                        contained_type=None,
                                        relationship="parents")
  assert isinstance(result, list)
  assert result[0].dcid == "parent1"
  endpoint.fetch_place_parents.assert_called_once_with(
      "test_dcid",
      as_dict=False,
  )


def test_fetch_relationship_lru_caches_results():
  """Test fetch_relationship_lru uses LRU cache and returns list."""
  endpoint = MagicMock()
  endpoint.fetch_place_parents.return_value.get.return_value = [
      Node(dcid="parentX", name="Parent X", types=["Region"])
  ]

  result1 = fetch_relationship_lru(endpoint,
                                   "nodeA",
                                   contained_type=None,
                                   relationship="parents")
  result2 = fetch_relationship_lru(endpoint,
                                   "nodeA",
                                   contained_type=None,
                                   relationship="parents")
  fetch_relationship_lru(endpoint,
                         "nodeA",
                         contained_type=None,
                         relationship="parents")

  assert isinstance(result1, list)
  assert result1[0].dcid == "parentX"
  assert result1 == result2
  assert endpoint.fetch_place_parents.call_count == 1


def test_build_ancestry_map_linear_tree():
  """A -> B -> C"""

  def fetch_mock(dcid):
    return {
        "C": [Node("B", "Node B", ["Type"])],
        "B": [Node("A", "Node A", ["Type"])],
        "A": [],
    }.get(dcid, [])

  root, graph = build_graph_map("C", fetch_mock, max_workers=2)

  assert root == "C"
  assert set(graph.keys()) == {"C", "B", "A"}
  assert graph["C"][0].dcid == "B"
  assert graph["B"][0].dcid == "A"
  assert graph["A"] == []


def test_build_ancestry_map_branching_graph():
  r"""
      Graph:
          F
         / \
        D   E
       / \ /
      B  C
       \/
        A
      """

  def fetch_mock(dcid):
    return {
        "A": (Node("B", "Node B", "Type"), Node("C", "Node C", "Type")),
        "B": (Node("D", "Node D", "Type"),),
        "C": (Node("D", "Node D", "Type"), Node("E", "Node E", "Type")),
        "D": (Node("F", "Node F", "Type"),),
        "E": (Node("F", "Node F", "Type"),),
        "F": tuple(),
    }.get(dcid, tuple())

  root, ancestry = build_graph_map("A", fetch_mock, max_workers=4)

  assert root == "A"
  assert set(ancestry.keys()) == {"A", "B", "C", "D", "E", "F"}
  assert [p.dcid for p in ancestry["A"]] == ["B", "C"]  # A has two parents
  assert [p.dcid for p in ancestry["B"]] == ["D"]  # B has one parent
  assert [p.dcid for p in ancestry["C"]] == ["D", "E"]  # C has two parents
  assert [p.dcid for p in ancestry["D"]] == ["F"]  # D has one parent
  assert [p.dcid for p in ancestry["E"]] == ["F"]  # E has one parent
  assert ancestry["F"] == []  # F has no parents


def test_build_ancestry_map_cycle_detection():
  """
    Graph with a cycle:
        A -> B -> C -> A
    (Should not loop infinitely)
    """

  call_count = defaultdict(int)

  def fetch_mock(dcid):
    call_count[dcid] += 1
    return {
        "A": (Node("B", "B", "Type"),),
        "B": (Node("C", "C", "Type"),),
        "C": (Node("A", "A", "Type"),),  # Cycle back to A
    }.get(dcid, tuple())

  root, ancestry = build_graph_map("A", fetch_mock, max_workers=2)

  assert root == "A"  # Since we start from A
  assert set(ancestry.keys()) == {"A", "B", "C"}
  assert [p.dcid for p in ancestry["A"]] == ["B"]  # A points to B
  assert [p.dcid for p in ancestry["B"]] == ["C"]  # B points to C
  assert [p.dcid for p in ancestry["C"]] == ["A"
                                            ]  # C points back to A but it's ok

  # Check that each node was fetched only once (particularly for A to avoid infinite loop)
  assert call_count["A"] == 1
  assert call_count["B"] == 1
  assert call_count["C"] == 1


def test_postorder_nodes_simple_graph():
  """Test postorder traversal on a simple graph."""
  ancestry = {
      "C": [Node("B", "B", "Type")],
      "B": [Node("A", "A", "Type")],
      "A": [],
  }

  order = _postorder_nodes("C", ancestry)
  assert order == ["A", "B", "C"]

  new_order = _postorder_nodes("B", ancestry)
  assert new_order == ["A", "B"]


def test_postorder_nodes_ignores_disconnected():
  """
    Graph:
        A <- B <- C
        D (disconnected)
    """
  graph = {
      "A": [Node("B", "B", ["Type"])],
      "B": [Node("C", "C", ["Type"])],
      "C": [],
      "D": [Node("Z", "Z", ["Type"])],
  }
  order = _postorder_nodes("A", graph)
  assert order == ["C", "B", "A"]
  assert "D" not in order


def test_assemble_tree_creates_nested_structure():
  """Test _assemble_tree creates a nested structure."""
  ancestry = {
      "C": [Node("B", "Node B", "Type")],
      "B": [Node("A", "Node A", "Type")],
      "A": [],
  }
  postorder = ["A", "B", "C"]
  tree = _assemble_tree(postorder, ancestry, relationship_key="parents")

  assert tree["dcid"] == "C"
  assert tree["parents"][0]["dcid"] == "B"
  assert tree["parents"][0]["parents"][0]["dcid"] == "A"


def test_postorder_nodes_ignores_unreachable_nodes():
  """
    Graph:
        A → B → C
    Ancestry map also includes D (unconnected)
    """
  ancestry = {
      "A": [Node("B", "B", "Type")],
      "B": [Node("C", "C", "Type")],
      "C": [],
      "D": [Node("X", "X", "Type")],
  }

  postorder = _postorder_nodes("A", ancestry)

  # Only nodes reachable from A should be included
  assert postorder == ["C", "B", "A"]
  assert "D" not in postorder


def test_assemble_tree_shared_parent_not_duplicated():
  """
    Structure:
        A → C
        B → C
    Both A and B have same parent C
    """

  ancestry = {
      "A": [Node("C", "C name", "City")],
      "B": [Node("C", "C name", "City")],
      "C": [],
  }

  postorder = ["C", "A", "B"]  # C first to allow bottom-up build
  tree = _assemble_tree(postorder, ancestry, relationship_key="parents")

  assert tree["dcid"] == "B"
  assert len(tree["parents"]) == 1
  assert tree["parents"][0]["dcid"] == "C"

  # Confirm C only appears once
  assert tree["parents"][0] is not None
  assert tree["parents"][0]["name"] == "C name"


def test_build_ancestry_tree_nested_output():
  """Test build_ancestry_tree creates a nested structure."""
  ancestry = {
      "C": [Node("B", "B", "Type")],
      "B": [Node("A", "A", "Type")],
      "A": [],
  }

  tree = build_relationship_tree("C", ancestry, relationship_key="parents")

  assert tree["dcid"] == "C"
  assert tree["parents"][0]["dcid"] == "B"
  assert tree["parents"][0]["parents"][0]["dcid"] == "A"


def test_flatten_ancestry_deduplicates():
  """Test flatten_ancestry deduplicates parents."""

  ancestry = {
      "X": [Node("A", "A", types=["Country"])],
      "Y": [Node("A", "A", types=["Country"]),
            Node("B", "B", types=["City"])],
  }

  flat = flatten_relationship(ancestry)

  assert {"dcid": "A", "name": "A", "types": ["Country"]} in flat
  assert {"dcid": "B", "name": "B", "types": ["City"]} in flat
  assert len(flat) == 2
