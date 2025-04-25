from collections import deque
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from functools import lru_cache
from typing import Callable, Literal, Optional, TypeAlias

from datacommons_client.models.node import Node

GRAPH_MAX_WORKERS = 10

RelationMap: TypeAlias = dict[str, list[Node]]
AncestorsMap = RelationMap
DescendantsMap = RelationMap

# -- -- Fetch tools -- --


def _fetch_relationship_uncached(
    endpoint,
    dcid: str,
    contained_type: str | None,
    relationship: Literal["parents", "children"],
) -> list[Node]:
  """Fetches the immediate parents/children of a given DCID from the endpoint, without caching.

    This function performs a direct, uncached call to the API. It exists
    primarily to serve as the internal, cache-free fetch use by functions with lru.

    By isolating the pure fetch logic here, we ensure that caching is handled separately
    and cleanly via `@lru_cache`, which requires its wrapped
    function to be deterministic and side-effect free.

    Args:
        endpoint: A client object with a `fetch_entity_parents` and `fetch_entity_children` method.
        dcid (str): The entity ID for which to fetch parents.
        contained_type (str): The type of the entity to be fetched.
        relationship (str): The type of relationship to fetch, either "parents" or "children".
    Returns:
        A list of Node objects.
    """

  if relationship == "parents":
    result = endpoint.fetch_place_parents(dcid, as_dict=False).get(dcid, [])

  else:
    result = endpoint.fetch_place_children(dcid,
                                           children_type=contained_type,
                                           as_dict=False).get(dcid, [])

  return result if isinstance(result, list) else [result]


@lru_cache(maxsize=512)
def fetch_relationship_lru(
    endpoint,
    dcid: str,
    contained_type: str | None,
    relationship: Literal["parents", "children"],
) -> list[Node]:
  """Fetches parents of a DCID using an LRU cache for improved performance.
    Args:
        endpoint: A Node client object.
        dcid (str): The entity ID to fetch parents/children for.
        contained_type (str): The type of the entity to be fetched.
        relationship (str): The type of relationship to fetch, either "parents" or "children".
    Returns:
        A list of `Node` objects corresponding to the entity's parents or children.
    """
  return _fetch_relationship_uncached(
      endpoint=endpoint,
      dcid=dcid,
      contained_type=contained_type,
      relationship=relationship,
  )


# -- -- Ancestry tools -- --


def build_graph_map(
    root: str,
    fetch_fn: Callable[..., tuple[Node, ...]],
    *,
    max_workers: Optional[int] = GRAPH_MAX_WORKERS,
) -> tuple[str, RelationMap]:
  """Constructs a complete ancestry/descendancy map for the root node using parallel
       Breadth-First Search (BFS).

    Traverses the graph from the root node, discovering all parent/children
    relationships (depending on the fetch_fn) by fetching in parallel.

    Args:
        root (str): The DCID of the root entity to start from.
        fetch_fn (Callable): A function that takes a DCID and returns Node tuples.
        max_workers (Optional[int]): Max number of threads to use for parallel fetching.
          Optional, defaults to `PARENTS_MAX_WORKERS`.

    Returns:
        A tuple containing:
            - The original root DCID.
            - A dictionary mapping each DCID to a Node list.
    """
  graph_map: RelationMap = {}
  visited: set[str] = set()
  in_progress: dict[str, Future] = {}

  original_root = root

  with ThreadPoolExecutor(max_workers=max_workers) as executor:
    queue = deque([root])

    # Standard BFS loop, but fetches are executed in parallel threads
    while queue or in_progress:
      # Submit fetch tasks for all nodes in the queue
      while queue:
        dcid = queue.popleft()
        # Check if the node has already been visited or is in progress
        if dcid not in visited and dcid not in in_progress:
          # Submit the fetch task
          in_progress[dcid] = executor.submit(fetch_fn, dcid=dcid)

      # Check if any futures are still in progress
      if not in_progress:
        continue

      # Wait for at least one future to complete
      done_futures, _ = wait(in_progress.values(), return_when=FIRST_COMPLETED)

      # Find which DCIDs have completed
      completed_dcids = [
          dcid for dcid, future in in_progress.items() if future in done_futures
      ]

      # Process completed fetches and enqueue any unseen parents
      for dcid in completed_dcids:
        future = in_progress.pop(dcid)
        nodes = list(future.result())
        graph_map[dcid] = nodes
        visited.add(dcid)

        for node in nodes:
          if (node and node.dcid not in visited and
              node.dcid not in in_progress):
            queue.append(node.dcid)

  return original_root, graph_map


def _postorder_nodes(root: str, graph: RelationMap) -> list[str]:
  """Generates a postorder list of all nodes reachable from the root.

    Postorder ensures children are processed before their parents. That way the tree
    is built bottom-up.

    Args:
        root (str): The root DCID to start traversal from.
        graph (RelationMap): The ancestry/descendancy map.
    Returns:
        A list of DCIDs in postorder (i.e children before parents).
    """
  # Initialize stack and postorder list
  stack, postorder, seen = [root], [], set()

  # Traverse the graph using a stack
  while stack:
    node = stack.pop()
    # Skip if already seen
    if node in seen:
      continue
    seen.add(node)
    postorder.append(node)
    # Push all unvisited Nodes onto the stack
    for relation in graph.get(node, []):
      if not relation:
        continue
      relation_dcid = relation.dcid
      if relation_dcid not in seen:
        stack.append(relation_dcid)

  # Reverse to get postorder relative to the adjacency direction
  return list(reversed(postorder))


def _assemble_tree(postorder: list[str], ancestry: RelationMap,
                   relationship_key: str) -> dict:
  """Builds a nested dictionary tree from a Node list and RelationMa[.
    Constructs a nested representation of the graph, ensuring that parents/children
    are embedded after their root Node (which is enabled by postorder).
    Args:
        postorder (list[str]): List of node DCIDs in postorder.
        ancestry (RelationMap): Map from DCID to list of Node objects.
        relationship_key (str): The key to use for the relationship in the tree.
    Returns:
        A nested dictionary representing the ancestry tree rooted at the last postorder node.
    """
  tree_cache: dict[str, dict] = {}

  for node in postorder:
    # Initialize the node dictionary.
    node_dict = {"dcid": node, "name": None, "type": None, relationship_key: []}

    # For each relationship of the current node, fetch its details and add it to the node_dict.
    for relationship in ancestry.get(node, []):
      if not relationship:
        continue
      relationship_dcid = relationship.dcid
      name = relationship.name
      entity_type = relationship.types

      # If the node is not already in the cache, add it.
      if relationship_dcid not in tree_cache:
        tree_cache[relationship_dcid] = {
            "dcid": relationship_dcid,
            "name": name,
            "type": entity_type,
            relationship_key: [],
        }

      relationship_node = tree_cache[relationship_dcid]

      # Ensure name/type are up to date (in case of duplicates)
      relationship_node["name"] = name
      relationship_node["type"] = entity_type
      node_dict[relationship_key].append(relationship_node)

    tree_cache[node] = node_dict

  # The root node is the last one in postorder, that's what gets returned
  return tree_cache[postorder[-1]]


def build_relationship_tree(root: str, graph: RelationMap,
                            relationship_key: str) -> dict:
  """Builds a nested ancestry tree from an ancestry map.
    Args:
        root (str): The DCID of the root node.
        graph (RelationMap): A dictionary mapping DCIDs to lists of Node objects.
        relationship_key (str): The key to use for the relationship in the tree.
    Returns:
        A nested dictionary tree rooted at the specified DCID.
    """
  postorder = _postorder_nodes(root, graph)
  return _assemble_tree(postorder, graph, relationship_key=relationship_key)


def flatten_relationship(graph: RelationMap) -> list[dict[str, str]]:
  """Flattens the RelationMap into a deduplicated list of parent/child records.
    Args:
        graph (GraphMap): mapping of DCIDs to lists of Node objects.
    Returns:
        A list of dictionaries with keys 'dcid', 'name', and 'type', containing
        each unique parent/child in the graph.
    """

  flat: list = []
  seen: set[str] = set()
  for relationships in graph.values():
    for relationship in relationships:
      if relationship and relationship.dcid not in seen:
        seen.add(relationship.dcid)
        flat.append(relationship.to_dict())
  return flat
