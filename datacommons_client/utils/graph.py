from collections import deque
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from functools import lru_cache
from typing import Callable, Optional

from datacommons_client.models.graph import AncestryMap
from datacommons_client.models.graph import Parent

PARENTS_MAX_WORKERS = 10

# -- -- Fetch tools -- --


def _fetch_parents_uncached(endpoint, dcid: str) -> list[Parent]:
  """Fetches the immediate parents of a given DCID from the endpoint, without caching.

    This function performs a direct, uncached call to the API. It exists
    primarily to serve as the internal, cache-free fetch used by `fetch_parents_lru`, which
    applies LRU caching on top of this raw access function.

    By isolating the pure fetch logic here, we ensure that caching is handled separately
    and cleanly via `@lru_cache` on `fetch_parents_lru`, which requires its wrapped
    function to be deterministic and side-effect free.

    Args:
        endpoint: A client object with a `fetch_entity_parents` method.
        dcid (str): The entity ID for which to fetch parents.
    Returns:
        A list of parent dictionaries, each containing 'dcid', 'name', and 'type'.
    """
  return endpoint.fetch_entity_parents(dcid, as_dict=False).get(dcid, [])


@lru_cache(maxsize=512)
def fetch_parents_lru(endpoint, dcid: str) -> tuple[Parent, ...]:
  """Fetches parents of a DCID using an LRU cache for improved performance.
    Args:
        endpoint: A client object with a `fetch_entity_parents` method.
        dcid (str): The entity ID to fetch parents for.
    Returns:
        A tuple of `Parent` objects corresponding to the entity’s parents.
    """
  parents = _fetch_parents_uncached(endpoint, dcid)
  return tuple(p for p in parents)


# -- -- Ancestry tools -- --
def build_parents_dictionary(data: dict) -> dict[str, list[Parent]]:
  """Transforms a dictionary of entities and their parents into a structured
    dictionary mapping each entity to its list of Parents.

    Args:
        data (dict): The properties dictionary of a Node.fetch_property_values call.

    Returns:
        dict[str, list[Parent]]: A dictionary where each key is an entity DCID
        and the value is a list of Parent objects representing its parents.

    """

  result: dict[str, list[Parent]] = {}

  for entity, properties in data.items():
    if not isinstance(properties, list):
      properties = [properties]

    for parent in properties:
      parent_type = parent.types[0] if len(parent.types) == 1 else parent.types
      result.setdefault(entity, []).append(
          Parent(dcid=parent.dcid, name=parent.name, type=parent_type))
  return result


def build_ancestry_map(
    root: str,
    fetch_fn: Callable[[str], tuple[Parent, ...]],
    max_workers: Optional[int] = PARENTS_MAX_WORKERS,
) -> tuple[str, AncestryMap]:
  """Constructs a complete ancestry map for the root node using parallel
       Breadth-First Search (BFS).

    Traverses the ancestry graph upward from the root node, discovering all parent
    relationships by fetching in parallel.

    Args:
        root (str): The DCID of the root entity to start from.
        fetch_fn (Callable): A function that takes a DCID and returns a Parent tuple.
        max_workers (Optional[int]): Max number of threads to use for parallel fetching.
          Optional, defaults to `PARENTS_MAX_WORKERS`.

    Returns:
        A tuple containing:
            - The original root DCID.
            - A dictionary mapping each DCID to a list of its `Parent`s.
    """
  ancestry: AncestryMap = {}
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
          in_progress[dcid] = executor.submit(fetch_fn, dcid)

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
        parents = list(future.result())
        ancestry[dcid] = parents
        visited.add(dcid)

        for parent in parents:
          if parent.dcid not in visited and parent.dcid not in in_progress:
            queue.append(parent.dcid)

  return original_root, ancestry


def _postorder_nodes(root: str, ancestry: AncestryMap) -> list[str]:
  """Generates a postorder list of all nodes reachable from the root.

    Postorder ensures children are processed before their parents. That way the tree
    is built bottom-up.

    Args:
        root (str): The root DCID to start traversal from.
        ancestry (AncestryMap): The ancestry graph.
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
    # Push all unvisited parents onto the stack (i.e climb up the graph, child -> parent)
    for parent in ancestry.get(node, []):
      parent_dcid = parent.dcid
      if parent_dcid not in seen:
        stack.append(parent_dcid)

  # Reverse the list so that parents come after their children (i.e postorder)
  return list(reversed(postorder))


def _assemble_tree(postorder: list[str], ancestry: AncestryMap) -> dict:
  """Builds a nested dictionary tree from a postorder node list and ancestry map.
    Constructs a nested representation of the ancestry graph, ensuring that parents
    are embedded after their children (which is enabled by postorder).
    Args:
        postorder (list[str]): List of node DCIDs in postorder.
        ancestry (AncestryMap): Map from DCID to list of Parent objects.
    Returns:
        A nested dictionary representing the ancestry tree rooted at the last postorder node.
    """
  tree_cache: dict[str, dict] = {}

  for node in postorder:
    # Initialize the node dictionary.
    node_dict = {"dcid": node, "name": None, "type": None, "parents": []}

    # For each parent of the current node, fetch its details and add it to the node_dict.
    for parent in ancestry.get(node, []):
      parent_dcid = parent.dcid
      name = parent.name
      entity_type = parent.type

      # If the parent node is not already in the cache, add it.
      if parent_dcid not in tree_cache:
        tree_cache[parent_dcid] = {
            "dcid": parent_dcid,
            "name": name,
            "type": entity_type,
            "parents": [],
        }

      parent_node = tree_cache[parent_dcid]

      # Ensure name/type are up to date (in case of duplicates)
      parent_node["name"] = name
      parent_node["type"] = entity_type
      node_dict["parents"].append(parent_node)

    tree_cache[node] = node_dict

  # The root node is the last one in postorder, that's what gets returned
  return tree_cache[postorder[-1]]


def build_ancestry_tree(root: str, ancestry: AncestryMap) -> dict:
  """Builds a nested ancestry tree from an ancestry map.
    Args:
        root (str): The DCID of the root node.
        ancestry (AncestryMap): A flat ancestry map built from `_build_ancestry_map`.
    Returns:
        A nested dictionary tree rooted at the specified DCID.
    """
  postorder = _postorder_nodes(root, ancestry)
  return _assemble_tree(postorder, ancestry)


def flatten_ancestry(ancestry: AncestryMap) -> list[dict[str, str]]:
  """Flattens the ancestry map into a deduplicated list of parent records.
    Args:
        ancestry (AncestryMap): Ancestry mapping of DCIDs to lists of Parent objects.
    Returns:
        A list of dictionaries with keys 'dcid', 'name', and 'type', containing
        each unique parent in the graph.
    """

  flat: list = []
  seen: set[str] = set()
  for parents in ancestry.values():
    for parent in parents:
      if parent.dcid in seen:
        continue
      seen.add(parent.dcid)
      flat.append({
          "dcid": parent.dcid,
          "name": parent.name,
          "type": parent.type
      })
  return flat
