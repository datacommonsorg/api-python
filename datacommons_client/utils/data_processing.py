from dataclasses import asdict
from typing import Any, Dict


def unpack_arcs(arcs: Dict[str, Any]) -> Any:
  """Simplify the 'arcs' structure."""
  if len(arcs) > 1:
    # Multiple arcs: return dictionary of property nodes
    return {prop: arc_data["nodes"] for prop, arc_data in arcs.items()}

  # Single arc: extract first node's data
  for property_data in arcs.values():
    nodes = property_data.nodes
    if nodes is not None:
      return nodes if len(nodes) > 1 else nodes[0]


def flatten_properties(data: Dict[str, Any]) -> Dict[str, Any]:
  """
    Flatten the properties of a node response.

    Processes a dictionary of node responses, extracting and
    simplifying their properties and arcs into a flattened dictionary.

    Args:
        data (Dict[str, Dict[str, Any]]):
            The input dictionary containing node responses. Each node maps to
            a dictionary with potential "arcs" and "properties" keys.

    Returns:
        Dict[str, Any]:
            A flattened dictionary where keys are node identifiers, and values
            are the simplified properties or nodes.
    """

  # Store simplified properties
  items = {}

  for node, node_data in data.items():
    arcs = getattr(node_data, "arcs", {})
    properties = getattr(node_data, "properties", None)

    processed_arcs = unpack_arcs(arcs) if arcs else None
    items[node] = processed_arcs if processed_arcs is not None else properties

  return items


def extract_observations(variable: str, entity: str, entity_data: dict,
                         facet_metadata: dict) -> list[dict]:
  """
    Extracts observations for a given variable, entity, and its data.

    Args:
        variable (str): The variable name.
        entity (str): The entity name.
        entity_data (dict): Data for the entity, including ordered facets.
        facet_metadata (dict): Metadata for facets.

    Returns:
        list[dict]: A list of observation records.
    """
  return [{
      "date": observation.date,
      "entity": entity,
      "variable": variable,
      "value": observation.value,
      "facetId": facet.facetId,
      **asdict(facet_metadata.get(facet.facetId, {})),
  }
          for facet in entity_data.get("orderedFacets", [])
          for observation in facet.observations]


def observations_as_records(data: dict, facets: dict) -> list[dict]:
  """
    Converts observation data into a list of records.

    Args:
        data (dict): A mapping of variables to entities and their data.
        facets (dict): Facet metadata for the observations.

    Returns:
        list[dict]: A flattened list of observation records.
    """
  return [
      record for variable, entities in data.items()
      for entity, entity_data in entities.items()
      for record in extract_observations(
          variable=variable,
          entity=entity,
          entity_data=entity_data,
          facet_metadata=facets,
      )
  ]
