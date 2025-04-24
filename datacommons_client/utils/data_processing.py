from dataclasses import asdict
import json
from typing import Any, Dict, List

from datacommons_client.models.node import ArcLabel
from datacommons_client.models.node import Arcs
from datacommons_client.models.node import Name
from datacommons_client.models.node import Node
from datacommons_client.models.node import NodeDCID
from datacommons_client.models.node import NodeGroup
from datacommons_client.models.node import Properties
from datacommons_client.models.node import Property


def unpack_arcs(arcs: Dict[ArcLabel, NodeGroup]) -> Dict[Property, List[Node]]:
  """Simplify the 'arcs' structure."""
  # Return dictionary of property nodes
  return {
      prop: getattr(arc_data, "nodes", []) for prop, arc_data in arcs.items()
  }


def flatten_properties(
    data: Dict[NodeDCID, Arcs | Properties]
) -> Dict[NodeDCID, List[Property] | Dict[Property, List[Node]]]:
  """
    Flatten the properties of a node response.

    Processes a dictionary of node responses, extracting and
    simplifying their properties and arcs into a flattened dictionary.

    Args:
        data (Dict[NodeDCID, Arcs | Properties]):
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


def group_variables_by_entity(
    data: dict[str, list[str]]) -> dict[str, list[str]]:
  """Groups variables by the entities they are associated with.
    Takes a dictionary mapping statistical variable DCIDs to a list of entity DCIDs,
    and returns a new dictionary mapping each entity DCID to a list of statistical
    variables available for that entity.
    Args:
        data: A dictionary where each key is a variable DCID and the value is a list
            of entity DCIDs that have observations for that variable.
    Returns:
        A dictionary where each key is an entity DCID and the value is a list of
        variable DCIDs available for that entity.
    """
  result: dict[str, list[str]] = {}
  for variable, entities in data.items():
    for entity in entities:
      result.setdefault(entity, []).append(variable)
  return result


class SerializableMixin:
  """Provides serialization methods for the Response dataclasses."""

  def to_dict(self, exclude_none: bool = True) -> Dict[str, Any]:
    """Converts the instance to a dictionary.

        Args:
            exclude_none: If True, only include non-empty values in the response.

        Returns:
            Dict[str, Any]: The dictionary representation of the instance.
        """

    def _remove_none(data: Any) -> Any:
      """Recursively removes None or empty values from a dictionary or list."""
      if isinstance(data, dict):
        return {k: _remove_none(v) for k, v in data.items() if v is not None}
      elif isinstance(data, list):
        return [_remove_none(item) for item in data]
      return data

    result = asdict(self)
    return _remove_none(result) if exclude_none else result

  def to_json(self, exclude_none: bool = True) -> str:
    """Converts the instance to a JSON string.

        Args:
            exclude_none: If True, only include non-empty values in the response.

        Returns:
            str: The JSON string representation of the instance.
        """
    return json.dumps(self.to_dict(exclude_none=exclude_none), indent=2)


def flatten_names_dictionary(names_dict: dict[str, Name]) -> dict[str, str]:
  """
    Flattens a dictionary which contains Name objects into a flattened dictionary
    with DCIDs as keys and names as values.

    Args:
        names_dict (dict[str, Name]): The input dictionary to flatten.

    Returns:
        dict[str, str]: A flattened dictionary with DCIDs as keys and names as values.
    """

  return {dcid: name.to_dict()['value'] for dcid, name in names_dict.items()}
