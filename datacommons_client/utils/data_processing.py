from dataclasses import asdict
import json
from typing import Any, Dict, List

from datacommons_client.models.base import ArcLabel
from datacommons_client.models.base import facetID
from datacommons_client.models.base import NodeDCID
from datacommons_client.models.base import Property
from datacommons_client.models.node import Arcs
from datacommons_client.models.node import FlattenedArcsMapping
from datacommons_client.models.node import FlattenedPropertiesMapping
from datacommons_client.models.node import Name
from datacommons_client.models.node import Node
from datacommons_client.models.node import NodeGroup
from datacommons_client.models.node import Properties
from datacommons_client.models.observation import Facet
from datacommons_client.models.observation import ObservationRecord
from datacommons_client.models.observation import ObservationRecords
from datacommons_client.models.observation import OrderedFacets
from datacommons_client.models.observation import VariableByEntity


def unpack_arcs(arcs: Dict[ArcLabel, NodeGroup]) -> dict[Property, list[Node]]:
  """Simplify the 'arcs' structure."""
  # Return dictionary of property nodes
  return {
      prop: getattr(arc_data, "nodes", []) for prop, arc_data in arcs.items()
  }


def flatten_properties(
    data: Dict[NodeDCID, Arcs | Properties]
) -> FlattenedPropertiesMapping | FlattenedArcsMapping:
  """
    Flatten the properties of a node response.

    Processes a dictionary of node responses, extracting and
    simplifying their properties and arcs into a flattened dictionary.

    Args:
        data (Dict[NodeDCID, Arcs | Properties]):
            The input dictionary containing node responses. Each node maps to
            a dictionary with potential "arcs" and "properties" keys.

    Returns:
        FlattenedPropertiesMapping | FlattenedArcsMapping:
            A flattened dictionary where keys are node identifiers, and values
            are the simplified properties or nodes.
    """
  if not data:
    return FlattenedPropertiesMapping.model_validate({})

  first_node = next(iter(data.values()))
  is_properties = isinstance(first_node, Properties)
  mapping_cls = FlattenedPropertiesMapping if is_properties else FlattenedArcsMapping

  # Store simplified properties
  items = {}
  for node_id, node_data in data.items():
    if is_properties:
      props = getattr(node_data, "properties", None)
      if props:
        items[node_id] = props
    else:
      arcs = getattr(node_data, "arcs", None)
      if arcs:
        items[node_id] = unpack_arcs(arcs)

  return mapping_cls.model_validate(items)


def extract_observations(
    variable: str, entity: str, entity_data: OrderedFacets,
    facet_metadata: dict[facetID, Facet]) -> list[ObservationRecord]:
  """
    Extracts observations for a given variable, entity, and its data.

    Args:
        variable (str): The variable name.
        entity (str): The entity name.
        entity_data (OrderedFacets): Data for the entity, including ordered facets.
        facet_metadata (dict[facetID, Facet]): Metadata for facets.

    Returns:
        list[dict]: A list of observation records.
    """
  observations = []
  for facet in entity_data.orderedFacets:
    for observation in facet.observations:
      observations.append(
          ObservationRecord.model_validate({
              "date": observation.date,
              "entity": entity,
              "variable": variable,
              "value": observation.value,
              "facetId": facet.facetId,
              **facet_metadata.get(facet.facetId, Facet()).to_dict(),
          }))

  return observations


def observations_as_records(data: VariableByEntity,
                            facets: dict[facetID, Facet]) -> ObservationRecords:
  """
    Converts observation data into a list of records.

    Args:
        data (VariableByEntity): A mapping of variables to entities and their data.
        facets (dict): Facet metadata for the observations.

    Returns:
        ObservationRecords: A flattened list of observation records.
    """

  records = []
  for variable, entities in data.items():
    for entity, entity_data in entities.items():
      for record in extract_observations(
          variable=variable,
          entity=entity,
          entity_data=entity_data,
          facet_metadata=facets,
      ):
        records.append(record)

  return ObservationRecords.model_validate(records)


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
