from typing import Any, Dict, List, Optional

from pydantic import Field
from pydantic import field_validator

from datacommons_client.models.base import BaseDCModel
from datacommons_client.models.base import facetID
from datacommons_client.models.node import Arcs
from datacommons_client.models.node import NextToken
from datacommons_client.models.node import Node
from datacommons_client.models.node import NodeDCID
from datacommons_client.models.node import Properties
from datacommons_client.models.node import Property
from datacommons_client.models.observation import ByVariable
from datacommons_client.models.observation import Facet
from datacommons_client.models.observation import ObservationRecords
from datacommons_client.models.observation import VariableByEntity
from datacommons_client.models.resolve import Entity
from datacommons_client.utils.data_processing import flatten_properties
from datacommons_client.utils.data_processing import observations_as_records


class NodeResponse(BaseDCModel):
  """Represents a response from the Node endpoint of the Data Commons API.

    Attributes:
        data: A dictionary mapping node DCIDs to Arcs or Properties objects.
        nextToken: A token for pagination, if present.
    """

  data: Dict[NodeDCID, Arcs | Properties] = Field(default_factory=dict)
  nextToken: NextToken = None

  @field_validator("data", mode="before")
  def _discriminate_data(cls, raw_data):
    """Discriminates between Arcs and Properties based on the presence of 'arcs' key."""

    def _parse_data(
        data: Arcs | Properties | dict[str, Any]) -> Arcs | Properties:
      if isinstance(data, (Arcs, Properties)):
        return data
      if "arcs" in data:
        return Arcs.model_validate(data)
      return Properties.model_validate(data)

    return {dcid: _parse_data(data) for dcid, data in raw_data.items()}

  def get_properties(self) -> Dict:
    return flatten_properties(self.data)

  def extract_connected_nodes(
      self,
      subject_dcid: NodeDCID,
      property_dcid: Property,
      connected_node_types: Optional[str | list[str]] = None) -> List[Node]:
    """Retrieves Node objects in the NodeResponse connected to the subject node
    via the specified property.

    Args:
      subject_dcid: The DCID of the starting node in the arc.
      property_dcid: The property connecting the subject node to the desired
        target nodes.
      connected_node_types: Optional. A type or list of types to filter the
        connected nodes. If provided, only connected nodes that have at least
        one of the specified types will be returned. If omitted, all nodes from
        the arc are returned.

    Returns:
      A list of Node objects that are connected to the subject node via the
      specified property.
    """
    if isinstance(connected_node_types, str):
      connected_node_types = [connected_node_types]

    nodes = self.get_properties().get(subject_dcid, {}).get(property_dcid, [])

    connected_nodes = []
    for node in nodes:
      if connected_node_types:
        # Filter out nodes that are missing a list of types or do not have the
        # desired type
        if not node.types or not any(nt in node.types
                                     for nt in connected_node_types):
          continue

      connected_nodes.append(node)

    return connected_nodes

  def extract_connected_dcids(
      self,
      subject_dcid: NodeDCID,
      property_dcid: Property,
      connected_node_types: Optional[str | list[str]] = None) -> List[NodeDCID]:
    """Retrieves DCIDs of the Nodes in the NodeResponse connected to the subject
    node via the specified property.

    Args:
      subject_dcid: The DCID of the starting node.
      property_dcid: The property connecting the subject node to the desired
        target nodes.
      connected_node_types: Optional. A type or list of types to filter the
        connected nodes. If provided, only DCIDs of connected nodes that have at
        least one of the specified types will be returned. If omitted, DCIDs of
        all nodes from the arc are returned.

    Returns:
      A list of NodeDCIDs for the nodes connected via the specified property
      from the subject node.
    """

    connected_nodes = self.extract_connected_nodes(subject_dcid, property_dcid,
                                                   connected_node_types)

    return [node.dcid for node in connected_nodes if node.dcid]


class ObservationResponse(BaseDCModel):
  """Represents a response from the Observation endpoint of the Data Commons API.

    Attributes:
       byVariable: A dictionary of variable DCIDs and their corresponding data.
         facets: A dictionary of facet IDs and their corresponding data.
    """

  byVariable: ByVariable = Field(default_factory=ByVariable)
  facets: Dict[facetID, Facet] = Field(default_factory=dict)

  def get_data_by_entity(self) -> VariableByEntity:
    """Unpacks the data for each entity, for each variable.

        Returns:
            Dict: The variables object from the response.
        """
    raw_payload = {
        var_dcid: var_model.byEntity
        for var_dcid, var_model in self.byVariable.root.items()
    }
    return VariableByEntity.model_validate(raw_payload)

  def to_observation_records(self) -> ObservationRecords:
    """Returns a flat list of observation records combining date, variable, entity,
         observation, and facet metadata.

    This method transforms the nested `byVariable` and `facets` data in the ObservationResponse
    into a flat list of dictionaries. Each dictionary (or "record") represents a single observation
    for a variable and entity, enriched with its associated facet metadata (e.g., measurement method,
    observation period, unit).

    This format is suitable for exporting to a DataFrame or serializing to JSON for tabular or analytical use.

    Returns:
        ObservationRecord: A list of observation records, where each record contains the variable,
        entity, date, value, facetId, and any additional metadata provided by the facet.
        """
    return observations_as_records(data=self.get_data_by_entity(),
                                   facets=self.facets)

  def get_facets_metadata(self) -> Dict[str, Any]:
    """Extract metadata about StatVars from the response. This data is
        structured as a dictionary of StatVars, each containing a dictionary of
        facets with their corresponding metadata.

        Returns:
            Dict[str, Any]: A dictionary of StatVars with their associated metadata,
             including earliest and latest observation dates, observation counts,
             measurementMethod, observationPeriod, and unit, etc.
        """
    # Dictionary to store metadata
    metadata = {}

    # Extract information from byVariable
    data_by_entity = self.get_data_by_entity()

    # Extract facet information
    facets_info = self.to_dict().get("facets", {})

    for dcid, variables in data_by_entity.items():
      metadata[dcid] = {}

      for entity_id, entity in variables.items():
        for facet in entity.get("orderedFacets", []):
          facet_metadata = metadata[dcid].setdefault(
              facet.facetId,
              {
                  "earliestDate": {},
                  "latestDate": {},
                  "obsCount": {},
              },
          )

          facet_metadata["earliestDate"][entity_id] = facet.earliestDate
          facet_metadata["latestDate"][entity_id] = facet.latestDate
          facet_metadata["obsCount"][entity_id] = facet.obsCount

          # Merge additional facet details
          facet_metadata.update(facets_info.get(facet.facetId, {}))

    return metadata

  def find_matching_facet_id(self, property_name: str,
                             value: str | list[str]) -> list[str]:
    """Finds facet IDs that match a given property and value.

        Args:
            property_name (str): The property to match.
            value (str | list): The value to match. Can be a string, number, or a list of values.
        Returns:
            list[str]: A list of facet IDs that match the property and value.
        """
    # Initialize an empty list to store matching facet IDs
    matching_facet_ids = []

    # Iterate over the facets metadata to find matching facet IDs
    for facet_data in self.get_facets_metadata().values():

      # Iterate over each facet and its associated metadata
      for facet_id, metadata in facet_data.items():

        # Get the value of the specified property from the data
        prop_value = metadata.get(property_name)

        # Check if the property value matches the specified value
        if isinstance(value, list):
          if prop_value in value:
            matching_facet_ids.append(facet_id)
        elif prop_value == value:
          matching_facet_ids.append(facet_id)

    return matching_facet_ids


class ResolveResponse(BaseDCModel):
  """Represents a response from the Resolve endpoint of the Data Commons API.

    Attributes:
        entities (List[Entity]): A list of entities resolved by the API, each
            containing the query node and its associated candidates.
    """

  entities: List[Entity] = Field(default_factory=list)

  def to_flat_dict(self) -> dict[str, list[str] | str]:
    """
      Flattens resolved candidate data into a dictionary where each node maps to its candidates.

      Returns:
          dict[str, Any]: A dictionary mapping nodes to their candidates.
          If a node has only one candidate, it maps directly to the candidate instead of a list.
      """
    items: dict[str, Any] = {}

    for entity in self.entities:
      node = entity.node
      if len(entity.candidates) == 1:
        items[node] = entity.candidates[0].dcid
      else:
        items[node] = [candidate.dcid for candidate in entity.candidates]

    return items
