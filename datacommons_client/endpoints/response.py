from dataclasses import dataclass
from dataclasses import field
from typing import Any, Dict, List

from datacommons_client.models.node import Arcs
from datacommons_client.models.node import NextToken
from datacommons_client.models.node import NodeDCID
from datacommons_client.models.node import Properties
from datacommons_client.models.observation import Facet
from datacommons_client.models.observation import facetID
from datacommons_client.models.observation import Variable
from datacommons_client.models.observation import variableDCID
from datacommons_client.models.resolve import Entity
from datacommons_client.utils.data_processing import flatten_properties
from datacommons_client.utils.data_processing import observations_as_records
from datacommons_client.utils.data_processing import SerializableMixin


@dataclass
class DCResponse(SerializableMixin):
  """Represents a structured response from the Data Commons API."""

  json: Dict[str, Any] = field(default_factory=dict)

  def __repr__(self) -> str:
    """Returns a string with the name of the object,"""
    return f"<Raw Data Commons API response>"

  @property
  def next_token(self):
    return self.json.get("nextToken")


@dataclass
class NodeResponse(SerializableMixin):
  """Represents a response from the Node endpoint of the Data Commons API.

    Attributes:
        data: A dictionary mapping node DCIDs to Arcs or Properties objects.
        nextToken: A token for pagination, if present.
    """

  data: Dict[NodeDCID, Arcs | Properties] = field(default_factory=dict)
  nextToken: NextToken = None

  @classmethod
  def from_json(cls, json_data: Dict[str, Any]) -> "NodeResponse":
    """Parses a dictionary of nodes from JSON.

        Args:
            json_data: The raw JSON data from the API response.

        Returns:
            A NodeResponse instance.
        """

    def parse_data(data: Dict[str, Any]) -> Arcs | Properties:
      if "arcs" in data:
        return Arcs.from_json(data["arcs"])
      return Properties.from_json(data)

    parsed_data = {
        dcid: parse_data(data)
        for dcid, data in json_data.get("data", {}).items()
    }
    return cls(data=parsed_data, nextToken=json_data.get("nextToken"))

  def get_properties(self) -> Dict:
    return flatten_properties(self.data)


@dataclass
class ObservationResponse(SerializableMixin):
  """Represents a response from the Observation endpoint of the Data Commons API.

    Attributes:
       byVariable: A dictionary of variable DCIDs and their corresponding data.
         facets: A dictionary of facet IDs and their corresponding data.
    """

  byVariable: Dict[variableDCID, Any] = field(default_factory=dict)
  facets: Dict[facetID, Any] = field(default_factory=dict)

  @classmethod
  def from_json(cls, json_data: Dict[str, Any]) -> "ObservationResponse":
    """Parses the data from the API response."""
    return cls(
        byVariable={
            variable: Variable.from_json(data)
            for variable, data in json_data.get("byVariable", {}).items()
        },
        facets={
            facet: Facet.from_json(data)
            for facet, data in json_data.get("facets", {}).items()
        },
    )

  def get_data_by_entity(self) -> Dict:
    """Unpacks the data for each entity, for each variable.

        Returns:
            Dict: The variables object from the response.
        """
    return {
        variable: data.byEntity for variable, data in self.byVariable.items()
    }

  def get_observations_as_records(self) -> List[Dict[str, Any]]:
    """Converts the observation data into a list of records.

        Returns:
            List[Dict[str, Any]]: A flattened list of observation records.
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


@dataclass
class ResolveResponse(SerializableMixin):
  """Represents a response from the Resolve endpoint of the Data Commons API.

    Attributes:
        entities (List[Entity]): A list of entities resolved by the API, each
            containing the query node and its associated candidates.
    """

  entities: List[Entity] = field(default_factory=list)

  @classmethod
  def from_json(cls, json_data: Dict[str, Any]) -> "ResolveResponse":
    """Parses a ResolveResponse instance from JSON data.

        Args:
            json_data (Dict[str, Any]): A dictionary containing the API response
                data, with keys like "entities".

        Returns:
            ResolveResponse: A populated instance of the ResolveResponse class.
        """
    return cls(entities=[
        Entity.from_json(entity) for entity in json_data.get("entities", [])
    ])

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
