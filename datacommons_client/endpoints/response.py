from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import field
import json
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
