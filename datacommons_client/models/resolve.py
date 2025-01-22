from dataclasses import dataclass
from dataclasses import field
from typing import Any, Dict, List, Optional, TypeAlias

Query: TypeAlias = str
DCID: TypeAlias = str
DominantType: TypeAlias = str


@dataclass
class Candidate:
  """Represents a candidate in the resolution response.

    Attributes:
        dcid (DCID): The Data Commons ID for the candidate.
        dominantType (Optional[DominantType]): The dominant type of the candidate,
            if available. This represents the primary type associated with the DCID.
    """

  dcid: DCID = field(default_factory=str)
  dominantType: Optional[DominantType] = None

  @classmethod
  def from_json(cls, json_data: Dict[str, Any]) -> "Candidate":
    """Parses a Candidate instance from the response data.

        Args:
            json_data (Dict[str, Any]): A dictionary containing candidate data,
                typically from the Data Commons API.

        Returns:
            Candidate: An instance of the Candidate class populated with the
            provided data.
        """
    return cls(
        dcid=json_data["dcid"],
        dominantType=json_data.get("dominantType"),
    )


@dataclass
class Entity:
  """Represents an entity with its resolution candidates.

    Attributes:
        node (Query): The query string or node being resolved.
        candidates (List[Candidate]): A list of candidates that match the query.
    """

  node: Query
  candidates: List[Candidate] = field(default_factory=list)

  @classmethod
  def from_json(cls, json_data: Dict[str, Any]) -> "Entity":
    """Parses an Entity instance from response data.

        Args:
            json_data (Dict[str, Any]): A dictionary containing entity data,
                including the node and associated candidates.

        Returns:
            Entity: A populated instance of the Entity class.
        """
    return cls(
        node=json_data.get("node"),
        candidates=[
            Candidate.from_json(candidate)
            for candidate in json_data.get("candidates", [])
        ],
    )
