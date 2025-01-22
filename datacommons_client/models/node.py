from dataclasses import dataclass
from dataclasses import field
from typing import Any, Dict, List, Optional, TypeAlias

NextToken: TypeAlias = Optional[str]
NodeDCID: TypeAlias = str
ArcLabel: TypeAlias = str
Property: TypeAlias = str
PropertyList: TypeAlias = list[Property]


@dataclass
class Node:
  """Represents an individual node in the Data Commons knowledge graph.

    Attributes:
        dcid: The unique identifier for the node.
        name: The name of the node.
        provenanceId: The provenance ID for the node.
        types: The types associated with the node.
        value: The value of the node.
    """

  dcid: str = None
  name: Optional[str] = None
  provenanceId: Optional[str] = None
  types: Optional[list[str]] = None
  value: Optional[str] = None

  @classmethod
  def from_json(cls, json_data: Dict[str, Any]) -> "Node":
    return cls(
        dcid=json_data.get("dcid"),
        name=json_data.get("name"),
        provenanceId=json_data.get("provenanceId"),
        types=json_data.get("types"),
        value=json_data.get("value"),
    )


@dataclass
class NodeGroup:
  """Represents a group of nodes in the Data Commons knowledge graph.

    Attributes:
        nodes: A list of Node objects in the group.
    """

  nodes: List[Node] = field(default_factory=list)

  @classmethod
  def from_json(cls, json_data: Dict[str, Any]) -> "NodeGroup":
    """Parses a dictionary of lists of nodes from the response data.

        Args:
            json_data: The raw JSON data containing node information.

        Returns:
            A NodeGroup instance.
        """
    return cls(
        nodes=[Node.from_json(node) for node in json_data.get("nodes", [])])


@dataclass
class Arcs:
  """Represents arcs in the Data Commons knowledge graph.

    Attributes:
        arcs: A dictionary mapping arc labels to NodeGroup objects.
    """

  arcs: Dict[ArcLabel, NodeGroup] = field(default_factory=dict)

  @classmethod
  def from_json(cls, json_data: Dict[str, Any]) -> "Arcs":
    """Parses a dictionary of arcs from JSON.

        Args:
            json_data: The raw JSON data containing arc information.

        Returns:
            An Arcs instance.
        """
    return cls(arcs={
        label: NodeGroup.from_json(nodes) for label, nodes in json_data.items()
    })


@dataclass
class Properties:
  """Represents a group of properties in the Data Commons knowledge graph.

    Attributes:
        properties: A list of property strings.
    """

  properties: List[Property] = field(default_factory=PropertyList)

  @classmethod
  def from_json(cls, json_data: Dict[str, Any]) -> "Properties":
    """Parses a list of properties from JSON.

        Args:
            json_data: The raw JSON data containing property information.

        Returns:
            A Properties instance.
        """
    return cls(properties=json_data.get("properties", []))
