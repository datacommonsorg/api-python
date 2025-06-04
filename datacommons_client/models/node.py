from typing import Dict, List, Optional, TypeAlias

from pydantic import Field

from datacommons_client.models.base import BaseDCModel

NextToken: TypeAlias = Optional[str]
NodeDCID: TypeAlias = str
ArcLabel: TypeAlias = str
Property: TypeAlias = str
PropertyList: TypeAlias = list[Property]


class Node(BaseDCModel):
  """Represents an individual node in the Data Commons knowledge graph.

    Attributes:
        dcid: The unique identifier for the node.
        name: The name of the node.
        provenanceId: The provenance ID for the node.
        types: The types associated with the node.
        value: The value of the node.
    """
  dcid: Optional[str] = None
  name: Optional[str] = None
  provenanceId: Optional[str | list[str]] = None
  types: Optional[list[str]] = None
  value: Optional[str] = None


class Name(BaseDCModel):
  """Represents a name associated with an Entity (node).

    Attributes:
        value: The name of the Entity
        language: The language of the name
        property: The property used to get the name
    """

  value: str
  language: str
  property: str


class NodeGroup(BaseDCModel):
  """Represents a group of nodes in the Data Commons knowledge graph.

    Attributes:
        nodes: A list of Node objects in the group.
    """

  nodes: List[Node] = Field(default_factory=list)


class Arcs(BaseDCModel):
  """Represents arcs in the Data Commons knowledge graph.

    Attributes:
        arcs: A dictionary mapping arc labels to NodeGroup objects.
    """

  arcs: Dict[ArcLabel, NodeGroup] = Field(default_factory=dict)


class Properties(BaseDCModel):
  """Represents a group of properties in the Data Commons knowledge graph.

    Attributes:
        properties: A list of property strings.
    """

  properties: Optional[PropertyList] = None
