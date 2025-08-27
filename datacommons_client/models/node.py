from typing import Optional

from pydantic import Field

from datacommons_client.models.base import ArcLabel
from datacommons_client.models.base import BaseDCModel
from datacommons_client.models.base import DictLikeRootModel
from datacommons_client.models.base import ListLikeRootModel
from datacommons_client.models.base import NodeDCID
from datacommons_client.models.base import Property
from datacommons_client.models.base import PropertyList


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

  nodes: list[Node] = Field(default_factory=list)


class Arcs(BaseDCModel):
  """Represents arcs in the Data Commons knowledge graph.

    Attributes:
        arcs: A dictionary mapping arc labels to NodeGroup objects.
    """

  arcs: dict[ArcLabel, NodeGroup] = Field(default_factory=dict)


class Properties(BaseDCModel):
  """Represents a group of properties in the Data Commons knowledge graph.

    Attributes:
        properties: A list of property strings.
    """

  properties: Optional[PropertyList] = None


class FlattenedPropertiesMapping(BaseDCModel,
                                 DictLikeRootModel[dict[NodeDCID,
                                                        PropertyList]]):
  """A model to represent a mapping of node DCIDs to their properties."""


class FlattenedArcsMapping(BaseDCModel,
                           DictLikeRootModel[dict[NodeDCID, dict[Property,
                                                                 list[Node]]]]):
  """A model to represent a mapping of node DCIDs to their arcs."""


class NodeList(BaseDCModel, ListLikeRootModel[list[Node]]):
  """A root model whose value is a list of Node objects."""


class NodeDCIDList(BaseDCModel, ListLikeRootModel[list[NodeDCID]]):
  """A root model whose value is a list of NodeDCID strings."""


class StatVarConstraint(BaseDCModel):
  """Represents a constraint for a statistical variable."""

  constraintId: NodeDCID
  constraintName: Optional[str] = None
  valueId: NodeDCID
  valueName: Optional[str] = None


class StatVarConstraints(BaseDCModel,
                         DictLikeRootModel[dict[NodeDCID,
                                                list[StatVarConstraint]]]):
  """A root model whose value is a dictionary of statvar ids - a list of StatVarConstraint objects.
    This model is used to represent constraints associated with statistical variables.
    """
