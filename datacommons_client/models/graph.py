from dataclasses import dataclass
from typing import TypeAlias

from datacommons_client.utils.data_processing import SerializableMixin


@dataclass(frozen=True)
class Parent(SerializableMixin):
  """A class representing a parent node in a graph.
    Attributes:
        dcid (str): The ID of the parent node.
        name (str): The name of the parent node.
        type (str | list[str]): The type(s) of the parent node.
    """

  dcid: str
  name: str
  type: str | list[str]


# A dictionary mapping DCIDs to lists of Parent objects.
AncestryMap: TypeAlias = dict[str, list[Parent]]
