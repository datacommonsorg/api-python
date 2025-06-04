from collections.abc import Mapping, MutableSequence
from pprint import pformat
from typing import Annotated, Any, Iterable, Optional, TypeAlias

from pydantic import BaseModel
from pydantic import BeforeValidator
from pydantic import ConfigDict
from pydantic import RootModel


def listify(v: Any) -> list[str]:
  if isinstance(v, (str, bytes)):
    return [v]
  if not isinstance(v, Iterable):
    return [v]
  return list(v)


variableDCID: TypeAlias = str
entityDCID: TypeAlias = str
facetID: TypeAlias = str
ListOrStr = Annotated[list[str] | str, BeforeValidator(listify)]
NextToken: TypeAlias = Optional[str]
NodeDCID: TypeAlias = str
ArcLabel: TypeAlias = str
Property: TypeAlias = str
PropertyList: TypeAlias = list[Property]
Query: TypeAlias = str
DominantType: TypeAlias = str


class BaseDCModel(BaseModel):
  """Provides serialization methods for the Pydantic models used by the client."""

  model_config = ConfigDict(validate_by_name=True,
                            validate_default=True,
                            validate_by_alias=True,
                            use_enum_values=True,
                            serialize_by_alias=True)

  def __str__(self) -> str:
    """Returns a string representation of the instance."""
    return self.to_json()

  def to_dict(self, exclude_none: bool = True) -> dict[str, Any]:
    """Converts the instance to a dictionary.

        Args:
            exclude_none: If True, only include non-empty values in the response.

        Returns:
            Dict[str, Any]: The dictionary representation of the instance.
        """

    return self.model_dump(mode="python", exclude_none=exclude_none)

  def to_json(self, exclude_none: bool = True) -> str:
    """Converts the instance to a JSON string.

        Args:
            exclude_none: If True, only include non-empty values in the response.

        Returns:
            str: The JSON string representation of the instance.
        """
    return self.model_dump_json(exclude_none=exclude_none, indent=2)


class DictLikeRootModel(RootModel, Mapping):
  """A base class for models that can be treated as dictionaries."""

  def __repr__(self) -> str:
    return f"{self.__class__.__name__}({self.root})"

  def __str__(self) -> str:
    return pformat(self.root, compact=True, width=80)

  def __getitem__(self, key: str) -> Any:
    return self.root[key]

  def __iter__(self) -> Iterable[Any]:
    return iter(self.root)

  def __len__(self) -> int:
    return len(self.root)

  def __eq__(self, other: Any) -> bool:
    if isinstance(other, DictLikeRootModel):
      return self.root == other.root
    else:
      return self.root == other


class ListLikeRootModel(MutableSequence, RootModel):
  """A base class for models that can be treated as lists."""

  def __repr__(self) -> str:
    return f"{self.__class__.__name__}({self.root})"

  def __str__(self) -> str:
    return pformat(self.root, compact=True, width=80)

  def __getitem__(self, index: int) -> Any:
    return self.root[index]

  def __setitem__(self, index: int, value: Any) -> None:
    self.root[index] = value

  def __delitem__(self, index: int) -> None:
    del self.root[index]

  def __len__(self) -> int:
    return len(self.root)

  def __eq__(self, other: Any) -> bool:
    if isinstance(other, ListLikeRootModel):
      return self.root == other.root
    else:
      return self.root == other

  def insert(self, index: int, item: Any) -> None:
    """Inserts an item at a specified index in the root list."""
    self.root.insert(index, item)
