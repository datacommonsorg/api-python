from collections.abc import Mapping, MutableSequence
from typing import Annotated, Any, Iterable, TypeAlias

from pydantic import BaseModel
from pydantic import BeforeValidator
from pydantic import ConfigDict
from pydantic import RootModel

variableDCID: TypeAlias = str
entityDCID: TypeAlias = str
facetID: TypeAlias = str


def listify(v: Any) -> list[str]:
  if isinstance(v, (str, bytes)):
    return [v]
  if not isinstance(v, Iterable):
    return [v]
  return list(v)


ListOrStr = Annotated[list[str] | str, BeforeValidator(listify)]


class BaseDCModel(BaseModel):
  """Provides serialization methods for the Pydantic models used by the client."""

  model_config = ConfigDict(validate_by_name=True,
                            validate_default=True,
                            validate_by_alias=True,
                            use_enum_values=True,
                            serialize_by_alias=True)

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


class DictLikeRootModel(Mapping, RootModel):
  """A base class for models that can be treated as dictionaries."""

  def __getitem__(self, key: str) -> Any:
    return self.root[key]

  def __iter__(self) -> Iterable[Any]:
    return iter(self.root)

  def __len__(self) -> int:
    return len(self.root)


class ListLikeRootModel(MutableSequence, RootModel):
  """A base class for models that can be treated as lists."""

  def __getitem__(self, index: int) -> Any:
    return self.root[index]

  def __setitem__(self, index: int, value: Any) -> None:
    self.root[index] = value

  def __delitem__(self, index: int) -> None:
    del self.root[index]

  def __len__(self) -> int:
    return len(self.root)

  def insert(self, index: int, item: Any) -> None:
    """Inserts an item at a specified index in the root list."""
    self.root.insert(index, item)