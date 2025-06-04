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


class DictLikeRootModel(RootModel):
  """A base class for models that can be treated as dictionaries."""

  def __getitem__(self, key: str) -> Any:
    return self.root[key]

  def __iter__(self) -> Iterable[Any]:
    return iter(self.root)

  def __len__(self) -> int:
    return len(self.root)

  def __contains__(self, key: str) -> bool:
    return key in self.root

  def keys(self) -> Iterable[str]:
    """Returns the keys of the root dictionary."""
    return self.root.keys()

  def items(self) -> Iterable[tuple[str, Any]]:
    """Returns the items of the root dictionary."""
    return self.root.items()

  def values(self) -> Iterable[Any]:
    """Returns the values of the root dictionary."""
    return self.root.values()

  def get(self, key: variableDCID, default: Any = None) -> Any:
    return self.root.get(key, default)


class ListLikeRootModel(RootModel):
  """A base class for models that can be treated as lists."""

  def __getitem__(self, index: int) -> Any:
    return self.root[index]

  def __iter__(self) -> Iterable[Any]:
    return iter(self.root)

  def __len__(self) -> int:
    return len(self.root)

  def __contains__(self, item: Any) -> bool:
    return item in self.root

  def append(self, item: Any) -> None:
    """Appends an item to the root list."""
    self.root.append(item)

  def extend(self, items: Iterable[Any]) -> None:
    """Extends the root list with items from an iterable."""
    self.root.extend(items)

  def sort(self, *args, **kwargs) -> None:
    """Sorts the root list in place."""
    self.root.sort(*args, **kwargs)

  def reverse(self) -> None:
    """"Reverses the root list in place."""
    self.root.reverse()

  def to_list(self) -> list:
    """Converts the root model to a list."""
    return list(self.root)
