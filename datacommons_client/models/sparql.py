from dataclasses import dataclass
from dataclasses import field
from typing import Any, Dict, List


@dataclass
class Cell:
  """Represents a single cell in a row.

    Attributes:
        value: The value contained in the cell.
    """

  value: str = field(default_factory=str)

  @classmethod
  def from_json(cls, json_data: Dict[str, Any]) -> "Cell":
    """Parses a Cell instance from response data.

        Args:
            json_data (Dict[str, Any]): A dictionary containing cell data.

        Returns:
            Cell: A populated instance of the Cell class.
        """

    return cls(value=json_data.get("value"))


@dataclass
class Row:
  """Parses a Row instance from response data.

    Args:
        json_data (Dict[str, Any]): A dictionary containing row data,
            typically with a "cells" key containing a list of cells.

    Returns:
        Row: A populated instance of the Row class.
    """

  cells: List[Cell] = field(default_factory=list)

  @classmethod
  def from_json(cls, json_data: Dict[str, Any]) -> "Row":
    return cls(
        cells=[Cell.from_json(cell) for cell in json_data.get("cells", [])])
