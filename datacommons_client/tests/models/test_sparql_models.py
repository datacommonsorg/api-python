from datacommons_client.models.sparql import Cell
from datacommons_client.models.sparql import Row


def test_cell_from_json():
  """Test the Cell.from_json method with valid data."""
  json_data = {"value": "Test Value"}
  cell = Cell.from_json(json_data)
  assert cell.value == "Test Value"


def test_cell_int_from_json():
  """ "Test the Cell.from_json method with valid data (int)."""
  json_data = {"value": 5}
  cell = Cell.from_json(json_data)
  assert cell.value == 5


def test_cell_from_json_missing_value():
  """Test Cell.from_json with missing value field."""
  json_data = {}
  cell = Cell.from_json(json_data)
  assert cell.value is None


def test_row_from_json():
  """Test the Row.from_json method with multiple cells."""
  json_data = {
      "cells": [
          {
              "value": "Cell 1"
          },
          {
              "value": "Cell 2"
          },
          {
              "value": "Cell 3"
          },
      ]
  }
  row = Row.from_json(json_data)
  assert len(row.cells) == 3
  assert row.cells[0].value == "Cell 1"
  assert row.cells[1].value == "Cell 2"
  assert row.cells[2].value == "Cell 3"


def test_row_from_json_empty_cells():
  """Test Row.from_json with an empty list of cells."""
  json_data = {"cells": []}
  row = Row.from_json(json_data)
  assert len(row.cells) == 0


def test_row_from_json_missing_cells():
  """Test Row.from_json with missing cells field."""
  json_data = {}
  row = Row.from_json(json_data)
  assert len(row.cells) == 0


def test_row_with_nested_empty_cells():
  """Test Row.from_json with cells containing empty cell data."""
  json_data = {
      "cells": [
          {},
          {
              "value": "Cell 2"
          },
      ]
  }
  row = Row.from_json(json_data)
  assert len(row.cells) == 2
  assert row.cells[0].value is None
  assert row.cells[1].value == "Cell 2"
