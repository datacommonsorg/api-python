from datacommons_client.utils.data_processing import group_variables_by_entity


def test_group_variables_by_entity_basic():
  """Test grouping with simple variable-entity mapping."""
  input_data = {
      "var1": ["ent1", "ent2"],
      "var2": ["ent2", "ent3"],
      "var3": ["ent1"],
  }
  expected_output = {
      "ent1": ["var1", "var3"],
      "ent2": ["var1", "var2"],
      "ent3": ["var2"],
  }

  result = group_variables_by_entity(input_data)
  assert result == expected_output


def test_group_variables_by_entity_duplicate_entities():
  """Test grouping when a variable has duplicate entities."""
  input_data = {
      "var1": ["ent1", "ent1", "ent2"],
  }
  result = group_variables_by_entity(input_data)
  assert result["ent1"].count("var1") == 2  # duplicates are preserved
  assert "ent2" in result
  assert result["ent2"] == ["var1"]


def test_group_variables_by_entity_preserves_order():
  """Test if the order of variables is preserved in the resulting entity lists."""
  input_data = {
      "var1": ["ent1"],
      "var2": ["ent1"],
      "var3": ["ent1"],
  }
  result = group_variables_by_entity(input_data)
  assert result["ent1"] == ["var1", "var2", "var3"]
