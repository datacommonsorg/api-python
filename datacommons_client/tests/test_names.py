from datacommons_client.models.node import Node
from datacommons_client.utils.names import extract_name_from_english_name_property
from datacommons_client.utils.names import extract_name_from_property_with_language


def test_extract_name_from_english_name_property_with_list():
  """Test extracting name from a list of Nodes."""
  properties = [Node(value="Test Name")]
  result = extract_name_from_english_name_property(properties)
  assert result == "Test Name"


def test_extract_name_from_english_empty_list():
  """Test extracting name from an empty list."""
  result = extract_name_from_english_name_property([])
  assert result == ""


def test_extract_name_from_english_not_list():
  """Test extracting name from a single Node (not in a list)."""
  property_node = Node(value="Single Node Name")
  result = extract_name_from_english_name_property(property_node)
  assert result == "Single Node Name"


def test_extract_name_from_property_with_language_match():
  """Test extracting name when desired language is present."""
  properties = [
      Node(value="Nombre@es"),
      Node(value="Name@en"),
  ]
  result = extract_name_from_property_with_language(properties,
                                                    language="es",
                                                    fallback_language="en")
  assert result[0] == "Nombre"
  assert result[1] == "es"


def test_extract_name_from_property_with_language_fallback():
  """Test fallback to English when desired language is not found."""
  properties = [
      Node(value="Name@en"),
      Node(value="Nom@fr"),
      Node(value="Nome@it"),
  ]
  result = extract_name_from_property_with_language(properties,
                                                    language="de",
                                                    fallback_language="it")
  assert result[0] == "Nome"
  assert result[1] == "it"


def test_extract_name_from_property_with_language_no_fallback():
  """Test no result when language is not found and fallback is disabled."""
  properties = [
      Node(value="Name@en"),
      Node(value="Nom@fr"),
  ]
  result = extract_name_from_property_with_language(properties, language="de")
  assert result[0] is None
  assert result[1] is None


def test_extract_name_from_property_without_language_tags():
  """Test that properties without language tags are skipped."""
  properties = [
      Node(value="Plain str"),
      Node(value="Name@en"),
  ]
  result = extract_name_from_property_with_language(properties, language="en")
  assert result[0] == "Name"
  assert result[1] == "en"
