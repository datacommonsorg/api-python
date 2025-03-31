from typing import Optional

from datacommons_client.models.node import Node


def extract_name_from_english_name_property(properties: list | Node) -> str:
  """
    Extracts the name from a list of properties with English names.
    Args:
        properties (list): A list of properties with English names.
    Returns:
        str: The extracted name.
    """
  if isinstance(properties, Node):
    properties = [properties]

  return properties[0].value


def extract_name_from_property_with_language(
    properties: list, language: str, fallback_to_en: bool) -> Optional[str]:
  """
    Extracts the name from a list of properties with language tags.
    Args:
        properties (list): A list of properties with language tags.
        language (str): The desired language code.
        fallback_to_en (bool): Whether to fall back to English if the desired language is not found.
    """
  # If a non-English language is requested, unpack the response to get it.
  fallback_name = None

  # Iterate through the properties to find the name in the specified language
  for candidate in properties:
    # If no language is specified, skip the candidate
    if "@" not in candidate.value:
      continue

    # Split the candidate value into name and language
    name, lang = candidate.value.rsplit("@", 1)

    # If the language matches, add the name to the dictionary.
    if lang == language:
      return name
    # If language is 'en', store the name as a fallback
    if lang == "en":
      fallback_name = name

  # If no name was found in the specified language, use the fallback name (if available and
  # fallback_to_en is True)
  return fallback_name if fallback_to_en else None
