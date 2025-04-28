from typing import Optional

from datacommons_client.models.node import Node

DEFAULT_NAME_PROPERTY: str = "name"
NAME_WITH_LANGUAGE_PROPERTY: str = "nameWithLanguage"
DEFAULT_NAME_LANGUAGE: str = "en"


def extract_name_from_english_name_property(properties: list | Node) -> str:
  """
    Extracts the name from a list of properties with English names.
    Args:
        properties (list): A list of properties with English names.
    Returns:
        str: The extracted name.
    """
  if not properties:
    return ''

  if isinstance(properties, Node):
    properties = [properties]

  return properties[0].value


def extract_name_from_property_with_language(
    properties: list,
    language: str,
    fallback_language: Optional[str] = None) -> tuple[str | None, str | None]:
  """
    Extracts the name from a list of properties with language tags.
    Args:
        properties (list): A list of properties with language tags.
        language (str): The desired language code.
        fallback_language: If provided, this language will be used as a fallback if the requested
            language is not available. If not provided, no fallback will be used.

    Returns:
        tuple[str,str]: A tuple containing the extracted name and its language.
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
      return name, lang
    # If language is 'en', store the name as a fallback
    if fallback_language and (lang == fallback_language):
      fallback_name = name

  # If no name was found in the specified language, use the fallback name (if available)
  return fallback_name, fallback_language if fallback_language else None
