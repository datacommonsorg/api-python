from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from dataclasses import field
from enum import Enum
from typing import Optional

from datacommons_client.utils.error_handling import InvalidObservationSelectError


def normalize_properties_to_string(properties: str | list[str]) -> str:
  """Converts a list of properties to a string."""

  if isinstance(properties, list):
    return f"[{', '.join(properties)}]"

  return properties


@dataclass
class EndpointRequestPayload(ABC):
  """
    Abstract base class for payload dataclasses.
    Defines the required interface for all payload dataclasses.
    """

  @abstractmethod
  def normalize(self) -> None:
    """Normalize the payload for consistent internal representation."""
    pass

  @abstractmethod
  def validate(self) -> None:
    """Validate the payload to ensure its structure and contents are correct."""
    pass

  @property
  @abstractmethod
  def to_dict(self) -> dict:
    """Convert the payload into a dictionary format for API requests."""
    pass


@dataclass
class NodeRequestPayload(EndpointRequestPayload):
  """
    A dataclass to structure, normalize, and validate the payload for a Node V2 API request.

    Attributes:
        node_dcids (str | list[str]): The DCID(s) of the nodes to query.
        expression (str): The property or relation expression(s) to query.
    """

  node_dcids: str | list[str]
  expression: str

  def __post_init__(self):
    self.normalize()
    self.validate()

  def normalize(self):
    if isinstance(self.node_dcids, str):
      self.node_dcids = [self.node_dcids]

    self.expression = normalize_properties_to_string(self.expression)

  def validate(self):
    if not isinstance(self.expression, str):
      raise ValueError("Expression must be a string.")

  @property
  def to_dict(self) -> dict:
    return {"nodes": self.node_dcids, "property": self.expression}


class ObservationSelect(str, Enum):
  DATE = "date"
  VARIABLE = "variable"
  ENTITY = "entity"
  VALUE = "value"

  @classmethod
  def _missing_(cls, value):
    """Handle missing enum values by raising a custom error."""
    valid_values = [member.value for member in cls]
    message = f"Invalid `select` field: '{value}'. Only {', '.join(valid_values)} are allowed."
    raise InvalidObservationSelectError(message=message)


class ObservationDate(str, Enum):
  LATEST = "LATEST"
  ALL = ""


@dataclass
class ObservationRequestPayload(EndpointRequestPayload):
  """
    A dataclass to structure, normalize, and validate the payload for an Observation V2 API request.

    Attributes:
        date (str): The date for which data is being requested.
        variable_dcids (str | list[str]): One or more variable IDs for the data.
        select (list[ObservationSelect]): Fields to include in the response.
            Defaults to ["date", "variable", "entity", "value"].
        entity_dcids (Optional[str | list[str]]): One or more entity IDs to filter the data.
        entity_expression (Optional[str]): A string expression to filter entities.
    """

  date: ObservationDate | str = ""
  variable_dcids: str | list[str] = field(default_factory=list)
  select: Optional[list[ObservationSelect | str]] = None
  entity_dcids: Optional[str | list[str]] = None
  entity_expression: Optional[str] = None

  def __post_init__(self):
    """
        Initializes the payload, performing validation and normalization.

        Raises:
            ValueError: If validation rules are violated.
        """
    if self.select is None:
      self.select = [
          ObservationSelect.DATE,
          ObservationSelect.VARIABLE,
          ObservationSelect.ENTITY,
          ObservationSelect.VALUE,
      ]

    self.RequiredSelect = {"variable", "entity"}
    self.normalize()
    self.validate()

  def normalize(self):
    """
        Normalizes the payload for consistent internal representation.

        - Converts `variable_dcids` and `entity_dcids` to lists if they are passed as strings.
        - Normalizes the `date` field to ensure it is in the correct format.
        """
    # Normalize variable
    if isinstance(self.variable_dcids, str):
      self.variable_dcids = [self.variable_dcids]

    # Normalize entity
    if isinstance(self.entity_dcids, str):
      self.entity_dcids = [self.entity_dcids]

    # Normalize date field
    if self.date.upper() == "ALL":
      self.date = ObservationDate.ALL
    elif (self.date.upper() == "LATEST") or (self.date == ""):
      self.date = ObservationDate.LATEST

  def validate(self):
    """
        Validates the payload to ensure consistency and correctness.

        Raises:
            ValueError: If both `entity_dcids` and `entity_expression` are set,
                        if neither is set, or if required fields are missing from `select`.
        """

    # Validate mutually exclusive entity fields
    if bool(self.entity_dcids) == bool(self.entity_expression):
      raise ValueError(
          "Exactly one of 'entity_dcids' or 'entity_expression' must be set.")

    # Check if all required fields are present
    missing_fields = self.RequiredSelect - set(self.select)
    if missing_fields:
      raise ValueError(
          f"The 'select' field must include at least the following: {', '.join(self.RequiredSelect)} "
          f"(missing: {', '.join(missing_fields)})")

    # Check all select fields are valid
    [ObservationSelect(select_field) for select_field in self.select]

  @property
  def to_dict(self) -> dict:
    """
        Converts the payload into a dictionary format for API requests.

        Returns:
            dict: The normalized and validated payload.
        """
    return {
        "date": self.date,
        "variable": {
            "dcids": self.variable_dcids
        },
        "entity": ({
            "dcids": self.entity_dcids
        } if self.entity_dcids else {
            "expression": self.entity_expression
        }),
        "select": self.select,
    }


@dataclass
class ResolveRequestPayload(EndpointRequestPayload):
  """
    A dataclass to structure, normalize, and validate the payload for a Resolve V2 API request.

    Attributes:
        node_dcids (str | list[str]): The DCID(s) of the nodes to query.
        expression (str): The relation expression to query.
    """

  node_dcids: str | list[str]
  expression: str

  def __post_init__(self):
    self.normalize()
    self.validate()

  def normalize(self):
    if isinstance(self.node_dcids, str):
      self.node_dcids = [self.node_dcids]

  def validate(self):
    if not isinstance(self.expression, str):
      raise ValueError("Expression must be a string.")

  @property
  def to_dict(self) -> dict:
    return {"nodes": self.node_dcids, "property": self.expression}
