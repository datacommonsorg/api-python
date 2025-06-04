from enum import Enum
from typing import List, Optional

from pydantic import Field
from pydantic import field_validator
from pydantic import model_serializer
from pydantic import RootModel

from datacommons_client.models.base import BaseDCModel
from datacommons_client.models.base import DictLikeRootModel
from datacommons_client.models.base import entityDCID
from datacommons_client.models.base import facetID
from datacommons_client.models.base import ListLikeRootModel
from datacommons_client.models.base import variableDCID
from datacommons_client.utils.error_handling import InvalidObservationSelectError


class ObservationDate(str, Enum):
  LATEST = "LATEST"
  ALL = ""

  @classmethod
  def _missing_(cls, value):
    if isinstance(value, str):
      u = value.strip().upper()
      if u == "LATEST":
        return cls.LATEST
      if u in ("ALL", ""):
        return cls.ALL
    raise ValueError(f"Invalid date value: '{value}'. Only 'LATEST' or"
                     f" '' (empty string) are allowed.")


class ObservationSelect(str, Enum):
  DATE = "date"
  VARIABLE = "variable"
  ENTITY = "entity"
  VALUE = "value"
  FACET = "facet"

  @classmethod
  def valid_values(cls):
    """Returns a list of valid enum values."""
    return sorted(cls._value2member_map_.keys())

  @classmethod
  def _missing_(cls, value):
    """Handle missing enum values by raising a custom error."""
    message = f"Invalid `select` Field: '{value}'. Only {', '.join(cls.valid_values())} are allowed."
    raise InvalidObservationSelectError(message=message)


class ObservationSelectList(RootModel[list[ObservationSelect]]):
  """A model to represent a list of ObservationSelect values.

    Attributes:
        select (List[ObservationSelect]): A list of ObservationSelect enum values.
    """

  root: Optional[list[ObservationSelect | str]] = None

  @field_validator("root", mode="before")
  def _validate_select(cls, v):
    if v is None:
      select = [
          ObservationSelect.DATE,
          ObservationSelect.VARIABLE,
          ObservationSelect.ENTITY,
          ObservationSelect.VALUE,
      ]
    else:
      select = v

    select = [ObservationSelect(s).value for s in select]

    required_select = {"variable", "entity"}

    missing_fields = required_select - set(select)
    if missing_fields:
      raise InvalidObservationSelectError(message=(
          f"The 'select' field must include at least the following: {', '.join(required_select)} "
          f"(missing: {', '.join(missing_fields)})"))

    return select

  @property
  def select(self) -> list[str]:
    """Return select values directly as list"""
    return self.root or []


class Observation(BaseDCModel):
  """Represents an observation with a date and value.

    Attributes:
        date (str): The date of the observation.
        value (float): Optional. The value of the observation.
    """

  date: Optional[str] = None
  value: Optional[float] = None


class OrderedFacet(BaseDCModel):
  """Represents ordered facets of observations.

    Attributes:
        earliestDate (str): The earliest date in the observations.
        facetId (str): The identifier for the facet.
        latestDate (str): The latest date in the observations.
        obsCount (int): The total number of observations.
        observations (List[Observation]): A list of observations associated with the facet.
    """

  earliestDate: Optional[str] = None
  facetId: Optional[str] = None
  latestDate: Optional[str] = None
  obsCount: Optional[int] = None
  observations: list[Observation] = Field(default_factory=list)


class OrderedFacets(BaseDCModel):
  """Represents a list of ordered facets.
  """
  orderedFacets: list[OrderedFacet] = Field(default_factory=list)


class Variable(BaseDCModel):
  """Represents a variable with data grouped by entity.

    Attributes:
        byEntity (dict[entityDCID, OrderedFacets]): A dictionary mapping
            entities to their ordered facets.
    """

  byEntity: dict[entityDCID, OrderedFacets] = Field(default_factory=dict)


class Facet(BaseDCModel):
  """Represents metadata for a facet.

    Attributes:
        importName (str): The name of the data import.
        measurementMethod (str): The method used to measure the data.
        observationPeriod (str): The period over which the observations were made.
        provenanceUrl (str): The URL of the data's provenance.
        unit (str): The unit of the observations.
    """

  importName: Optional[str] = None
  measurementMethod: Optional[str] = None
  observationPeriod: Optional[str] = None
  provenanceUrl: Optional[str] = None
  unit: Optional[str] = None


class ByVariable(BaseDCModel, DictLikeRootModel[dict[variableDCID, Variable]]):
  """A root model whose value is a dict mapping variableDCID to Variable."""


class VariableByEntity(BaseDCModel,
                       DictLikeRootModel[dict[variableDCID,
                                              dict[entityDCID,
                                                   OrderedFacets]]]):
  """A root model whose value is a dict mapping entityDCID to Variable."""


class ObservationRecord(Observation, Facet):
  """Represents a record of observations for a specific variable and entity.

    Attributes:
        date (str): The date of the observation.
        value (float): The value of the observation.
    """

  entity: Optional[entityDCID] = None
  variable: Optional[variableDCID] = None
  facetId: Optional[facetID] = None

  _order = [
      "date", "entity", "variable", "facetId", "importName",
      "measurementMethod", "observationPeriod", "provenanceUrl", "unit", "value"
  ]

  @model_serializer(mode="wrap")
  def _reorder(self, helper):
    """Reorders the fields for serialization."""
    data = helper(self)
    ordered = {}

    # Ensure the order of fields matches the specified order
    for key in self._order:
      if key in data:
        ordered[key] = data.pop(key)

    # Add any remaining fields that were not in the order list
    ordered.update(data)

    # Ensure the 'value' field is always at the end
    if "value" in ordered:
      ordered["value"] = ordered.pop("value")

    return ordered


class ObservationRecords(BaseDCModel,
                         ListLikeRootModel[list[ObservationRecord]]):
  """A root model whose value is a list of ObservationRecord."""
