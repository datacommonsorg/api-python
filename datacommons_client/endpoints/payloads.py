from typing import Optional

from pydantic import Field
from pydantic import field_serializer
from pydantic import field_validator
from pydantic import model_serializer
from pydantic import model_validator

from datacommons_client.models.base import BaseDCModel
from datacommons_client.models.base import ListOrStr
from datacommons_client.models.observation import ObservationDate
from datacommons_client.models.observation import ObservationSelect
from datacommons_client.models.observation import ObservationSelectList


def normalize_list_to_string(value: str | list[str]) -> str:
  """Converts a list of properties to a string."""

  if isinstance(value, list):
    return f"[{', '.join(value)}]"

  return value


class NodeRequestPayload(BaseDCModel):
  """
    A Pydantic model to structure, normalize, and validate the payload for a Node V2 API request.

    Attributes:
        node_dcids (str | list[str]): The DCID(s) of the nodes to query.
        expression (str): The property or relation expression(s) to query.
    """

  node_dcids: ListOrStr = Field(..., serialization_alias="nodes")
  expression: list | str = Field(..., serialization_alias="property")


class ObservationRequestPayload(BaseDCModel):
  """
    A Pydantic model to structure, normalize, and validate the payload for an Observation V2 API request.

    Attributes:
        date (str): The date for which data is being requested.
        variable_dcids (str | list[str]): One or more variable IDs for the data.
        select (list[ObservationSelect]): Fields to include in the response.
          Defaults to ["date", "variable", "entity", "value"].
        entity_dcids (Optional[str | list[str]]): One or more entity IDs to filter the data.
        entity_expression (Optional[str]): A string expression to filter entities.
        filter_facet_domains (Optional[str | list[str]]): One or more domain names to filter the data.
        filter_facet_ids (Optional[str | list[str]]): One or more facet IDs to filter the data.
    """

  date: ObservationDate | str = Field(default_factory=str,
                                      validate_default=True)
  variable_dcids: Optional[ListOrStr] = Field(default=None,
                                              serialization_alias="variable")
  select: Optional[list[str]] = None
  entity_dcids: Optional[ListOrStr] = None
  entity_expression: Optional[str | list[str]] = None
  filter_facet_domains: Optional[ListOrStr] = None
  filter_facet_ids: Optional[ListOrStr] = None

  @field_validator("date", mode="before")
  def _validate_date(cls, v):
    try:
      return ObservationDate(v)
    except ValueError:
      return v

  @field_validator("select", mode="before")
  def _coerce_select(cls, v):
    return ObservationSelectList.model_validate(v).select

  @field_validator("entity_expression", mode="before")
  def _coerce_expr(cls, v):
    if v is None:
      return v
    if isinstance(v, list):
      return normalize_list_to_string(v)
    if isinstance(v, str):
      return v
    raise TypeError("expression must be a string or list[str]")

  @field_serializer("variable_dcids", "entity_dcids", when_used="unless-none")
  def _serialise_dcids_fields(self, v):
    return {"dcids": v}

  @field_serializer("entity_expression", when_used="unless-none")
  def _serialise_expression_field(self, v):
    return {"expression": v}

  @model_validator(mode="after")
  def _check_one(self):
    if bool(self.entity_dcids) == bool(self.entity_expression):
      raise ValueError("Exactly one of dcids or expression must be set")
    return self

  @model_serializer(mode="wrap")
  def _wrap_filter(self, handler):
    # Normal dump
    data = handler(self)

    # pull out entity dcid or expression
    entity = data.pop("entity_dcids", None) or data.pop("entity_expression",
                                                        None)

    # add entity to the data dictionary
    data["entity"] = entity

    # pull out the two filter keys if present
    domains = data.pop("filter_facet_domains", None)
    ids = data.pop("filter_facet_ids", None)

    # only add "filter" if at least one is set
    if domains or ids:
      filter_dict = {}
      if domains is not None:
        filter_dict["domains"] = domains
      if ids is not None:
        filter_dict["facet_ids"] = ids
      data["filter"] = filter_dict

    return data


class ResolveRequestPayload(BaseDCModel):
  """
    A Pydantic model to structure, normalize, and validate the payload for a Resolve V2 API request.

    Attributes:
        node_dcids (str | list[str]): The DCID(s) of the nodes to query.
        expression (str): The relation expression to query.
    """

  node_dcids: ListOrStr = Field(..., serialization_alias="nodes")
  expression: str | list[str] = Field(..., serialization_alias="property")
