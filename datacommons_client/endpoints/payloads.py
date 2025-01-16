from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from dataclasses import field
from enum import Enum
from typing import Optional


@dataclass
class EndpointPayload(ABC):
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
class NodePayload(EndpointPayload):
    """
    A dataclass to structure, normalize, and validate the payload for a Node V2 API request.

    Attributes:
        nodes (str | list[str]): The DCID(s) of the nodes to query.
        prop (str): The property or relation expression(s) to query.
    """

    nodes: str | list[str]
    prop: str

    def __post_init__(self):
        self.normalize()
        self.validate()

    def normalize(self):
        if isinstance(self.nodes, str):
            self.nodes = [self.nodes]

    def validate(self):
        if not isinstance(self.prop, str):
            raise ValueError("Property must be a string.")

    @property
    def to_dict(self) -> dict:
        return {"nodes": self.nodes, "property": self.prop}


class ObservationSelect(str, Enum):
    DATE = "date"
    VARIABLE = "variable"
    ENTITY = "entity"
    VALUE = "value"


class ObservationDate(str, Enum):
    LATEST = "LATEST"
    ALL = ""


@dataclass
class ObservationPayload(EndpointPayload):
    """
    A dataclass to structure, normalize, and validate the payload for an Observation V2 API request.

    Attributes:
        date (str): The date for which data is being requested.
        variable_dcids (str | list[str]): One or more variable IDs for the data.
        select (list[ObservationSelect]): Fields to include in the response.
        entity_dcids (Optional[str | list[str]]): One or more entity IDs to filter the data.
        entity_expression (Optional[str]): A string expression to filter entities.
    """

    date: ObservationDate | str = ""
    variable_dcids: str | list[str] = field(default_factory=list)
    select: list[ObservationSelect | str] = field(default_factory=list)
    entity_dcids: Optional[str | list[str]] = None
    entity_expression: Optional[str] = None

    def __post_init__(self):
        """
        Initializes the payload, performing validation and normalization.

        Raises:
            ValueError: If validation rules are violated.
        """
        self.RequiredSelect = {"variable", "entity"}
        self.normalize()
        self.validate()

    def normalize(self):
        """
        Normalizes the payload for consistent internal representation.

        - Converts `variable_dcids` and `entity_dcids` to lists if they are passed as strings.
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
                "Exactly one of 'entity_dcids' or 'entity_expression' must be set."
            )

        # Check if all required fields are present
        missing_fields = self.RequiredSelect - set(self.select)
        if missing_fields:
            raise ValueError(
                f"'entity' and 'variable' must be selected (missing: {', '.join(missing_fields)})"
            )

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
            "variable": {"dcids": self.variable_dcids},
            "entity": (
                {"dcids": self.entity_dcids}
                if self.entity_dcids
                else {"expression": self.entity_expression}
            ),
            "select": self.select,
        }


@dataclass
class ResolvePayload(EndpointPayload):
    """
    A dataclass to structure, normalize, and validate the payload for a Resolve V2 API request.

    Attributes:
        nodes (str | list[str]): The DCID(s) of the nodes to query.
        expression (str): The relation expression to query.
    """

    nodes: str | list[str]
    expression: str

    def __post_init__(self):
        self.normalize()
        self.validate()

    def normalize(self):
        if isinstance(self.nodes, str):
            self.nodes = [self.nodes]

    def validate(self):
        if not isinstance(self.expression, str):
            raise ValueError("Expression must be a string.")

    @property
    def to_dict(self) -> dict:
        return {"nodes": self.nodes, "property": self.expression}
