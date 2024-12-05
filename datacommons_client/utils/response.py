from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass
class DCResponse:
    """Represents a structured response from the Data Commons API."""

    json: Dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        """Returns a string with the name of the object,"""
        return f"<Raw Data Commons API response>"

    @property
    def next_token(self):
        return self.json.get("nextToken")
