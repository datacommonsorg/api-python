from dataclasses import asdict, dataclass, field
from typing import Any, Dict

from datacommons_client.models.node import Arcs, NextToken, NodeDCID, Properties


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


@dataclass
class NodeResponse:
    """Represents the response from the Data Commons API.

    Attributes:
        data: A dictionary mapping node DCIDs to Arcs or Properties objects.
        nextToken: A token for pagination, if present.
    """

    data: Dict[NodeDCID, Arcs | Properties] = field(default_factory=dict)
    nextToken: NextToken = None

    @classmethod
    def from_json(cls, json_data: Dict[str, Any]) -> "NodeResponse":
        """Parses a dictionary of nodes from JSON.

        Args:
            json_data: The raw JSON data from the API response.

        Returns:
            A NodeResponse instance.
        """

        def parse_data(data: Dict[str, Any]) -> Arcs | Properties:
            if "arcs" in data:
                return Arcs.from_json(data["arcs"])
            return Properties.from_json(data)

        parsed_data = {
            dcid: parse_data(data)
            for dcid, data in json_data.get("data", {}).items()
        }
        return cls(data=parsed_data, nextToken=json_data.get("nextToken"))

    def get_properties(self) -> Dict:
        return flatten_properties(self.data)

    @property
    def json(self):
        return asdict(self)


def flatten_properties(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flatten the properties of a node response.

    Processes a dictionary of node responses, extracting and
    simplifying their properties and arcs into a flattened dictionary.

    Args:
        data (Dict[str, Dict[str, Any]]):
            The input dictionary containing node responses. Each node maps to
            a dictionary with potential "arcs" and "properties" keys.

    Returns:
        Dict[str, Any]:
            A flattened dictionary where keys are node identifiers, and values
            are the simplified properties or nodes.
    """

    # Store simplified properties
    items = {}

    for node, node_data in data.items():
        # If arcs are present, process them
        if hasattr(node_data, "arcs"):
            processed_arcs = _unpack_arcs(node_data.arcs)
            if processed_arcs is not None:
                items[node] = processed_arcs
                continue

        # Include properties if present
        if hasattr(node_data, "properties"):
            items[node] = node_data.properties

        return items


def _unpack_arcs(arcs: Dict[str, Any]) -> Any:
    """Simplify the 'arcs' structure."""
    if len(arcs) > 1:
        # Multiple arcs: return dictionary of property nodes
        return {prop: arc_data["nodes"] for prop, arc_data in arcs.items()}

    # Single arc: extract first node's data
    for property_data in arcs.values():
        nodes = property_data.get("nodes", [])
        return nodes if len(nodes) > 1 else (nodes[0] if nodes else None)
