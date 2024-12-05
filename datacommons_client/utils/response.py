from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List

from datacommons_client.models.node import Arcs, NextToken, NodeDCID, Properties
from datacommons_client.models.observation import (
    Facet,
    Variable,
    facetID,
    variableDCID,
)
from datacommons_client.models.resolve import Entity
from datacommons_client.models.sparql import Row


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


@dataclass
class ObservationResponse:
    """Represents the response from the Data Commons API Observation endpoint.

    Attributes:
       byVariable: A dictionary of variable DCIDs and their corresponding data.
         facets: A dictionary of facet IDs and their corresponding data.
    """

    byVariable: Dict[variableDCID, Any] = field(default_factory=dict)
    facets: Dict[facetID, Any] = field(default_factory=dict)

    @classmethod
    def from_json(cls, json_data: Dict[str, Any]) -> "ObservationResponse":
        """Parses the data from the API response."""
        return cls(
            byVariable={
                variable: Variable.from_json(data)
                for variable, data in json_data.get("byVariable", {}).items()
            },
            facets={
                facet: Facet.from_json(data)
                for facet, data in json_data.get("facets", {}).items()
            },
        )

    @property
    def json(self):
        return asdict(self)

    def get_data_by_entity(self) -> Dict:
        """Unpacks the data for each entity, for each variable.

        Returns:
            Dict: The variables object from the response.
        """
        return {
            variable: data.byEntity
            for variable, data in self.byVariable.items()
        }

    def get_observations_as_records(self) -> List[Dict[str, Any]]:
        """Converts the observation data into a list of records.

        Returns:
            List[Dict[str, Any]]: A flattened list of observation records.
        """
        return observations_as_records(
            data=self.get_data_by_entity(), facets=self.facets
        )


def extract_observations(
    variable: str, entity: str, entity_data: dict, facet_metadata: dict
) -> list[dict]:
    """
    Extracts observations for a given variable, entity, and its data.

    Args:
        variable (str): The variable name.
        entity (str): The entity name.
        entity_data (dict): Data for the entity, including ordered facets.
        facet_metadata (dict): Metadata for facets.

    Returns:
        list[dict]: A list of observation records.
    """
    # Store observation records
    records = []

    # Extract observations
    for facet in entity_data.get("orderedFacets", []):
        facet_id = facet.facetId
        metadata = facet_metadata.get(facet_id, {})
        records.extend(
            {
                "date": observation.date,
                "entity": entity,
                "variable": variable,
                "value": observation.value,
                "facetId": facet_id,
                **asdict(metadata),
            }
            for observation in facet.observations
        )
    return records


def observations_as_records(data: dict, facets: dict) -> list[dict]:
    """
    Converts observation data into a list of records.

    Args:
        data (dict): A mapping of variables to entities and their data.
        facets (dict): Facet metadata for the observations.

    Returns:
        list[dict]: A flattened list of observation records.
    """
    return [
        record
        for variable, entities in data.items()
        for entity, entity_data in entities.items()
        for record in extract_observations(
            variable=variable,
            entity=entity,
            entity_data=entity_data,
            facet_metadata=facets,
        )
    ]


@dataclass
class ResolveResponse:
    """Represents the response from the Data Commons API Resolve endpoint.

    Attributes:
        entities (List[Entity]): A list of entities resolved by the API, each
            containing the query node and its associated candidates.
    """

    entities: List[Entity] = field(default_factory=list)

    @classmethod
    def from_json(cls, json_data: Dict[str, Any]) -> "ResolveResponse":
        """Parses a ResolveResponse instance from JSON data.

        Args:
            json_data (Dict[str, Any]): A dictionary containing the API response
                data, with keys like "entities".

        Returns:
            ResolveResponse: A populated instance of the ResolveResponse class.
        """
        return cls(
            entities=[
                Entity.from_json(entity)
                for entity in json_data.get("entities", [])
            ]
        )

    @property
    def json(self):
        """Converts the ResolveResponse instance to a dictionary.

        This is useful for serializing the response data back into a JSON-compatible
        format.

        Returns:
            Dict[str, Any]: The dictionary representation of the ResolveResponse instance.
        """
        return asdict(self)


@dataclass
class SparqlResponse:
    """Represents the response from a SPARQL query.

    Attributes:
        header (List[str]): The list of strings representing the query header.
        rows (List[Row]): A list of rows, each containing cells with values.
    """

    header: List[str] = field(default_factory=list)
    rows: List[Row] = field(default_factory=list)

    @classmethod
    def from_json(cls, json_data: Dict[str, Any]) -> "SparqlResponse":
        """Parses a SparqlResponse instance from JSON data.

        Args:
            json_data (Dict[str, Any]): A dictionary containing the SPARQL query
                response data, with keys like "header" and "rows".

        Returns:
            SparqlResponse: A populated instance of the SparqlResponse class.
        """
        return cls(
            header=json_data.get("header", []),
            rows=[Row.from_json(row) for row in json_data.get("rows", [])],
        )

    def to_json(self) -> Dict[str, Any]:
        """Converts the SparqlResponse object to a JSON-compatible dictionary.

        Returns:
            Dict[str, Any]: A dictionary representing the SparqlResponse object,
            suitable for serialization into JSON format.
        """
        return asdict(self)
