from typing import Any, Optional

from datacommons_client.endpoints.base import API
from datacommons_client.endpoints.base import Endpoint
from datacommons_client.endpoints.payloads import ResolveRequestPayload
from datacommons_client.endpoints.response import ResolveResponse


def flatten_resolve_response(data: ResolveResponse) -> dict[str, Any]:
    """
    Flattens resolved candidate data into a dictionary where each node maps to its candidates.

    Args:
        data (ResolveResponse): The response object containing the resolved data.

    Returns:
        dict[str, Any]: A dictionary mapping nodes to their candidates.
        If a node has only one candidate, it maps directly to the candidate instead of a list.
    """
    items: dict[str, Any] = {}

    for entity in data.entities:
        node = entity.node
        if len(entity.candidates) == 1:
            items[node] = entity.candidates[0].dcid
        else:
            items[node] = [candidate.dcid for candidate in entity.candidates]

    return items


def resolve_correspondence_expression(
    from_type: str, to_type: str, entity_type: str | None = None
) -> str:
    """
    Constructs a relation expression for fetching correspondence between entities of two types.

    Args:
        from_type (str): The source entity type.
        to_type (str): The target entity type.
        entity_type (Optional[str]): Optional type of the entities.

    Returns:
        str: The relation expression to fetch correspondence between entities of the given types.
    """
    return (
        f"<-{from_type}{{typeOf:{entity_type}}}->{to_type}"
        if entity_type
        else f"<-{from_type}->{to_type}"
    )


class ResolveEndpoint(Endpoint):
    """
    A class to interact with the resolve API endpoint.

    Args:
        api (API): The API instance providing the environment configuration
            (base URL, headers, authentication) to be used for requests.
    """

    def __init__(self, api: API):
        """Initializes the ResolveEndpoint instance."""
        super().__init__(endpoint="resolve", api=api)

    def fetch(
        self, node_dcids: str | list[str], expression: str | list[str]
    ) -> ResolveResponse:
        """
        Fetches resolved data for the given nodes and expressions.

        Args:
            node_dcids (str | list[str]): One or more node IDs to resolve.
            expression (str): The relation expression to query.

        Returns:
            ResolveResponse: The response object containing the resolved data.
        """
        # Check if the node_dcids is a single string. If so, convert it to a list.
        if isinstance(node_dcids, str):
            node_dcids = [node_dcids]

        # Construct the payload
        payload = ResolveRequestPayload(
            node_dcids=node_dcids, expression=expression
        ).to_dict

        # Send the request and return the response
        return ResolveResponse.from_json(self.post(payload))

    def fetch_dcid_by_name(
        self, names: str | list[str], entity_type: Optional[str] = None
    ) -> ResolveResponse:
        """
        Fetches DCIDs for entities by their names.

        Args:
            names (str | list[str]): One or more entity names to resolve.
            entity_type (Optional[str]): Optional type of the entities.

        Returns:
            ResolveResponse: The response object containing the resolved DCIDs.
        """

        expression = resolve_correspondence_expression(
            from_type="description", to_type="dcid", entity_type=entity_type
        )

        return self.fetch(node_dcids=names, expression=expression)

    def fetch_dcid_by_wikidata_id(
        self, wikidata_id: str | list[str], entity_type: Optional[str] = None
    ) -> ResolveResponse:
        """
        Fetches DCIDs for entities by their Wikidata IDs.

        Args:
            wikidata_id (str | list[str]): One or more Wikidata IDs to resolve.
            entity_type (Optional[str]): Optional type of the entities.

        Returns:
            ResolveResponse: The response object containing the resolved DCIDs.
        """
        expression = resolve_correspondence_expression(
            from_type="wikidataId", to_type="dcid", entity_type=entity_type
        )

        return self.fetch(node_dcids=wikidata_id, expression=expression)

    def fetch_dcid_by_coordinates(
        self, latitude: str, longitude: str, entity_type: Optional[str] = None
    ) -> ResolveResponse:
        """
        Fetches DCIDs for entities by their geographic coordinates.

        Args:
            latitude (str): Latitude of the entity.
            longitude (str): Longitude of the entity.
            entity_type (Optional[str]): Optional type of the entities.

        Returns:
            ResolveResponse: The response object containing the resolved DCIDs.
        """
        expression = resolve_correspondence_expression(
            from_type="geoCoordinate", to_type="dcid", entity_type=entity_type
        )
        coordinates = f"{latitude}#{longitude}"
        return self.fetch(node_dcids=coordinates, expression=expression)

    def fetch_from_type_to_type(
        self,
        entities: str | list[str],
        from_type: str,
        to_type: str,
        entity_type: str | None = None,
    ) -> ResolveResponse:
        """
        Fetches the correspondence between entities of two types.

        Args:
            entities (str | list[str]): The entities to resolve.
            from_type (str): The source entity type.
            to_type (str): The target entity type.
            entity_type (Optional[str]): Optional type of the entities.

        Returns:
            ResolveResponse: The response object containing the resolved correspondence.
        """
        expression = resolve_correspondence_expression(
            from_type=from_type, to_type=to_type, entity_type=entity_type
        )
        return self.fetch(node_dcids=entities, expression=expression)