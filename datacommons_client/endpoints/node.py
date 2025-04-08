from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from datacommons_client.endpoints.base import API
from datacommons_client.endpoints.base import Endpoint
from datacommons_client.endpoints.payloads import NodeRequestPayload
from datacommons_client.endpoints.payloads import normalize_properties_to_string
from datacommons_client.endpoints.response import NodeResponse
from datacommons_client.models.node import Name
from datacommons_client.models.node import Node
from datacommons_client.utils.graph import build_ancestry_map
from datacommons_client.utils.graph import build_ancestry_tree
from datacommons_client.utils.graph import fetch_parents_lru
from datacommons_client.utils.graph import flatten_ancestry
from datacommons_client.utils.names import DEFAULT_NAME_LANGUAGE
from datacommons_client.utils.names import DEFAULT_NAME_PROPERTY
from datacommons_client.utils.names import extract_name_from_english_name_property
from datacommons_client.utils.names import extract_name_from_property_with_language
from datacommons_client.utils.names import NAME_WITH_LANGUAGE_PROPERTY

ANCESTRY_MAX_WORKERS = 10


class NodeEndpoint(Endpoint):
  """Initializes the NodeEndpoint with a given API configuration.

    Args:
        api (API): The API instance providing the environment configuration
            (base URL, headers, authentication) to be used for requests.
    """

  def __init__(self, api: API):
    """Initializes the NodeEndpoint with a given API configuration."""
    super().__init__(endpoint="node", api=api)

  def fetch(
      self,
      node_dcids: str | list[str],
      expression: str,
      *,
      all_pages: bool = True,
      next_token: Optional[str] = None,
  ) -> NodeResponse:
    """Fetches properties or arcs for given nodes and properties.

        Args:
            node_dcids (str | List[str]): The DCID(s) of the nodes to query.
            expression (str): The property or relation expression(s) to query.
            all_pages: If True, fetch all pages of the response. If False, fetch only the first page.
              Defaults to True. Set to False to only fetch the first page. In that case, a
              `next_token` key in the response will indicate if more pages are available.
              That token can be used to fetch the next page.
            next_token: Optionally, the token to fetch the next page of results. Defaults to None.

        Returns:
            NodeResponse: The response object containing the queried data.

        Example:
            ```python
            response = node.fetch(
                node_dcids=["geoId/06"],
                expression="<-"
            )
            print(response)
            ```
        """

    # Create the payload
    payload = NodeRequestPayload(node_dcids=node_dcids,
                                 expression=expression).to_dict

    # Make the request and return the response.
    return NodeResponse.from_json(
        self.post(payload, all_pages=all_pages, next_token=next_token))

  def fetch_property_labels(
      self,
      node_dcids: str | list[str],
      out: bool = True,
      *,
      all_pages: bool = True,
      next_token: Optional[str] = None,
  ) -> NodeResponse:
    """Fetches all property labels for the given nodes.

        Args:
            node_dcids (str | list[str]): The DCID(s) of the nodes to query.
            out (bool): Whether to fetch outgoing properties (`->`). Defaults to True.
            all_pages: If True, fetch all pages of the response. If False, fetch only the first page.
              Defaults to True. Set to False to only fetch the first page. In that case, a
              `next_token` key in the response will indicate if more pages are available.
              That token can be used to fetch the next page.
            next_token: Optionally, the token to fetch the next page of results. Defaults to None.

        Returns:
            NodeResponse: The response object containing the property labels.

        Example:
            ```python
            response = node.fetch_property_labels(node_dcids="geoId/06")
            print(response)
            ```
        """
    # Determine the direction of the properties.
    expression = "->" if out else "<-"

    # Make the request and return the response.
    return self.fetch(
        node_dcids=node_dcids,
        expression=expression,
        all_pages=all_pages,
        next_token=next_token,
    )

  def fetch_property_values(
      self,
      node_dcids: str | list[str],
      properties: str | list[str],
      constraints: Optional[str] = None,
      out: bool = True,
      *,
      all_pages: bool = True,
      next_token: Optional[str] = None,
  ) -> NodeResponse:
    """Fetches the values of specific properties for given nodes.

        Args:
            node_dcids (str | List[str]): The DCID(s) of the nodes to query.
            properties (str | List[str]): The property or relation expression(s) to query.
            constraints (Optional[str]): Additional constraints for the query. Defaults to None.
            out (bool): Whether to fetch outgoing properties. Defaults to True.
            all_pages: If True, fetch all pages of the response. If False, fetch only the first page.
              Defaults to True. Set to False to only fetch the first page. In that case, a
              `next_token` key in the response will indicate if more pages are available.
              That token can be used to fetch the next page.
            next_token: Optionally, the token to fetch the next page of results. Defaults to None.


        Returns:
            NodeResponse: The response object containing the property values.

        Example:
            ```python
            response = node.fetch_property_values(
                node_dcids=["geoId/06"],
                properties="name",
                out=True
            )
            print(response)
            ```
        """

    # Normalize the input to a string (if it's a list), otherwise use the string as is.
    properties = normalize_properties_to_string(properties)

    # Construct the expression based on the direction and constraints.
    direction = "->" if out else "<-"
    expression = f"{direction}{properties}"
    if constraints:
      expression += f"{{{constraints}}}"

    return self.fetch(
        node_dcids=node_dcids,
        expression=expression,
        all_pages=all_pages,
        next_token=next_token,
    )

  def fetch_all_classes(
      self,
      *,
      all_pages: bool = True,
      next_token: Optional[str] = None,
  ) -> NodeResponse:
    """Fetches all Classes available in the Data Commons knowledge graph.

        Args:
          all_pages: If True, fetch all pages of the response. If False, fetch only the first page.
              Defaults to True. Set to False to only fetch the first page. In that case, a
              `next_token` key in the response will indicate if more pages are available.
              That token can be used to fetch the next page.
          next_token: Optionally, the token to fetch the next page of results. Defaults to None.


        Returns:
            NodeResponse: The response object containing all statistical variables.

        Example:
            ```python
            response = node.fetch_all_classes()
            print(response)
            ```
        """

    return self.fetch_property_values(
        node_dcids="Class",
        properties="typeOf",
        out=False,
        all_pages=all_pages,
        next_token=next_token,
    )

  def fetch_entity_names(
      self,
      entity_dcids: str | list[str],
      language: Optional[str] = DEFAULT_NAME_LANGUAGE,
      fallback_language: Optional[str] = None,
  ) -> dict[str, Name]:
    """
        Fetches entity names in the specified language, with optional fallback to English.
        Args:
          entity_dcids: A single DCID or a list of DCIDs to fetch names for.
          language: Language code (e.g., "en", "es"). Defaults to "en" (DEFAULT_NAME_LANGUAGE).
          fallback_language: If provided, this language will be used as a fallback if the requested
            language is not available. If not provided, no fallback will be used.
        Returns:
          A dictionary mapping each DCID to a dictionary with the mapped name, language, and
            the property used.
        """

    # Check if entity_dcids is a single string. If so, convert it to a list.
    if isinstance(entity_dcids, str):
      entity_dcids = [entity_dcids]

    # If langauge is English, use the more efficient 'name' property.
    name_property = (DEFAULT_NAME_PROPERTY if language == DEFAULT_NAME_LANGUAGE
                     else NAME_WITH_LANGUAGE_PROPERTY)

    # Fetch names the given entity DCIDs.
    data = self.fetch_property_values(
        node_dcids=entity_dcids, properties=name_property).get_properties()

    names: dict[str, Name] = {}

    # Iterate through the fetched data and populate the names dictionary.
    for dcid, properties in data.items():
      if language == "en":
        name = extract_name_from_english_name_property(properties=properties)
        lang_used = "en"
      else:
        name, lang_used = extract_name_from_property_with_language(
            properties=properties,
            language=language,
            fallback_language=fallback_language,
        )
      if name:
        names[dcid] = Name(
            value=name,
            language=lang_used,
            property=name_property,
        )

    return names

  def fetch_entity_parents(
      self,
      entity_dcids: str | list[str],
      *,
      as_dict: bool = True) -> dict[str, list[Node | dict]]:
    """Fetches the direct parents of one or more entities using the 'containedInPlace' property.

        Args:
            entity_dcids (str | list[str]): A single DCID or a list of DCIDs to query.
            as_dict (bool): If True, returns a dictionary mapping each input DCID to its
                immediate parent entities. If False, returns a dictionary of Parent objects (which
                are dataclasses).

        Returns:
            dict[str, list[Parent | dict]]: A dictionary mapping each input DCID to a list of its
            immediate parent entities. Each parent is represented as a Parent object (which
            contains the DCID, name, and type of the parent entity) or as a dictionary with
            the same data.
        """
    # Fetch property values from the API
    data = self.fetch_property_values(
        node_dcids=entity_dcids,
        properties="containedInPlace",
    ).get_properties()

    if as_dict:
      return {k: v.to_dict() for k, v in data.items()}

    return data

  def _fetch_parents_cached(self, dcid: str) -> tuple[Node, ...]:
    """Returns cached parent nodes for a given entity using an LRU cache.

        This private wrapper exists because `@lru_cache` cannot be applied directly
        to instance methods. By passing the `NodeEndpoint` instance (`self`) as an
        argument caching is preserved while keeping the implementation modular and testable.

        Args:
            dcid (str): The DCID of the entity whose parents should be fetched.

        Returns:
            tuple[Parent, ...]: A tuple of Parent objects representing the entity's immediate parents.
        """
    return fetch_parents_lru(self, dcid)

  def fetch_entity_ancestry(
      self,
      entity_dcids: str | list[str],
      as_tree: bool = False,
      *,
      max_concurrent_requests: Optional[int] = ANCESTRY_MAX_WORKERS
  ) -> dict[str, list[dict[str, str]] | dict]:
    """Fetches the full ancestry (flat or nested) for one or more entities.
        For each input DCID, this method builds the complete ancestry graph using a
        breadth-first traversal and parallel fetching.
        It returns either a flat list of unique parents or a nested tree structure for
        each entity, depending on the `as_tree` flag. The flat list matches the structure
        of the `/api/place/parent` endpoint of the DC website.
        Args:
            entity_dcids (str | list[str]): One or more DCIDs of the entities whose ancestry
               will be fetched.
            as_tree (bool): If True, returns a nested tree structure; otherwise, returns a flat list.
                Defaults to False.
            max_concurrent_requests (Optional[int]): The maximum number of concurrent requests to make.
                Defaults to ANCESTRY_MAX_WORKERS.
        Returns:
            dict[str, list[dict[str, str]] | dict]: A dictionary mapping each input DCID to either:
                - A flat list of parent dictionaries (if `as_tree` is False), or
                - A nested ancestry tree (if `as_tree` is True). Each parent is represented by
                  a dict with 'dcid', 'name', and 'type'.
        """

    if isinstance(entity_dcids, str):
      entity_dcids = [entity_dcids]

    result = {}

    # Use a thread pool to fetch ancestry graphs in parallel for each input entity
    with ThreadPoolExecutor(max_workers=max_concurrent_requests) as executor:
      futures = [
          executor.submit(build_ancestry_map,
                          root=dcid,
                          fetch_fn=self._fetch_parents_cached)
          for dcid in entity_dcids
      ]

      # Gather ancestry maps and postprocess into flat or nested form
      for future in futures:
        dcid, ancestry = future.result()
        if as_tree:
          ancestry = build_ancestry_tree(dcid, ancestry)
        else:
          ancestry = flatten_ancestry(ancestry)
        result[dcid] = ancestry

    return result
