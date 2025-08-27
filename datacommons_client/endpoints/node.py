from concurrent.futures import ThreadPoolExecutor
from functools import partial
from functools import wraps
from typing import Literal, Optional
import warnings

from datacommons_client.endpoints.base import API
from datacommons_client.endpoints.base import Endpoint
from datacommons_client.endpoints.payloads import NodeRequestPayload
from datacommons_client.endpoints.payloads import normalize_list_to_string
from datacommons_client.endpoints.response import NodeResponse
from datacommons_client.models.node import Name
from datacommons_client.models.node import Node
from datacommons_client.models.node import StatVarConstraint
from datacommons_client.models.node import StatVarConstraints
from datacommons_client.utils.graph import build_graph_map
from datacommons_client.utils.graph import build_relationship_tree
from datacommons_client.utils.graph import fetch_relationship_lru
from datacommons_client.utils.graph import flatten_relationship
from datacommons_client.utils.names import DEFAULT_NAME_LANGUAGE
from datacommons_client.utils.names import DEFAULT_NAME_PROPERTY
from datacommons_client.utils.names import extract_name_from_english_name_property
from datacommons_client.utils.names import extract_name_from_property_with_language
from datacommons_client.utils.names import NAME_WITH_LANGUAGE_PROPERTY

PLACES_MAX_WORKERS = 10

CONSTRAINT_PROPERTY: str = "constraintProperties"

_DEPRECATED_METHODS: dict[str, dict[str, str | dict[str, str]]] = {
    "fetch_entity_parents": {
        "new_name": "fetch_place_parents",
        "arg_map": {
            "entity_dcids": "place_dcids"
        }
    },
    "fetch_entity_ascendancy": {
        "new_name": "fetch_place_ancestors",
        "arg_map": {
            "entity_dcids": "place_dcids"
        }
    }
}


class NodeEndpoint(Endpoint):
  """Initializes the NodeEndpoint with a given API configuration.

    Args:
        api (API): The API instance providing the environment configuration
            (base URL, headers, authentication) to be used for requests.
    """

  def __init__(self, api: API):
    """Initializes the NodeEndpoint with a given API configuration."""
    super().__init__(endpoint="node", api=api)

  def __getattr__(self, name):
    if name in _DEPRECATED_METHODS:
      method_info = _DEPRECATED_METHODS[name]
      new_name = method_info["new_name"]
      arg_map = method_info.get("arg_map", {})
      new_method = getattr(self, new_name)

      @wraps(new_method)
      def wrapper(*args, **kwargs):
        for old_arg, new_arg in arg_map.items():
          if old_arg in kwargs:
            warnings.warn(
                f"Argument '{old_arg}' has been renamed and will removed"
                f" in a future version. Use '{new_arg}' instead.",
                category=DeprecationWarning,
                stacklevel=2)
            if new_arg not in kwargs:
              kwargs[new_arg] = kwargs.pop(old_arg)

        warnings.warn(
            f"'{name}' is deprecated and will be removed in a future version. "
            f"Use '{new_name}' instead.",
            category=DeprecationWarning,
            stacklevel=2)
        return new_method(*args, **kwargs)

      return wrapper
    raise AttributeError(
        f"'{self.__class__.__name__}' object has no attribute '{name}'")

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
                                 expression=expression).to_dict()

    # Make the request and return the response.
    return NodeResponse.model_validate(
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
    properties = normalize_list_to_string(properties)

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
      if not properties:
        continue
      if language == "en":
        name = extract_name_from_english_name_property(
            properties=properties.get(name_property, []))
        lang_used = "en"
      else:
        name, lang_used = extract_name_from_property_with_language(
            properties=properties.get(name_property, []),
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

  def _fetch_contained_in_place(
      self,
      node_dcids: str | list[str],
      out: bool = True,
      contained_type: Optional[str] = None,
      as_dict: bool = False,
  ) -> dict[str, list[Node | dict]]:
    """Fetches places that contain or are contained in the given nodes. Uses the
          `containedInPlace` property to fetch parent or child place relationships.

        Args:
            node_dcids (str | list[str]): One or more DCIDs representing geographic places.
            out (bool, optional): If True, fetch places contained in the given node(s).
                If False, fetch places that contain the given node(s). Defaults to True.
            contained_type (str, optional): Optional type constraint (e.g., 'Country',
                'Country'). If provided, only fetches places of that type.
            as_dict (bool, optional): If True, returns the result as a dictionary of
                lists of dictionaries. If False, returns Node objects. Defaults to False.

        Returns:
            dict[str, list[dict]] | dict[str, list[Any]]: A dictionary where keys are DCIDs
            and values are lists of place relationships, either as raw objects or
            dictionaries (if `as_dict` is True).
        """
    if out and contained_type:
      raise ValueError("When 'out' is True, `contained_type' must be None.")

    prop = "containedInPlace+" if contained_type else "containedInPlace"

    data = self.fetch_property_values(
        node_dcids=node_dcids,
        properties=prop,
        out=out,
        constraints=f"typeOf:{contained_type}" if contained_type else None,
    ).get_properties()

    result = {}
    for entity, property_nodes in data.items():
      nodes = property_nodes.get(prop, [])
      result[entity] = [node.to_dict() for node in nodes] if as_dict else nodes

    return result

  def fetch_place_parents(
      self,
      place_dcids: str | list[str],
      *,
      as_dict: bool = True,
  ) -> dict[str, list[Node | dict]]:
    """Fetches the direct parents of one or more entities using the 'containedInPlace' property.

        Args:
            place_dcids (str | list[str]): A single place DCID or a list of DCIDs to query.
            as_dict (bool): If True, returns a dictionary mapping each input DCID to its
                immediate parent entities. If False, returns a dictionary of Node objects.

        Returns:
            dict[str, list[Node | dict]]: A dictionary mapping each input DCID to a list of its
            immediate parent entities. Each parent is represented as a Node object or
            as a dictionary with the same data.
        """
    return self._fetch_contained_in_place(
        node_dcids=place_dcids,
        out=True,
        contained_type=None,
        as_dict=as_dict,
    )

  def fetch_place_children(
      self,
      place_dcids: str | list[str],
      *,
      children_type: Optional[str] = None,
      as_dict: bool = True,
  ) -> dict[str, list[Node | dict]]:
    """Fetches the direct children of one or more entities using the 'containedInPlace' property.

        Args:
            place_dcids (str | list[str]): A single place DCID or a list of DCIDs to query.
            children_type (str, optional): The type of the child entities to
                fetch (e.g., 'Country', 'State', 'IPCCPlace_50'). If None, fetches all child types.
            as_dict (bool): If True, returns a dictionary mapping each input DCID to its
                immediate children entities. If False, returns a dictionary of Node objects.

        Returns:
            dict[str, list[Node | dict]]: A dictionary mapping each input DCID to a list of its
            immediate children. Each child is represented as a Node object or as a dictionary with
            the same data.
        """
    return self._fetch_contained_in_place(
        node_dcids=place_dcids,
        out=False,
        contained_type=children_type,
        as_dict=as_dict,
    )

  def _fetch_place_relationships(
      self,
      place_dcids: str | list[str],
      as_tree: bool = False,
      *,
      contained_type: Optional[str] = None,
      relationship: Literal["parents", "children"],
      max_concurrent_requests: Optional[int] = PLACES_MAX_WORKERS,
  ) -> dict[str, list[dict[str, str]] | dict]:
    """Fetches a full ancestors/descendants map per place DCID.

        For each input place DCID, this method builds the complete graph using a
        breadth-first traversal and parallel fetching.

        Args:
            place_dcids (str | list[str]): One or more DCIDs of the entities whose ancestry
               will be fetched.
            as_tree (bool): If True, returns a nested tree structure; otherwise, returns a flat list.
                Defaults to False.
            contained_type (Optional[str]): The type of the ancestry to fetch (e.g., 'Country', 'State').
                If None, fetches all ancestry types.
            relationship (Literal["parents", "children"]): The type of relationship to fetch.
            max_concurrent_requests (Optional[int]): The maximum number of concurrent requests to make.
                Defaults to PLACES_MAX_WORKERS.
        Returns:
            dict[str, list[dict[str, str]] | dict]: A dictionary mapping each input DCID to either:
                - A flat list of Node dictionaries (if `as_tree` is False), or
                - A nested tree (if `as_tree` is True).
        """

    if isinstance(place_dcids, str):
      place_dcids = [place_dcids]

    result = {}

    # Create a partial function to fetch relationships with the current parameters
    fetch_fn = partial(
        fetch_relationship_lru,
        self,
        contained_type=contained_type,
        relationship=relationship,
    )

    # Use a thread pool to fetch ancestry graphs in parallel for each input entity
    with ThreadPoolExecutor(max_workers=max_concurrent_requests) as executor:
      futures = [
          executor.submit(build_graph_map, root=dcid, fetch_fn=fetch_fn)
          for dcid in place_dcids
      ]
      # Gather ancestry maps and postprocess into flat or nested form
      for future in futures:
        dcid, ancestry = future.result()
        if as_tree:
          ancestry = build_relationship_tree(root=dcid,
                                             graph=ancestry,
                                             relationship_key=relationship)
        else:
          ancestry = flatten_relationship(ancestry)
        result[dcid] = ancestry

    return result

  def fetch_place_ancestors(
      self,
      place_dcids: str | list[str],
      as_tree: bool = False,
      *,
      max_concurrent_requests: Optional[int] = PLACES_MAX_WORKERS,
  ) -> dict[str, list[dict[str, str]] | dict]:
    """Fetches the full ancestry (flat or nested) for one or more entities.
        For each input DCID, this method builds the complete ancestry graph using a
        breadth-first traversal and parallel fetching.
        It returns either a flat list of unique parents or a nested tree structure for
        each entity, depending on the `as_tree` flag. The flat list matches the structure
        of the `/api/place/parent` endpoint of the DC website.
        Args:
            place_dcids (str | list[str]): One or more DCIDs of the entities whose ancestry
               will be fetched.
            as_tree (bool): If True, returns a nested tree structure; otherwise, returns a flat list.
                Defaults to False.
            max_concurrent_requests (Optional[int]): The maximum number of concurrent requests to make.
                Defaults to PLACES_MAX_WORKERS.
        Returns:
            dict[str, list[dict[str, str]] | dict]: A dictionary mapping each input DCID to either:
                - A flat list of parent dictionaries (if `as_tree` is False), or
                - A nested ancestry tree (if `as_tree` is True). Each parent is represented by
                  a dict with 'dcid', 'name', and 'type'.
        """

    return self._fetch_place_relationships(
        place_dcids=place_dcids,
        as_tree=as_tree,
        contained_type=None,
        relationship="parents",
        max_concurrent_requests=max_concurrent_requests,
    )

  def fetch_place_descendants(
      self,
      place_dcids: str | list[str],
      descendants_type: Optional[str] = None,
      as_tree: bool = False,
      *,
      max_concurrent_requests: Optional[int] = PLACES_MAX_WORKERS,
  ) -> dict[str, list[dict[str, str]] | dict]:
    """Fetches the full descendants (flat or nested) for one or more entities.
        For each input DCID, this method builds the complete descendants graph using a
        breadth-first traversal and parallel fetching.

        It returns either a flat list of unique child or a nested tree structure for
        each entity, depending on the `as_tree` flag.

        Args:
            place_dcids (str | list[str]): One or more DCIDs of the entities whose descendants
               will be fetched.
            descendants_type (Optional[str]): The type of the descendants to fetch (e.g., 'Country', 'State').
                If None, fetches all descendant types.
            as_tree (bool): If True, returns a nested tree structure; otherwise, returns a flat list.
                Defaults to False.
            max_concurrent_requests (Optional[int]): The maximum number of concurrent requests to make.
                Defaults to PLACES_MAX_WORKERS.
        Returns:
            dict[str, list[dict[str, str]] | dict]: A dictionary mapping each input DCID to either:
                - A flat list of Node dictionaries (if `as_tree` is False), or
                - A nested ancestry tree (if `as_tree` is True). Each child is represented by
                  a dict.
        """

    return self._fetch_place_relationships(
        place_dcids=place_dcids,
        as_tree=as_tree,
        contained_type=descendants_type,
        relationship="children",
        max_concurrent_requests=max_concurrent_requests,
    )

  def _fetch_property_id_names(self, node_dcids: str | list[str],
                               properties: str | list[str]):
    """Fetch target nodes for given properties and return only (dcid, name).

        For each input node and each requested property, returns the list of target
        nodes as dictionaries with ``dcid`` and ``name``.

        Args:
            node_dcids: A single DCID or a list of DCIDs to query.
            properties: A property string or list of property strings.

        Returns:
            A mapping:
            `{ node_dcid: { property: [ {dcid, name}, ... ], ... }, ... }`.
        """
    data = self.fetch_property_values(node_dcids=node_dcids,
                                      properties=properties).get_properties()

    result: dict[str, dict[str, list[dict]]] = {}

    for node, props in data.items():
      result.setdefault(node, {})
      for prop, metadata in props.items():
        dest = result[node].setdefault(prop, [])
        for n in metadata:
          # Prefer 'dcid', but if property is terminal, fall back to 'value'.
          dcid = n.dcid or n.value
          name = n.name or n.value
          dest.append({"dcid": dcid, "name": name})
    return result

  def fetch_statvar_constraints(
      self, variable_dcids: str | list[str]) -> StatVarConstraints:
    """Fetch constraint property/value pairs for statistical variables, using
        the `constraintProperties` property.

        This returns, for each StatisticalVariable, the constraints that define it.

        Args:
            variable_dcids: One or more StatisticalVariable DCIDs.

        Returns:
            StatVarConstraints:
            ``{
                <sv_dcid>: [
                    {
                        "constraint_id": <constraint_property_dcid>,
                        "constraint_name": <constraint_property_name>,
                        "value_id": <value_node_dcid>,
                        "value_name": <value_node_name>,
                    },
                    ...
                ],
                ...
            }``
        """
    # Ensure variable_dcids is a list
    if isinstance(variable_dcids, str):
      variable_dcids = [variable_dcids]

    # Get constraints for the given variable DCIDs.
    constraints_mapping = self._fetch_property_id_names(
        node_dcids=variable_dcids, properties=[CONSTRAINT_PROPERTY])

    # Per statvar mapping of dcid - name
    per_sv_constraint_names = {}
    # Global set of all constraint property IDs
    all_constraint_prop_ids = set()

    for sv in variable_dcids:
      # Get the constraint properties for this statvar
      prop_entries = constraints_mapping.get(sv,
                                             {}).get(CONSTRAINT_PROPERTY, [])
      # Map the constraint properties to their names
      id_to_name = {entry["dcid"]: entry.get("name") for entry in prop_entries}
      # Add an entry for this statvar to the constraint names mapping
      per_sv_constraint_names[sv] = id_to_name
      # Update the global set of all constraint property IDs
      all_constraint_prop_ids.update(id_to_name.keys())

    # In a single request, fetch all values for all the constraints, for all statvars.
    values_map = self._fetch_property_id_names(
        node_dcids=variable_dcids,
        properties=sorted(all_constraint_prop_ids),
    )

    # Build structured response. This will include vars with no constraints (empty dicts).
    result = {sv: [] for sv in variable_dcids}

    for sv in variable_dcids:
      constraint_names = per_sv_constraint_names.get(sv, {})
      sv_values = values_map.get(sv, {})

      for constraintId, constraintName in constraint_names.items():
        values = sv_values.get(constraintId, [])
        # Continue if the stat var doesn't actually define a value for one of its constraintProperties.
        if not values:
          continue

        # Build the StatVarConstraint object
        result[sv].append(
            StatVarConstraint(
                constraintId=constraintId,
                constraintName=constraintName,
                valueId=values[0]["dcid"],
                valueName=values[0].get("name"),
            ))

    return StatVarConstraints.model_validate(result)
