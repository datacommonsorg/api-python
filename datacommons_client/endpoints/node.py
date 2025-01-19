from typing import Optional

from datacommons_client.endpoints.base import API
from datacommons_client.endpoints.base import Endpoint
from datacommons_client.endpoints.payloads import NodeRequestPayload
from datacommons_client.endpoints.response import NodeResponse


def _normalize_expression_to_string(expression: str | list[str]) -> str:
<<<<<<< HEAD
  """Converts a list of expressions to a string."""
  if isinstance(expression, str):
    return expression

  return (f"[{', '.join(expression)}]"
          if isinstance(expression, list) else expression)


class NodeEndpoint(Endpoint):
  """Initializes the NodeEndpoint with a given API configuration.
=======
    """Converts a list of expressions to a string."""
    if isinstance(expression, str):
        return expression

    return (
        f"[{', '.join(expression)}]"
        if isinstance(expression, list)
        else expression
    )


class NodeEndpoint(Endpoint):
    """Initializes the NodeEndpoint with a given API configuration.
>>>>>>> 5172973 (`Node` endpoint and tests)

    Args:
        api (API): The API instance providing the environment configuration
            (base URL, headers, authentication) to be used for requests.
<<<<<<< HEAD
<<<<<<< HEAD
        max_pages (Optional[int]|None): Optionally, set the maximum number of pages to fetch.
            Defaults to None, which fetches all the pages.
    """

  def __init__(self, api: API, max_pages: Optional[int] = None):
    """Initializes the NodeEndpoint with a given API configuration."""
    super().__init__(endpoint="node", api=api)
    self.max_pages = max_pages

  def fetch(self, node_dcids: str | list[str],
            expression: str | list[str]) -> NodeResponse:
    """Fetches properties or arcs for given nodes and properties.
=======
=======
        max_pages (Optional[int]|None): Optionally, set the maximum number of pages to fetch.
            Defaults to None, which fetches all the pages.
>>>>>>> c37c913 (Improve Node docstring)
    """

    def __init__(self, api: API, max_pages: Optional[int] = None):
        """Initializes the NodeEndpoint with a given API configuration."""
        super().__init__(endpoint="node", api=api)
        self.max_pages = max_pages

    def fetch(
        self, node_dcids: str | list[str], expression: str | list[str]
    ) -> NodeResponse:
        """Fetches properties or arcs for given nodes and properties.
>>>>>>> 5172973 (`Node` endpoint and tests)

        Args:
            node_dcids (str | List[str]): The DCID(s) of the nodes to query.
            expression (str | List[str]): The property or relation expression(s) to query.

        Returns:
            NodeResponse: The response object containing the queried data.

        Example:
            ```python
            response = node_endpoint.fetch(
                nodes=["geoId/06"],
                property="<-"
            )
            print(response.data)
            ```
        """
<<<<<<< HEAD
    # Normalize the input expression
    expression = _normalize_expression_to_string(expression)

    # Create the payload
    payload = NodeRequestPayload(node_dcids=node_dcids,
                                 expression=expression).to_dict

    # Make the request and return the response.
    return NodeResponse.from_json(self.post(payload, max_pages=self.max_pages))

  def fetch_property_labels(self,
                            node_dcids: str | list[str],
                            out: bool = True) -> NodeResponse:
    """Fetches all property labels for the given nodes.
=======
        # Normalize the input expression
        expression = _normalize_expression_to_string(expression)

        # Create the payload
        payload = NodeRequestPayload(
            node_dcids=node_dcids, expression=expression
        ).to_dict

        # Make the request and return the response.
        return NodeResponse.from_json(
            self.post(payload, max_pages=self.max_pages)
        )

    def fetch_property_labels(
        self, node_dcids: str | list[str], out: bool = True
    ) -> NodeResponse:
        """Fetches all property labels for the given nodes.
>>>>>>> 5172973 (`Node` endpoint and tests)

        Args:
            node_dcids (str | list[str]): The DCID(s) of the nodes to query.
            out (bool): Whether to fetch outgoing properties (`->`). Defaults to True.

        Returns:
            NodeResponse: The response object containing the property labels.

        Example:
            ```python
            response = node_endpoint.fetch_properties(node_dcids="geoId/06")
            print(response.data)
            ```
        """
<<<<<<< HEAD
    # Determine the direction of the properties.
    expression = "->" if out else "<-"

    # Make the request and return the response.
    return self.fetch(node_dcids=node_dcids, expression=expression)

  def fetch_property_values(
      self,
      node_dcids: str | list[str],
      expression: str | list[str],
      constraints: Optional[str] = None,
      out: bool = True,
  ) -> NodeResponse:
    """Fetches the values of specific properties for given nodes.
=======
        # Determine the direction of the properties.
        expression = "->" if out else "<-"

        # Make the request and return the response.
        return self.fetch(node_dcids=node_dcids, expression=expression)

    def fetch_property_values(
        self,
        node_dcids: str | list[str],
        expression: str | list[str],
        constraints: Optional[str] = None,
        out: bool = True,
    ) -> NodeResponse:
        """Fetches the values of specific properties for given nodes.
>>>>>>> 5172973 (`Node` endpoint and tests)

        Args:
            node_dcids (str | List[str]): The DCID(s) of the nodes to query.
            expression (str | List[str]): The property or relation expression(s) to query.
            constraints (Optional[str]): Additional constraints for the query. Defaults to None.
            out (bool): Whether to fetch outgoing properties. Defaults to True.

        Returns:
            NodeResponse: The response object containing the property values.

        Example:
            ```python
            response = node_endpoint.fetch_property_values(
                node_dcids=["geoId/06"],
                expression="->name"
            )
            print(response.data)
            ```
        """

<<<<<<< HEAD
    # Normalize the input to a string (if it's a list), otherwise use the string as is.
    expression = _normalize_expression_to_string(expression)

    # Construct the expression based on the direction and constraints.
    if out:
      expression = f"->{expression}"
      if constraints:
        expression += f"{{{constraints}}}"
    else:
      expression = f"<-{expression}"
      if constraints:
        expression += f"{{{constraints}}}"

    return self.fetch(node_dcids=node_dcids, expression=expression)

  def fetch_all_classes(self) -> NodeResponse:
    """Fetches all Classes available in the Data Commons knowledge graph.
=======
        # Normalize the input to a string (if it's a list), otherwise use the string as is.
        expression = _normalize_expression_to_string(expression)

        # Construct the expression based on the direction and constraints.
        if out:
            expression = f"->{expression}"
            if constraints:
                expression += f"{{{constraints}}}"
        else:
            expression = f"<-{expression}"
            if constraints:
                expression += f"{{{constraints}}}"

        return self.fetch(node_dcids=node_dcids, expression=expression)

    def fetch_all_classes(self) -> NodeResponse:
        """Fetches all Classes available in the Data Commons knowledge graph.
>>>>>>> 5172973 (`Node` endpoint and tests)

        Returns:
            NodeResponse: The response object containing all statistical variables.

        Example:
            ```python
            response = node_endpoint.fetch_all_classes()
            print(response.data)
            ```
        """

<<<<<<< HEAD
    return self.fetch_property_values(node_dcids="Class",
                                      expression="typeOf",
                                      out=False)
=======
        return self.fetch_property_values(
            node_dcids="Class", expression="typeOf", out=False
        )
>>>>>>> 5172973 (`Node` endpoint and tests)
