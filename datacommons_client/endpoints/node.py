from typing import Optional

from datacommons_client.endpoints.base import API
from datacommons_client.endpoints.base import Endpoint
from datacommons_client.endpoints.payloads import NodeRequestPayload
from datacommons_client.endpoints.payloads import normalize_properties_to_string
from datacommons_client.endpoints.response import NodeResponse


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
      expression: str | list[str],
      *,
      all_pages: bool = True,
      next_token: Optional[str] = None,
  ) -> NodeResponse:
    """Fetches properties or arcs for given nodes and properties.

        Args:
            node_dcids (str | List[str]): The DCID(s) of the nodes to query.
            expression (str | List[str]): The property or relation expression(s) to query.
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
    return self.fetch(node_dcids=node_dcids,
                      expression=expression,
                      all_pages=all_pages,
                      next_token=next_token)

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

    return self.fetch(node_dcids=node_dcids,
                      expression=expression,
                      all_pages=all_pages,
                      next_token=next_token)

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

    return self.fetch_property_values(node_dcids="Class",
                                      properties="typeOf",
                                      out=False,
                                      all_pages=all_pages,
                                      next_token=next_token)
