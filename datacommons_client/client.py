from typing import Literal, Optional

from datacommons_client.endpoints.base import API
from datacommons_client.endpoints.node import NodeEndpoint
from datacommons_client.endpoints.observation import ObservationEndpoint
from datacommons_client.endpoints.payloads import ObservationDate
from datacommons_client.endpoints.resolve import ResolveEndpoint
from datacommons_client.utils.decorators import requires_pandas

try:
  import pandas as pd
except ImportError:
  pd = None


class DataCommonsClient:
  """
    A client for interacting with the Data Commons API.

    This class provides convenient access to the V2 Data Commons API endpoints.

    Attributes:
        api (API): An instance of the API class that handles requests.
        node (NodeEndpoint): Provides access to node-related queries, such as fetching property labels
            and values for individual or multiple nodes in the Data Commons knowledge graph.
        observation (ObservationEndpoint): Handles observation-related queries, allowing retrieval of
            statistical observations associated with entities, variables, and dates (e.g., GDP of California in 2010).
        resolve (ResolveEndpoint): Manages resolution queries to find different DCIDs for entities.

    """

  def __init__(
      self,
      api_key: Optional[str] = None,
      *,
      dc_instance: Optional[str] = "datacommons.org",
      url: Optional[str] = None,
  ):
    """
        Initializes the DataCommonsClient.

        Args:
            api_key (Optional[str]): The API key for authentication. Defaults to None. Note that
                custom DC instances do not currently require an API key.
            dc_instance (Optional[str]): The Data Commons instance to use. Defaults to "datacommons.org".
            url (Optional[str]): A custom, fully resolved URL for the Data Commons API. Defaults to None.
        """
    # If a fully resolved URL is provided, and the default dc_instance is used,
    # ignore that default value
    if dc_instance == "datacommons.org" and url:
      dc_instance = None

    # Create an instance of the API class which will be injected to the endpoints
    self.api = API(api_key=api_key, dc_instance=dc_instance, url=url)

    # Create instances of the endpoints
    self.node = NodeEndpoint(api=self.api)
    self.observation = ObservationEndpoint(api=self.api)
    self.resolve = ResolveEndpoint(api=self.api)

  @requires_pandas
  def observations_dataframe(
      self,
      variable_dcids: str | list[str],
      date: ObservationDate | str,
      entity_dcids: Literal["all"] | list[str] = "all",
      entity_type: Optional[str] = None,
      parent_entity: Optional[str] = None,
  ):
    """
        Fetches statistical observations and returns them as a Pandas DataFrame.

        The Observation API fetches statistical observations linked to entities and variables
        at a particular date (e.g., "population of USA in 2020", "GDP of California in 2010").

        Args:
            variable_dcids (str | list[str]): One or more variable DCIDs for the observation.
            date (ObservationDate | str): The date for which observations are requested. It can be
            a specific date, "all" to retrieve all observations, or "latest" to get the most recent observations.
            entity_dcids (Literal["all"] | list[str], optional): The entity DCIDs to retrieve data for.
                Defaults to "all". DCIDs must include their type (e.g "country/GTM" for Guatemala).
            entity_type (Optional[str], optional): The type of entities to filter by when `entity_dcids="all"`.
                Required if `entity_dcids="all"`. Defaults to None.
            parent_entity (Optional[str], optional): The parent entity under which the target entities fall.
                Used only when `entity_dcids="all"`. Defaults to None.

        Returns:
            pd.DataFrame: A DataFrame containing the requested observations.
        """

    if entity_dcids == "all" and not entity_type:
      raise ValueError(
          "When 'entity_dcids' is 'all', 'entity_type' must be specified.")

    if entity_dcids != "all" and (entity_type or parent_entity):
      raise ValueError(
          "Specify 'entity_type' and 'parent_entity' only when 'entity_dcids' is 'all'."
      )

    if entity_dcids == "all":
      observations = self.observation.fetch_observations_by_entity_type(
          variable_dcids=variable_dcids,
          date=date,
          entity_type=entity_type,
          parent_entity=parent_entity,
      )
    else:
      observations = self.observation.fetch_observations_by_entity(
          variable_dcids=variable_dcids, date=date, entity_dcids=entity_dcids)

    return pd.DataFrame(observations.get_observations_as_records())
