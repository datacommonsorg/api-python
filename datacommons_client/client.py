from typing import Literal, Optional

from datacommons_client.endpoints.base import API
from datacommons_client.endpoints.node import NodeEndpoint
from datacommons_client.endpoints.observation import ObservationEndpoint
from datacommons_client.endpoints.resolve import ResolveEndpoint
from datacommons_client.models.observation import ObservationDate
from datacommons_client.utils.dataframes import add_entity_names_to_observations_dataframe
from datacommons_client.utils.dataframes import add_property_constraints_to_observations_dataframe
from datacommons_client.utils.decorators import requires_pandas
from datacommons_client.utils.error_handling import NoDataForPropertyError

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

  def __init__(self,
               api_key: Optional[str] = None,
               *,
               dc_instance: Optional[str] = "datacommons.org",
               url: Optional[str] = None,
               surface_header_value: Optional[str] = None):
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
    self.api = API(api_key=api_key,
                   dc_instance=dc_instance,
                   url=url,
                   surface_header_value=surface_header_value)

    # Create instances of the endpoints
    self.node = NodeEndpoint(api=self.api)
    self.observation = ObservationEndpoint(api=self.api)
    self.resolve = ResolveEndpoint(api=self.api)

  def _find_filter_facet_ids(
      self,
      fetch_by: Literal["entity", "entity_type"],
      date: ObservationDate | str,
      variable_dcids: str | list[str],
      entity_dcids: Literal["all"] | list[str] = "all",
      entity_type: Optional[str] = None,
      parent_entity: Optional[str] = None,
      property_filters: Optional[dict[str, str | list[str]]] = None,
  ) -> list[str] | None:
    """Finds matching facet IDs for property filters.

        Args:
            fetch_by (Literal["entity", "entity_type"]): Determines whether to fetch by entity or entity type.
            variable_dcids (str | list[str]): The variable DCIDs for which to retrieve facet IDs.
            entity_dcids (Literal["all"] | list[str], optional): The entity DCIDs, or "all" if filtering by entity type.
            entity_type (Optional[str]): The entity type, required if fetching by entity type.
            parent_entity (Optional[str]): The parent entity, used when fetching by entity type.
            property_filters (Optional[dict[str, str | list[str]]): A dictionary of properties to match facets against.

        Returns:
            list[str] | None: A list of matching facet IDs, or None if no filters are applied.
        """

    if not property_filters:
      return None

    if fetch_by == "entity":
      observations = self.observation.fetch_observations_by_entity_dcid(
          date=date,
          entity_dcids=entity_dcids,
          variable_dcids=variable_dcids,
          select=["variable", "entity", "facet"],
      )
    else:
      observations = self.observation.fetch_observations_by_entity_type(
          date=date,
          entity_type=entity_type,
          parent_entity=parent_entity,
          variable_dcids=variable_dcids,
          select=["variable", "entity", "facet"],
      )

    facet_sets = [
        observations.find_matching_facet_id(property_name=p, value=v)
        for p, v in property_filters.items()
    ]

    facet_ids = list({facet for facets in facet_sets for facet in facets})

    return facet_ids

  @requires_pandas
  def observations_dataframe(
      self,
      variable_dcids: str | list[str],
      date: ObservationDate | str,
      entity_dcids: Literal["all"] | list[str] = "all",
      entity_type: Optional[str] = None,
      parent_entity: Optional[str] = None,
      property_filters: Optional[dict[str, str | list[str]]] = None,
      include_constraints_metadata: bool = False,
  ):
    """
        Fetches statistical observations and returns them as a Pandas DataFrame.

        The Observation API fetches statistical observations linked to entities and variables
        at a particular date (e.g., "population of USA in 2020", "GDP of California in 2010").

        Args:
        variable_dcids (str | list[str]): One or more variable DCIDs for the observation.
        date (ObservationDate | str): The date for which observations are requested. It can be
            a specific date, "all" to retrieve all observations, or "latest" to get the most recent observations.
        entity_dcids (Literal["all"] | list[str], optional): The entity DCIDs for which to retrieve data.
            Defaults to "all".
        entity_type (Optional[str]): The type of entities to filter by when `entity_dcids="all"`.
            Required if `entity_dcids="all"`. Defaults to None.
        parent_entity (Optional[str]): The parent entity under which the target entities fall.
            Required if `entity_dcids="all"`. Defaults to None.
        property_filters (Optional[dict[str, str | list[str]]): An optional dictionary used to filter
            the data by using observation properties like `measurementMethod`, `unit`, or `observationPeriod`.
        include_constraints_metadata (bool): If True, includes the dcid and name of any constraint
            properties associated with the variable DCIDs (based on the `constraintProperties` property)
            in the returned DataFrame. Defaults to False.

        Returns:
            pd.DataFrame: A DataFrame containing the requested observations.
        """

    if entity_dcids == "all" and not (entity_type and parent_entity):
      raise ValueError(
          "When 'entity_dcids' is 'all', both 'parent_entity' and 'entity_type' must be specified."
      )

    if entity_dcids != "all" and (entity_type or parent_entity):
      raise ValueError(
          "Specify 'entity_type' and 'parent_entity' only when 'entity_dcids' is 'all'."
      )

    # If property filters are provided, fetch the required facet IDs. Otherwise, set to None.
    facets = self._find_filter_facet_ids(
        fetch_by="entity" if entity_dcids != "all" else "entity_type",
        date=date,
        variable_dcids=variable_dcids,
        entity_dcids=entity_dcids,
        entity_type=entity_type,
        parent_entity=parent_entity,
        property_filters=property_filters,
    )

    if not facets and property_filters:
      raise NoDataForPropertyError

    if entity_dcids == "all":
      observations = self.observation.fetch_observations_by_entity_type(
          date=date,
          parent_entity=parent_entity,
          entity_type=entity_type,
          variable_dcids=variable_dcids,
          filter_facet_ids=facets,
      )
    else:
      observations = self.observation.fetch_observations_by_entity_dcid(
          date=date,
          entity_dcids=entity_dcids,
          variable_dcids=variable_dcids,
          filter_facet_ids=facets,
      )

    # Convert the observations to a DataFrame
    df = pd.DataFrame(observations.to_observation_records().model_dump())

    # Add entity names to the DataFrame
    df = add_entity_names_to_observations_dataframe(
        endpoint=self.node,
        observations_df=df,
        entity_columns=["entity", "variable"],
    )

    if include_constraints_metadata:
      df = add_property_constraints_to_observations_dataframe(
          endpoint=self.node,
          observations_df=df,
      )

    return df
