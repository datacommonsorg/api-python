from typing import Optional

from datacommons_client.endpoints.base import API
from datacommons_client.endpoints.base import Endpoint
from datacommons_client.endpoints.payloads import ObservationDate
from datacommons_client.endpoints.payloads import ObservationRequestPayload
from datacommons_client.endpoints.payloads import ObservationSelect
from datacommons_client.endpoints.response import ObservationResponse


class ObservationEndpoint(Endpoint):
  """
    A class to interact with the observation API endpoint.

    Args:
        api (API): The API instance providing the environment configuration
            (base URL, headers, authentication) to be used for requests.
    """

  def __init__(self, api: API):
    """Initializes the ObservationEndpoint instance."""
    super().__init__(endpoint="observation", api=api)

  def fetch(
      self,
      variable_dcids: str | list[str],
      date: ObservationDate | str = ObservationDate.LATEST,
      select: Optional[list[ObservationSelect | str]] = None,
      entity_dcids: Optional[str | list[str]] = None,
      entity_expression: Optional[str] = None,
      filter_facet_domains: Optional[str | list[str]] = None,
      filter_facet_ids: Optional[str | list[str]] = None,
  ) -> ObservationResponse:
    """
        Fetches data from the observation endpoint.

        Args:
            variable_dcids (str | list[str]): One or more variable IDs for the data.
            date (str | ObservationDate): The date for which data is being requested.
                Defaults to the latest observation.
            select (list[ObservationSelect]): Fields to include in the response.
                Defaults to ["date", "variable", "entity", "value"].
            entity_dcids (Optional[str | list[str]]): One or more entity IDs to filter the data.
            entity_expression (Optional[str]): A string expression to filter entities.
            filter_facet_domains (Optional[str | list[str]]): One or more domain names to filter the data.
            filter_facet_ids (Optional[str | list[str]]): One or more facet IDs to filter the data.

        Returns:
            ObservationResponse: The response object containing observations for the specified query.
        """
    # Construct the payload
    payload = ObservationRequestPayload(
        date=date,
        variable_dcids=variable_dcids,
        select=select,
        entity_dcids=entity_dcids,
        entity_expression=entity_expression,
        filter_facet_domains=filter_facet_domains,
        filter_facet_ids=filter_facet_ids,
    ).to_dict

    # Send the request
    return ObservationResponse.from_json(self.post(payload))

  def fetch_latest_observations(
      self,
      variable_dcids: str | list[str],
      entity_dcids: Optional[str | list[str]] = None,
      entity_expression: Optional[str] = None,
      *,
      select: Optional[list[ObservationSelect | str]] = None,
      filter_facet_domains: Optional[str | list[str]] = None,
      filter_facet_ids: Optional[str | list[str]] = None,
  ) -> ObservationResponse:
    """
        Fetches the latest observations for the given variable and entity.

        Args:
            variable_dcids (str | list[str]): One or more variable IDs for the data.
            entity_dcids (Optional[str | list[str]]): One or more entity IDs to filter the data.
            entity_expression (Optional[str]): A string expression to filter entities.
            select (Optional[list[ObservationSelect | str]]): Fields to include in the response.
                If not provided, defaults to ["date", "variable", "entity", "value"].
            filter_facet_domains: Optional[str | list[str]: One or more domain names to filter the data.
            filter_facet_ids: Optional[str | list[str]: One or more facet IDs to filter the data.

        Returns:
            ObservationResponse: The response object containing observations for the specified query.
        """
    return self.fetch(
        variable_dcids=variable_dcids,
        date=ObservationDate.LATEST,
        entity_dcids=entity_dcids,
        entity_expression=entity_expression,
        filter_facet_domains=filter_facet_domains,
        filter_facet_ids=filter_facet_ids,
        select=[s for s in ObservationSelect] if not select else select,
    )

  def fetch_latest_observations_by_entity(
      self,
      variable_dcids: str | list[str],
      entity_dcids: str | list[str],
      *,
      select: Optional[list[ObservationSelect | str]] = None,
      filter_facet_domains: Optional[str | list[str]] = None,
      filter_facet_ids: Optional[str | list[str]] = None,
  ) -> ObservationResponse:
    """Fetches the latest observations for the given variable and entities.

        Args:
            variable_dcids (str | list[str]): One or more variable IDs for the data.
            entity_dcids (str | list[str]): One or more entity IDs to filter the data.
            select (Optional[list[ObservationSelect | str]]): Fields to include in the response.
                If not provided, defaults to ["date", "variable", "entity", "value"].
            filter_facet_domains: Optional[str | list[str]: One or more domain names to filter the data.
            filter_facet_ids: Optional[str | list[str]: One or more facet IDs to filter the data.

        Returns:
            ObservationResponse: The response object containing observations for the specified query.
        """

    return self.fetch_latest_observations(
        variable_dcids=variable_dcids,
        entity_dcids=entity_dcids,
        select=[s for s in ObservationSelect] if not select else select,
        filter_facet_domains=filter_facet_domains,
        filter_facet_ids=filter_facet_ids,
    )

  def fetch_observations_by_entity_type(
      self,
      date: ObservationDate | str,
      parent_entity: str,
      entity_type: str,
      variable_dcids: str | list[str],
      *,
      select: Optional[list[ObservationSelect | str]] = None,
      filter_facet_domains: Optional[str | list[str]] = None,
      filter_facet_ids: Optional[str | list[str]] = None,
  ) -> ObservationResponse:
    """
        Fetches all observations for a given entity type.

        Args:
            date (ObservationDate | str): The date option for the observations.
                Use 'all' for all dates, 'latest' for the most recent data,
                or provide a date as a string (e.g., "2024").
            parent_entity (str): The parent entity under which the target entities fall.
                For example, "africa" for African countries, or "Earth" for all countries.
            entity_type (str): The type of entities for which to fetch observations.
                For example, "Country" or "Region".
            variable_dcids (str | list[str]): The variable(s) to fetch observations for.
                This can be a single variable ID or a list of IDs.
            select (Optional[list[ObservationSelect | str]]): Fields to include in the response.
                If not provided, defaults to ["date", "variable", "entity", "value"].
            filter_facet_domains: Optional[str | list[str]: One or more domain names to filter the data.
            filter_facet_ids: Optional[str | list[str]: One or more facet IDs to filter the data.

        Returns:
            ObservationResponse: The response object containing observations for the specified entity type.

        Example:
            To fetch all observations for African countries for a specific variable:

            ```python
            api = API()
            ObservationEndpoint(api).fetch_observations_by_entity_type(
                date="all",
                parent_entity="africa",
                entity_type="Country",
                variable_dcids="sdg/SI_POV_DAY1"
            )
            ```
        """

    return self.fetch(
        variable_dcids=variable_dcids,
        date=date,
        select=[s for s in ObservationSelect] if not select else select,
        entity_expression=
        f"{parent_entity}<-containedInPlace+{{typeOf:{entity_type}}}",
        filter_facet_domains=filter_facet_domains,
        filter_facet_ids=filter_facet_ids,
    )

  def fetch_observations_by_entity(
      self,
      date: ObservationDate | str,
      entity_dcids: str | list[str],
      variable_dcids: str | list[str],
      *,
      select: Optional[list[ObservationSelect | str]] = None,
      filter_facet_domains: Optional[str | list[str]] = None,
      filter_facet_ids: Optional[str | list[str]] = None,
  ) -> ObservationResponse:
    """
        Fetches all observations for a given entity type.

        Args:
            date (ObservationDate | str): The date option for the observations.
                Use 'all' for all dates, 'latest' for the most recent data,
                or provide a date as a string (e.g., "2024").
            entity_dcids (str | list[str]): One or more entity IDs to filter the data.
            variable_dcids (str | list[str]): The variable(s) to fetch observations for.
                This can be a single variable ID or a list of IDs.
            select (Optional[list[ObservationSelect | str]]): Fields to include in the response.
                If not provided, defaults to ["date", "variable", "entity", "value"].
            filter_facet_domains: Optional[str | list[str]: One or more domain names to filter the data.
            filter_facet_ids: Optional[str | list[str]: One or more facet IDs to filter the data.

        Returns:
            ObservationResponse: The response object containing observations for the specified entity type.

        Example:
            To fetch all observations for Nigeria for a specific variable:

            ```python
            api = API()
            ObservationEndpoint(api).fetch_observations_by_entity(
                date="all",
                entity_dcids="country/NGA",
                variable_dcids="sdg/SI_POV_DAY1"
            )
            ```
        """

    return self.fetch(
        variable_dcids=variable_dcids,
        date=date,
        select=[s for s in ObservationSelect] if not select else select,
        entity_dcids=entity_dcids,
        filter_facet_domains=filter_facet_domains,
        filter_facet_ids=filter_facet_ids,
    )
