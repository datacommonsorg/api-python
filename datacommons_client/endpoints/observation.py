from typing import Optional

from datacommons_client.endpoints.base import API
from datacommons_client.endpoints.base import Endpoint
from datacommons_client.endpoints.payloads import ObservationRequestPayload
from datacommons_client.endpoints.response import ObservationResponse
from datacommons_client.models.observation import ObservationDate
from datacommons_client.models.observation import ObservationSelect
from datacommons_client.utils.data_processing import group_variables_by_entity


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
    ).to_dict()

    response = self.post(payload)

    # Send the request
    return ObservationResponse.model_validate(response)

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

  def fetch_observations_by_entity_dcid(
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
            ObservationEndpoint(api).fetch_observations_by_entity_dcid(
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

  def fetch_available_statistical_variables(
      self,
      entity_dcids: str | list[str],
  ) -> dict[str, list[str]]:
    """
        Fetches available statistical variables (which have observations) for given entities.
        Args:
            entity_dcids (str | list[str]): One or more entity DCIDs(s) to fetch variables for.
        Returns:
            dict[str, list[str]]: A dictionary mapping entity DCIDs to their available statistical variables.
        """

    # Fetch observations for the given entity DCIDs. If variable is empty list
    # all available variables are retrieved.
    data = self.fetch(
        entity_dcids=entity_dcids,
        select=[ObservationSelect.VARIABLE, ObservationSelect.ENTITY],
        variable_dcids=[],
    ).get_data_by_entity()

    return group_variables_by_entity(data=data)
