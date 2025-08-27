from datacommons_client.endpoints.node import NodeEndpoint
from datacommons_client.utils.data_processing import flatten_names_dictionary

try:
  import pandas as pd
except ImportError:
  pd = None

from datacommons_client.utils.decorators import requires_pandas


@requires_pandas
def add_entity_names_to_observations_dataframe(
    endpoint: NodeEndpoint,
    observations_df: "pd.DataFrame",  # type: ignore[reportInvalidTypeForm]
    entity_columns: str | list[str],
) -> "pd.DataFrame":  # type: ignore[reportInvalidTypeForm]
  """
    Adds entity names to the observations DataFrame.

    Args:
        endpoint (NodeEndpoint): The NodeEndpoint instance for fetching entity names.
        observations_df (dict): The DataFrame containing observations.
        entity_columns (str | list[str]): The column(s) containing entity DCIDs.
    """

  # Guard against empty DataFrame
  if observations_df.empty:
    return observations_df

  if not isinstance(entity_columns, list):
    entity_columns = [entity_columns]

  for entity_column in entity_columns:
    if entity_column not in observations_df.columns:
      raise ValueError(
          "The specified entity column does not exist in the DataFrame.")

    # Get unique entity DCIDs from the DataFrame
    unique_values = observations_df[entity_column].dropna().unique().tolist()

    # Guard against empty unique values
    if not unique_values:
      continue

    # Fetch entity names from the endpoint
    response = endpoint.fetch_entity_names(entity_dcids=unique_values)

    # Flatten the response to get a dictionary of names
    names = flatten_names_dictionary(response)

    # Insert the names into a column next to the entity column
    name_column = f"{entity_column}_name"
    if name_column not in observations_df.columns:
      observations_df.insert(
          loc=observations_df.columns.get_loc(entity_column) + 1,
          column=name_column,
          value=observations_df[entity_column].map(names),
      )

  return observations_df


@requires_pandas
def add_property_constraints_to_observations_dataframe(
    endpoint: NodeEndpoint,
    observations_df: "pd.DataFrame",  # type: ignore[reportInvalidTypeForm]
) -> "pd.DataFrame":  # type: ignore[reportInvalidTypeForm]
  """
    Adds property constraint dcids and names to the observations DataFrame.

    Args:
        endpoint (NodeEndpoint): The NodeEndpoint instance for fetching entity names.
        observations_df (dict): The DataFrame containing observations.
    """

  # Guard against empty DataFrame
  if observations_df.empty:
    return observations_df

  # Get constraints
  constraints_data = endpoint.fetch_statvar_constraints(
      variable_dcids=observations_df.variable.unique().tolist())

  for statvar, constraints in constraints_data.items():
    for constraint in constraints:
      # Fill the columns with the corresponding values
      observations_df.loc[observations_df.variable == statvar,
                          constraint.constraintId] = constraint.valueId

      observations_df.loc[observations_df.variable == statvar,
                          constraint.constraintId +
                          "_name"] = constraint.valueName

  return observations_df
