from unittest.mock import MagicMock

import pandas as pd

from datacommons_client.endpoints.node import NodeEndpoint
from datacommons_client.models.node import StatVarConstraint
from datacommons_client.models.node import StatVarConstraints
from datacommons_client.utils.dataframes import add_property_constraints_to_observations_dataframe


def test_add_property_constraints_to_observations_dataframe_adds_columns():
  """Adds constraint id and name columns based on statvar metadata."""
  # Input observations
  df = pd.DataFrame([
      {
          "date": "2020",
          "entity": "geo/1",
          "variable": "sv/A",
          "value": 10,
          "unit": "Count",
      },
      {
          "date": "2020",
          "entity": "geo/2",
          "variable": "sv/B",
          "value": 20,
          "unit": "Count",
      },
  ])

  endpoint = MagicMock(spec=NodeEndpoint)

  endpoint.fetch_statvar_constraints.return_value = StatVarConstraints.model_validate(
      {
          "sv/A": [
              StatVarConstraint(
                  constraintId="DevelopmentFinanceScheme",
                  constraintName="Development Finance Scheme",
                  valueId="ODAGrants",
                  valueName="Official Development Assistance Grants",
              ),
              StatVarConstraint(
                  constraintId="DevelopmentFinanceRecipient",
                  constraintName="Development Finance Recipient",
                  valueId="country/GTM",
                  valueName="Guatemala",
              ),
          ],
          "sv/B": [
              StatVarConstraint(
                  constraintId="sex",
                  constraintName="Sex",
                  valueId="Female",
                  valueName="Female",
              )
          ],
      })

  out = add_property_constraints_to_observations_dataframe(endpoint=endpoint,
                                                           observations_df=df)

  # Columns for constraints should be present and filled per variable
  assert "DevelopmentFinanceScheme" in out.columns
  assert "DevelopmentFinanceScheme_name" in out.columns
  assert ("DevelopmentFinanceRecipient" in out.columns and
          "DevelopmentFinanceRecipient_name" in out.columns)
  assert "sex" in out.columns and "sex_name" in out.columns

  # Row-wise checks
  row_a = out[out["variable"] == "sv/A"].iloc[0]
  assert row_a["DevelopmentFinanceScheme"] == "ODAGrants"
  assert row_a[
      "DevelopmentFinanceScheme_name"] == "Official Development Assistance Grants"
  assert row_a["DevelopmentFinanceRecipient"] == "country/GTM"
  assert row_a["DevelopmentFinanceRecipient_name"] == "Guatemala"

  row_b = out[out["variable"] == "sv/B"].iloc[0]
  assert row_b["sex"] == "Female"
  assert row_b["sex_name"] == "Female"


def test_add_property_constraints_to_observations_dataframe_empty():
  """Empty DataFrame returns unchanged."""
  endpoint = MagicMock(spec=NodeEndpoint)
  empty_df = pd.DataFrame([])
  out = add_property_constraints_to_observations_dataframe(
      endpoint=endpoint, observations_df=empty_df)
  assert out.empty
