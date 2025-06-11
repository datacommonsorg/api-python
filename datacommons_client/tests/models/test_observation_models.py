import pytest

from datacommons_client.models.observation import Facet
from datacommons_client.models.observation import Observation
from datacommons_client.models.observation import ObservationSelectList
from datacommons_client.models.observation import OrderedFacet
from datacommons_client.models.observation import Variable
from datacommons_client.utils.error_handling import InvalidObservationSelectError


def test_observation_model_validation():
  """Test that Observation.model_validate parses data correctly."""
  json_data = {"date": "2024-01-01", "value": 123.45}
  observation = Observation.model_validate(json_data)
  assert observation.date == "2024-01-01"
  assert observation.value == 123.45
  assert isinstance(observation.value, float)


def test_observation_model_validation_partial():
  """Test Observation.model_validate with missing data."""
  json_data = {"date": "2024-01-01"}
  observation = Observation.model_validate(json_data)
  assert observation.date == "2024-01-01"
  assert observation.value is None


def test_ordered_facets_model_validation():
  """Test that OrderedFacet.model_validate parses data correctly."""
  json_data = {
      "earliestDate":
          "2023-01-01",
      "facetId":
          "facet123",
      "latestDate":
          "2024-01-01",
      "obsCount":
          2,
      "observations": [
          {
              "date": "2023-01-01",
              "value": 100.0
          },
          {
              "date": "2024-01-01",
              "value": 200.0
          },
      ],
  }
  ordered_facets = OrderedFacet.model_validate(json_data)
  assert ordered_facets.earliestDate == "2023-01-01"
  assert ordered_facets.facetId == "facet123"
  assert ordered_facets.latestDate == "2024-01-01"
  assert ordered_facets.obsCount == 2
  assert len(ordered_facets.observations) == 2
  assert ordered_facets.observations[0].value == 100.0


def test_ordered_facets_model_validation_empty_observations():
  """Test OrderedFacet.model_validate with empty observations."""
  json_data = {
      "earliestDate": "2023-01-01",
      "facetId": "facet123",
      "latestDate": "2024-01-01",
      "obsCount": 0,
      "observations": [],
  }
  ordered_facets = OrderedFacet.model_validate(json_data)
  assert len(ordered_facets.observations) == 0


def test_variable_model_validation():
  """Test that Variable.model_validate parses data correctly."""
  json_data = {
      "byEntity": {
          "entity1": {
              "orderedFacets": [{
                  "earliestDate":
                      "2023-01-01",
                  "facetId":
                      "facet1",
                  "latestDate":
                      "2023-12-31",
                  "obsCount":
                      2,
                  "observations": [
                      {
                          "date": "2023-01-01",
                          "value": 50.0
                      },
                      {
                          "date": "2023-12-31",
                          "value": 75.0
                      },
                  ],
              }]
          }
      }
  }
  variable = Variable.model_validate(json_data)
  assert "entity1" in variable.byEntity
  facets = variable.byEntity["entity1"].orderedFacets
  assert len(facets) == 1
  assert facets[0].facetId == "facet1"
  assert facets[0].observations[0].value == 50.0


def test_variable_model_validation_empty():
  """Test Variable.model_validate with empty byEntity."""
  json_data = {"byEntity": {}}
  variable = Variable.model_validate(json_data)
  assert len(variable.byEntity) == 0


def test_facet_model_validation():
  """Test that Facet.model_validate parses data correctly."""
  json_data = {
      "importName": "Import 1",
      "measurementMethod": "Method A",
      "observationPeriod": "2023",
      "provenanceUrl": "http://example.com",
      "unit": "usd",
  }
  facet = Facet.model_validate(json_data)
  assert facet.importName == "Import 1"
  assert facet.measurementMethod == "Method A"
  assert facet.observationPeriod == "2023"
  assert facet.provenanceUrl == "http://example.com"
  assert facet.unit == "usd"


def test_facet_model_validation_partial():
  """Test Facet.model_validate with missing data."""
  json_data = {"importName": "Import 1", "unit": "GTQ"}
  facet = Facet.model_validate(json_data)
  assert facet.importName == "Import 1"
  assert facet.measurementMethod is None
  assert facet.unit == "GTQ"
  assert facet.provenanceUrl is None


def test_observation_select_list_defaults():
  """ObservationSelectList returns default selects when none provided."""
  osl = ObservationSelectList.model_validate(None)
  assert osl.select == ["date", "variable", "entity", "value"]


def test_observation_select_list_custom():
  """ObservationSelectList accepts custom select lists."""
  osl = ObservationSelectList.model_validate(["variable", "entity", "facet"])
  assert osl.select == ["variable", "entity", "facet"]


def test_observation_select_list_missing_required():
  """Missing required select entries raises InvalidObservationSelectError."""
  with pytest.raises(InvalidObservationSelectError):
    ObservationSelectList.model_validate(["date", "value"])
