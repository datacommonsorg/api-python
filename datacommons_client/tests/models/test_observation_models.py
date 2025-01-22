from datacommons_client.models.observation import Facet
from datacommons_client.models.observation import Observation
from datacommons_client.models.observation import OrderedFacets
from datacommons_client.models.observation import Variable


def test_observation_from_json():
  """Test the Observation.from_json method."""
  json_data = {"date": "2024-01-01", "value": 123.45}
  observation = Observation.from_json(json_data)
  assert observation.date == "2024-01-01"
  assert observation.value == 123.45
  assert isinstance(observation.value, float)


def test_observation_from_json_partial():
  """Test Observation.from_json with missing data."""
  json_data = {"date": "2024-01-01"}
  observation = Observation.from_json(json_data)
  assert observation.date == "2024-01-01"
  assert observation.value is None


def test_ordered_facets_from_json():
  """Test the OrderedFacets.from_json method."""
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
  ordered_facets = OrderedFacets.from_json(json_data)
  assert ordered_facets.earliestDate == "2023-01-01"
  assert ordered_facets.facetId == "facet123"
  assert ordered_facets.latestDate == "2024-01-01"
  assert ordered_facets.obsCount == 2
  assert len(ordered_facets.observations) == 2
  assert ordered_facets.observations[0].value == 100.0


def test_ordered_facets_from_json_empty_observations():
  """Test OrderedFacets.from_json with empty observations."""
  json_data = {
      "earliestDate": "2023-01-01",
      "facetId": "facet123",
      "latestDate": "2024-01-01",
      "obsCount": 0,
      "observations": [],
  }
  ordered_facets = OrderedFacets.from_json(json_data)
  assert len(ordered_facets.observations) == 0


def test_variable_from_json():
  """Test the Variable.from_json method."""
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
  variable = Variable.from_json(json_data)
  assert "entity1" in variable.byEntity
  facets = variable.byEntity["entity1"]["orderedFacets"]
  assert len(facets) == 1
  assert facets[0].facetId == "facet1"
  assert facets[0].observations[0].value == 50.0


def test_variable_from_json_empty():
  """Test Variable.from_json with empty byEntity."""
  json_data = {"byEntity": {}}
  variable = Variable.from_json(json_data)
  assert len(variable.byEntity) == 0


def test_facet_from_json():
  """Test the Facet.from_json method."""
  json_data = {
      "importName": "Import 1",
      "measurementMethod": "Method A",
      "observationPeriod": "2023",
      "provenanceUrl": "http://example.com",
      "unit": "usd",
  }
  facet = Facet.from_json(json_data)
  assert facet.importName == "Import 1"
  assert facet.measurementMethod == "Method A"
  assert facet.observationPeriod == "2023"
  assert facet.provenanceUrl == "http://example.com"
  assert facet.unit == "usd"


def test_facet_from_json_partial():
  """Test Facet.from_json with missing data."""
  json_data = {"importName": "Import 1", "unit": "GTQ"}
  facet = Facet.from_json(json_data)
  assert facet.importName == "Import 1"
  assert facet.measurementMethod is None
  assert facet.unit == "GTQ"
  assert facet.provenanceUrl is None
