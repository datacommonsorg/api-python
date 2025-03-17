import json

from datacommons_client.endpoints.response import DCResponse
from datacommons_client.endpoints.response import NodeResponse
from datacommons_client.endpoints.response import ObservationResponse
from datacommons_client.endpoints.response import ResolveResponse
from datacommons_client.models.observation import Facet
from datacommons_client.models.observation import Observation
from datacommons_client.models.observation import OrderedFacets
from datacommons_client.models.observation import Variable
from datacommons_client.utils.data_processing import extract_observations
from datacommons_client.utils.data_processing import flatten_properties
from datacommons_client.utils.data_processing import unpack_arcs

### ----- Test DCResponse ----- ###


def test_next_token():
  """Test that the next_token property returns the correct value."""
  response = DCResponse(json={"nextToken": "abc123"})
  assert response.next_token == "abc123"


def test_empty_next_token():
  """Test that the next_token property returns None when the key is not present."""
  response = DCResponse(json={})
  assert response.next_token is None


### ----- Test Node Response ----- ###


def test_node_response_from_json():
  """Test that the NodeResponse.from_json method correctly parses JSON data."""

  # Mocking JSON data
  json_data = {
      "data": {
          "geoId/06": {
              "properties": [
                  "affectedPlace",
                  "containedInPlace",
                  "location",
                  "member",
                  "overlapsWith",
              ]
          }
      },
      "nextToken": "token123",
  }

  response = NodeResponse.from_json(json_data)

  assert response.nextToken == "token123"
  assert response.data["geoId/06"].properties == [
      "affectedPlace",
      "containedInPlace",
      "location",
      "member",
      "overlapsWith",
  ]
  assert "geoId/06" in response.data


def test_node_as_dict():
  """Test that the NodeResponse.json property returns the correct dictionary."""
  json_data = {
      "data": {
          "geoId/06": {
              "properties": [
                  "affectedPlace",
                  "containedInPlace",
                  "location",
                  "member",
                  "overlapsWith",
              ]
          }
      },
      "nextToken": "token123",
  }

  response = NodeResponse.from_json(json_data)
  result = response.to_dict()

  assert result == json_data


def test_node_as_dict_exclude_none():
  """Test that the NodeResponse.json property returns the correct dictionary."""
  json_data = {
      "data": {
          "geoId/06": {
              "properties": None
          }
      },
      "nextToken": "token123",
  }

  expected = {
      "data": {
          "geoId/06": {}
      },
      "nextToken": "token123",
  }

  response = NodeResponse.from_json(json_data)
  result = response.to_dict(exclude_none=True)

  assert result == expected


def test_node_as_dict_include_none():
  """Test that the NodeResponse.json property returns the correct dictionary."""
  json_data = {
      "data": {
          "geoId/06": {
              "properties": None
          }
      },
      "nextToken": "token123",
  }

  response = NodeResponse.from_json(json_data)
  result = response.to_dict(exclude_none=False)

  assert result == json_data


def test_flatten_properties():
  """Test that the flatten_properties function correctly flattens the properties."""

  # Mocking node data
  json_data = {
      "data": {
          "geoId/06": {
              "properties": [
                  "affectedPlace",
                  "containedInPlace",
                  "location",
                  "member",
                  "overlapsWith",
              ]
          }
      }
  }

  response = NodeResponse.from_json(json_data)
  result = flatten_properties(response.data)
  function_result = response.get_properties()

  assert result == function_result
  assert "geoId/06" in result
  assert result["geoId/06"] == [
      "affectedPlace",
      "containedInPlace",
      "location",
      "member",
      "overlapsWith",
  ]


def test_flatten_arcs():
  """Test that the flatten_properties function correctly flattens the arcs."""
  json_data = {
      "data": {
          "dc/03lw9rhpendw5": {
              "arcs": {
                  "name": {
                      "nodes": [{
                          "provenanceId": "dc/base/EIA_860",
                          "value": "191 Peachtree Tower",
                      }]
                  }
              }
          }
      }
  }
  response = NodeResponse.from_json(json_data)
  result = flatten_properties(response.data)

  assert "dc/03lw9rhpendw5" in result
  assert result["dc/03lw9rhpendw5"].value == "191 Peachtree Tower"


def test_unpack_arcs_multiple_properties():
  """Test that _unpack_arcs correctly handles multiple properties with nodes."""
  arcs = {
      "prop1": {
          "nodes": ["node1", "node2"]
      },
      "prop2": {
          "nodes": ["node3"]
      },
      "prop3": {
          "nodes": []
      },  # Empty nodes for completeness
  }

  result = unpack_arcs(arcs)

  # Expected output
  expected = {
      "prop1": ["node1", "node2"],
      "prop2": ["node3"],
      "prop3": [],
  }

  assert result == expected


### ----- Test Observation Response ----- ###


def test_get_data_by_entity():
  """Test that get_data_by_entity correctly extracts data grouped by entities."""
  # Mocking ObservationResponse with byVariable data
  mock_data = {
      "variable1":
          Variable(byEntity={
              "entity1": {
                  "orderedFacets": []
              },
              "entity2": {
                  "orderedFacets": []
              },
          }),
      "variable2":
          Variable(byEntity={
              "entity3": {
                  "orderedFacets": []
              },
          }),
  }
  response = ObservationResponse(byVariable=mock_data)

  result = response.get_data_by_entity()

  # Expected output
  expected = {
      "variable1": {
          "entity1": {
              "orderedFacets": []
          },
          "entity2": {
              "orderedFacets": []
          },
      },
      "variable2": {
          "entity3": {
              "orderedFacets": []
          },
      },
  }

  assert result == expected


def test_observation_as_dict():
  """Test that the ObservationResponse.json property returns the correct dictionary."""
  json_data = {
      "byVariable": {
          "var1": {
              "byEntity": {
                  "entity1": {
                      "orderedFacets": [{
                          "facetId": "facet1"
                      }]
                  }
              }
          }
      },
      "facets": {
          "facet1": {
              "unit": "GTQ",
              "importName": "Import Name",
          }
      },
  }

  # Parsing JSON data
  response = ObservationResponse.from_json(json_data)

  # Getting it back as a dictionary
  result = response.to_dict()

  assert "byVariable" in result
  assert "facets" in result
  assert "var1" in result["byVariable"]
  assert "entity1" in result["byVariable"]["var1"]["byEntity"]


def test_observation_as_dict_exclude_none():
  """Test that the ObservationResponse.json property returns the correct dictionary."""
  json_data = {
      "byVariable": {
          "var1": {
              "byEntity": {
                  "entity1": {
                      "orderedFacets": [{
                          "facetId": "facet1"
                      }]
                  }
              }
          }
      },
      "facets": {
          "facet1": {
              "unit": "GTQ",
              "importName": "Import Name",
          }
      },
  }

  # Parsing JSON data
  response = ObservationResponse.from_json(json_data)

  # Getting it back as a dictionary
  result = response.to_dict(exclude_none=True)

  assert ("latestDate" not in result["byVariable"]["var1"]["byEntity"]
          ["entity1"]["orderedFacets"][0])


def test_observation_response_from_json():
  """Test that the ObservationResponse.from_json method parses JSON correctly."""
  json_data = {
      "byVariable": {
          "var1": {
              "byEntity": {
                  "entity1": {
                      "orderedFacets": [{
                          "facetId": "facet1"
                      }]
                  }
              }
          }
      },
      "facets": {
          "facet1": {
              "unit": "GTQ",
              "importName": "Import Name",
          }
      },
  }

  # Parsing JSON data
  response = ObservationResponse.from_json(json_data)

  assert "var1" in response.byVariable
  assert "entity1" in response.byVariable["var1"].byEntity
  assert "facet1" in response.facets
  assert response.facets["facet1"].unit == "GTQ"


def test_get_data_by_entity_from_method():
  """Test that get_data_by_entity correctly extracts data grouped by entities."""
  # Mocking ObservationResponse with byVariable data
  mock_data = {
      "variable1":
          Variable(byEntity={
              "entity1": {
                  "orderedFacets": []
              },
              "entity2": {
                  "orderedFacets": []
              },
          }),
      "variable2":
          Variable(byEntity={
              "entity3": {
                  "orderedFacets": []
              },
          }),
  }
  response = ObservationResponse(byVariable=mock_data)

  result = response.get_data_by_entity()

  # Expected output
  expected = {
      "variable1": {
          "entity1": {
              "orderedFacets": []
          },
          "entity2": {
              "orderedFacets": []
          },
      },
      "variable2": {
          "entity3": {
              "orderedFacets": []
          },
      },
  }

  assert result == expected


def test_extract_observations():
  """Test that extract_observations correctly processes observations with typed inputs."""
  variable = "var1"
  entity = "entity1"

  # Mocking OrderedFacets and Observations
  entity_data = {
      "orderedFacets": [
          OrderedFacets(
              facetId="facet1",
              earliestDate="2023-01-01",
              latestDate="2023-01-31",
              obsCount=2,
              observations=[
                  Observation(date="2023-01-01", value=10.0),
                  Observation(date="2023-01-15", value=15.0),
              ],
          )
      ]
  }

  # Mocking facet metadata
  facet_metadata = {
      "facet1":
          Facet(
              importName="Example Import",
              measurementMethod="Example Method",
              observationPeriod="P1D",
              provenanceUrl="http://example.com",
              unit="Example Unit",
          )
  }

  # Extracting observations
  result = extract_observations(variable, entity, entity_data, facet_metadata)

  # Assertions
  assert len(result) == 2, "There should be two observation records."
  assert result[0]["date"] == "2023-01-01"
  assert result[0]["value"] == 10.0
  assert result[0]["facetId"] == "facet1"
  assert result[0]["importName"] == "Example Import"
  assert result[1]["date"] == "2023-01-15"
  assert result[1]["value"] == 15.0


def test_get_observations_as_records():
  """Test that get_observations_as_records correctly converts data into records."""
  # Minimal input setup for byVariable and facets
  mock_data = {
      "variable1":
          Variable(
              byEntity={
                  "entity1": {
                      "orderedFacets": [
                          OrderedFacets(
                              facetId="facet1",
                              observations=[
                                  Observation(date="2023-01-01", value=10.0),
                                  Observation(date="2023-01-15", value=15.0),
                              ],
                          )
                      ]
                  }
              })
  }

  mock_facets = {"facet1": Facet(importName="ImportName",)}

  # Create an ObservationResponse instance
  response = ObservationResponse(byVariable=mock_data, facets=mock_facets)

  # Call the method and get the result
  result = response.get_observations_as_records()

  # Expected output
  expected = [
      {
          "date": "2023-01-01",
          "entity": "entity1",
          "variable": "variable1",
          "value": 10.0,
          "facetId": "facet1",
          "importName": "ImportName",
          "measurementMethod": "",
          "observationPeriod": "",
          "provenanceUrl": "",
          "unit": "",
      },
      {
          "date": "2023-01-15",
          "entity": "entity1",
          "variable": "variable1",
          "value": 15.0,
          "facetId": "facet1",
          "importName": "ImportName",
          "measurementMethod": "",
          "observationPeriod": "",
          "provenanceUrl": "",
          "unit": "",
      },
  ]

  # Assert the results
  assert result == expected


### ----- Test Resolve Response ----- ###


def test_resolve_response_from_json():
  """Test that ResolveResponse.from_json correctly parses entities and candidates."""
  # Mock input JSON
  json_data = {
      "entities": [
          {
              "node":
                  "entity1",
              "candidates": [
                  {
                      "dcid": "dcid1",
                      "dominantType": "Type1"
                  },
                  {
                      "dcid": "dcid2",
                      "dominantType": None
                  },
              ],
          },
          {
              "node": "entity2",
              "candidates": [{
                  "dcid": "dcid3",
                  "dominantType": "Type2"
              },],
          },
      ]
  }

  # Parse the response
  response = ResolveResponse.from_json(json_data)

  # Assert the number of entities
  assert len(response.entities) == 2

  # Validate the first entity
  entity1 = response.entities[0]
  assert entity1.node == "entity1"
  assert len(entity1.candidates) == 2
  assert entity1.candidates[0].dcid == "dcid1"
  assert entity1.candidates[0].dominantType == "Type1"
  assert entity1.candidates[1].dcid == "dcid2"
  assert entity1.candidates[1].dominantType is None

  # Validate the second entity
  entity2 = response.entities[1]
  assert entity2.node == "entity2"
  assert len(entity2.candidates) == 1
  assert entity2.candidates[0].dcid == "dcid3"
  assert entity2.candidates[0].dominantType == "Type2"


def test_resolve_response_dict():
  """Test that ResolveResponse.to_dict and json are consistent."""
  # Input dictionary
  input_data = {
      "entities": [
          {
              "node":
                  "entity1",
              "candidates": [
                  {
                      "dcid": "dcid1",
                      "dominantType": "Type1"
                  },
                  {
                      "dcid": "dcid2",
                      "dominantType": None
                  },
              ],
          },
          {
              "node": "entity2",
              "candidates": [{
                  "dcid": "dcid3",
                  "dominantType": "Type2"
              },],
          },
      ]
  }

  # Create ResolveResponse from the dictionary
  response = ResolveResponse.from_json(input_data)

  # Convert back to dictionary using the json property
  result = response.to_dict(exclude_none=False)

  # Assert that the resulting dictionary matches the original input
  assert result == input_data


def test_resolve_response_dict_exclude_none():
  """Test that ResolveResponse.to_dict and json are consistent."""
  # Input dictionary
  input_data = {
      "entities": [{
          "node":
              "entity1",
          "candidates": [
              {
                  "dcid": "dcid1",
                  "dominantType": "Type1"
              },
              {
                  "dcid": "dcid2",
                  "dominantType": None
              },
          ],
      }, {
          "node": "entity2",
          "candidates": [{
              "dcid": "dcid3",
              "dominantType": "Type2"
          },],
      }, {
          "node": "entity3",
          "candidates": [],
      }]
  }

  # Expected data
  expected_data = {
      "entities": [{
          "node":
              "entity1",
          "candidates": [
              {
                  "dcid": "dcid1",
                  "dominantType": "Type1"
              },
              {
                  "dcid": "dcid2"
              },
          ],
      }, {
          "node": "entity2",
          "candidates": [{
              "dcid": "dcid3",
              "dominantType": "Type2"
          },],
      }, {
          "node": "entity3",
          "candidates": [],
      }]
  }

  # Create ResolveResponse from the dictionary
  response = ResolveResponse.from_json(input_data)

  # Convert back to dictionary using the json property
  result = response.to_dict(exclude_none=True)

  # Assert that the resulting dictionary matches the original input
  assert result == expected_data


def test_resolve_response_json_string_exclude_none():
  """Test that ResolveResponse.to_dict and json are consistent."""
  # Input dictionary
  input_data = {
      "entities": [
          {
              "node":
                  "entity1",
              "candidates": [
                  {
                      "dcid": "dcid1",
                      "dominantType": "Type1"
                  },
                  {
                      "dcid": "dcid2",
                      "dominantType": None
                  },
              ],
          },
          {
              "node": "entity2",
              "candidates": [{
                  "dcid": "dcid3",
                  "dominantType": "Type2"
              },],
          },
          {
              "node": "entity3",
              "candidates": [],
          },
      ]
  }

  # Expected data
  expected = json.dumps(input_data, indent=2)

  # Create ResolveResponse from the dictionary
  response = ResolveResponse.from_json(input_data)

  # Convert back to dictionary using the json property
  result = response.to_json(exclude_none=False)

  # Assert that the resulting dictionary matches the original input
  assert result == expected
