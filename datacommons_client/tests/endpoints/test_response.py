import json

from datacommons_client.endpoints.response import NodeResponse
from datacommons_client.endpoints.response import ObservationResponse
from datacommons_client.endpoints.response import ResolveResponse
from datacommons_client.models.node import Node
from datacommons_client.models.node import NodeGroup
from datacommons_client.models.observation import ByVariable
from datacommons_client.models.observation import Facet
from datacommons_client.models.observation import Observation
from datacommons_client.models.observation import OrderedFacet
from datacommons_client.models.observation import OrderedFacets
from datacommons_client.models.observation import Variable
from datacommons_client.utils.data_processing import extract_observations
from datacommons_client.utils.data_processing import flatten_properties
from datacommons_client.utils.data_processing import unpack_arcs

### ----- Test Node Response ----- ###


def test_node_response_model_validation():
  """Test that NodeResponse.model_validate correctly parses JSON data."""

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

  response = NodeResponse.model_validate(json_data)

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

  response = NodeResponse.model_validate(json_data)
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

  response = NodeResponse.model_validate(json_data)
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

  response = NodeResponse.model_validate(json_data)
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

  response = NodeResponse.model_validate(json_data)
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
  response = NodeResponse.model_validate(json_data)
  result = flatten_properties(response.data)

  assert "dc/03lw9rhpendw5" in result
  assert result["dc/03lw9rhpendw5"] == {
      "name": [
          Node(value="191 Peachtree Tower", provenanceId="dc/base/EIA_860")
      ]
  }


def test_flatten_multiple_arcs_with_multiple_nodes():
  """Test that the flatten_properties function correctly flattens the
  NodeResponse containing multiple property arcs and mutliple nodes within the
  arcs."""

  # Mocking node data
  json_data = {
      "data": {
          "geoId/06": {
              "arcs": {
                  "containedInPlace": {
                      "nodes": [{
                          "dcid": "country/USA",
                          "name": "United States",
                          "provenanceId": "dc/base/WikidataOtherIdGeos",
                          "types": ["Country"]
                      }, {
                          "dcid": "usc/PacificDivision",
                          "name": "Pacific Division",
                          "provenanceId": "dc/base/WikidataOtherIdGeos",
                          "types": ["CensusDivision"]
                      }]
                  },
                  "name": {
                      "nodes": [{
                          "provenanceId": "dc/base/WikidataOtherIdGeos",
                          "value": "California"
                      }]
                  }
              }
          }
      }
  }

  response = NodeResponse.model_validate(json_data)
  result = flatten_properties(response.data)
  function_result = response.get_properties()

  assert result == function_result
  assert "geoId/06" in result
  assert result["geoId/06"] == {
      "containedInPlace": [
          Node(dcid='country/USA',
               name='United States',
               provenanceId='dc/base/WikidataOtherIdGeos',
               types=['Country']),
          Node(dcid='usc/PacificDivision',
               name='Pacific Division',
               provenanceId='dc/base/WikidataOtherIdGeos',
               types=['CensusDivision'])
      ],
      "name": [
          Node(
              value='California',
              provenanceId='dc/base/WikidataOtherIdGeos',
          )
      ],
  }


def test_unpack_arcs_missing_nodes_key():
  """Test that unpack_arcs handles arcs with no 'nodes' key."""
  arcs = {
      "prop1": NodeGroup(nodes=[Node(
          dcid='node1'), Node(dcid='node2')]),
      "prop2": NodeGroup(),  # No 'nodes' key here
      "prop3": NodeGroup(nodes=[]),
  }

  result = unpack_arcs(arcs)
  assert result == {
      "prop1": [Node(dcid='node1'), Node(dcid='node2')],
      "prop2": [],
      "prop3": [],
  }


def test_unpack_arcs_multiple_properties():
  """Test that _unpack_arcs correctly handles multiple properties with nodes."""
  arcs = {
      "prop1": NodeGroup(nodes=[Node(
          dcid='node1'), Node(dcid='node2')]),
      "prop2": NodeGroup(nodes=[Node(dcid='node3')]),
      "prop3": NodeGroup(nodes=[]),  # Empty nodes for completeness
  }

  result = unpack_arcs(arcs)

  # Expected output
  expected = {
      "prop1": [Node(dcid='node1'), Node(dcid='node2')],
      "prop2": [Node(dcid='node3')],
      "prop3": [],
  }

  assert result == expected


def test_extract_connected_dcids():
  """Test that extract_connected_dcids is successful when multiple dcid and multiple
  properties are in the response."""
  json_data = {
      "data": {
          "geoId/06": {
              "arcs": {
                  "containedInPlace": {
                      "nodes": [{
                          "dcid": "country/USA",
                          "name": "United States",
                          "provenanceId": "dc/base/WikidataOtherIdGeos",
                          "types": ["Country"]
                      }, {
                          "dcid": "usc/PacificDivision",
                          "name": "Pacific Division",
                          "provenanceId": "dc/base/WikidataOtherIdGeos",
                          "types": ["CensusDivision"]
                      }]
                  },
                  "name": {
                      "nodes": [{
                          "provenanceId": "dc/base/WikidataOtherIdGeos",
                          "value": "California"
                      }]
                  }
              }
          },
          "geoId/07": {
              "arcs": {
                  "containedInPlace": {
                      "nodes": [{
                          "dcid": "country/USA",
                          "name": "United States",
                          "provenanceId": "dc/base/WikidataOtherIdGeos",
                          "types": ["Country"]
                      }]
                  },
              }
          }
      }
  }
  response = NodeResponse.model_validate(json_data)
  result = response.extract_connected_dcids(subject_dcid='geoId/06',
                                            property_dcid='containedInPlace')
  assert result == ['country/USA', 'usc/PacificDivision']


def test_extract_connected_dcids_with_nonexistent_dcid():
  """Test that extract_connected_dcids returns empty when requested dcid is not in the
  NodeResponse."""
  json_data = {
      "data": {
          "geoId/06": {
              "arcs": {
                  "name": {
                      "nodes": [{
                          "provenanceId": "dc/base/WikidataOtherIdGeos",
                          "value": "California"
                      }]
                  }
              }
          },
      }
  }
  response = NodeResponse.model_validate(json_data)
  result = response.extract_connected_dcids(subject_dcid='geoId/07',
                                            property_dcid='name')
  assert result == []


def test_extract_connected_dcids_with_nonexistent_property():
  """Test that extract_connected_dcids returns empty when requested property is not in
  the NodeResponse."""
  json_data = {
      "data": {
          "geoId/06": {
              "arcs": {
                  "name": {
                      "nodes": [{
                          "provenanceId": "dc/base/WikidataOtherIdGeos",
                          "value": "California"
                      }]
                  }
              }
          },
      }
  }
  response = NodeResponse.model_validate(json_data)
  result = response.extract_connected_dcids(subject_dcid='geoId/06',
                                            property_dcid='containedInPlace')
  assert result == []


def test_extract_connected_dcids_does_not_include_none_for_value_only_nodes():
  """Test that extract_connected_dcids does not include None in the returned list
  when the nodes in the response only contain values and not dcids."""
  json_data = {
      "data": {
          "geoId/06": {
              "arcs": {
                  "name": {
                      "nodes": [{
                          "provenanceId": "dc/base/WikidataOtherIdGeos",
                          "value": "California"
                      }]
                  }
              }
          },
      }
  }
  response = NodeResponse.model_validate(json_data)
  result = response.extract_connected_dcids(subject_dcid='geoId/06',
                                            property_dcid='name')
  assert result == []


def test_extract_connected_dcids_with_node_type_filter():
  """Test that extract_connected_dcids returns dcids with the corresponding
  node_type."""

  json_data = {
      "data": {
          "geoId/06": {
              "arcs": {
                  "relatedPlaces": {
                      "nodes": [{
                          "dcid": "country/USA",
                          "name": "United States",
                          "provenanceId": "dc/base/WikidataOtherIdGeos",
                          "types": ["Country"]
                      }, {
                          "dcid": "usc/PacificDivision",
                          "name": "Pacific Division",
                          "provenanceId": "dc/base/WikidataOtherIdGeos",
                          "types": ["CensusDivision"]
                      }, {
                          "dcid": "node3",
                      }]
                  }
              }
          },
      }
  }
  response = NodeResponse.model_validate(json_data)
  result = response.extract_connected_dcids(subject_dcid='geoId/06',
                                            property_dcid='relatedPlaces',
                                            connected_node_types="Country")
  assert result == ['country/USA']


def test_extract_connected_dcids_with_multiple_node_type_filter():
  """Test that extract_connected_dcids returns dcids with the corresponding
  connected_node_types."""
  json_data = {
      "data": {
          "geoId/06": {
              "arcs": {
                  "relatedPlaces": {
                      "nodes": [{
                          "dcid": "country/USA",
                          "name": "United States",
                          "provenanceId": "dc/base/WikidataOtherIdGeos",
                          "types": ["Country"]
                      }, {
                          "dcid": "usc/PacificDivision",
                          "name": "Pacific Division",
                          "provenanceId": "dc/base/WikidataOtherIdGeos",
                          "types": ["CensusDivision"]
                      }, {
                          "dcid": "node3",
                          "types": ["City"]
                      }]
                  }
              }
          },
      }
  }
  response = NodeResponse.model_validate(json_data)
  result = response.extract_connected_dcids(
      subject_dcid='geoId/06',
      property_dcid='relatedPlaces',
      connected_node_types=["Country", "City"])
  assert result == ['country/USA', 'node3']


def test_extract_connected_nodes_with_multiple_node_type_filter():
  """Test that extract_connected_nodes returns only nodes with the corresponding
  connected_node_types."""

  json_data = {
      "data": {
          "geoId/06": {
              "arcs": {
                  "relatedPlaces": {
                      "nodes": [{
                          "dcid": "country/USA",
                          "name": "United States",
                          "provenanceId": "dc/base/WikidataOtherIdGeos",
                          "types": ["Country"]
                      }, {
                          "dcid": "usc/PacificDivision",
                          "name": "Pacific Division",
                          "provenanceId": "dc/base/WikidataOtherIdGeos",
                          "types": ["CensusDivision"]
                      }, {
                          "dcid": "node3",
                          "types": ["City"]
                      }]
                  }
              }
          },
      }
  }
  response = NodeResponse.model_validate(json_data)
  result = response.extract_connected_nodes(
      subject_dcid='geoId/06',
      property_dcid='relatedPlaces',
      connected_node_types=["Country", "City"])
  assert result == [
      Node(dcid="country/USA",
           name="United States",
           provenanceId="dc/base/WikidataOtherIdGeos",
           types=["Country"]),
      Node(dcid="node3", types=["City"])
  ]


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
  response = ObservationResponse.model_validate({"byVariable": mock_data})

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

  assert result.to_dict() == expected


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
  response = ObservationResponse.model_validate(json_data)

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
  response = ObservationResponse.model_validate(json_data)

  # Getting it back as a dictionary
  result = response.to_dict(exclude_none=True)

  assert ("latestDate" not in result["byVariable"]["var1"]["byEntity"]
          ["entity1"]["orderedFacets"][0])


def test_observation_response_model_validation():
  """Test that ObservationResponse.model_validate parses JSON correctly."""
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
  response = ObservationResponse.model_validate(json_data)

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
  response = ObservationResponse.model_validate({"byVariable": mock_data})

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

  assert result.to_dict() == expected


def test_extract_observations():
  """Test that extract_observations correctly processes observations with typed inputs."""
  variable = "var1"
  entity = "entity1"

  # Mocking OrderedFacet and Observations
  entity_data = OrderedFacets.model_validate({
      "orderedFacets": [
          OrderedFacet(
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
  })

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
  result = extract_observations(variable=variable,
                                entity=entity,
                                entity_data=entity_data,
                                facet_metadata=facet_metadata)

  # Assertions
  assert len(result) == 2, "There should be two observation records."
  assert result[0].date == "2023-01-01"
  assert result[0].value == 10.0
  assert result[0].facetId == "facet1"
  assert result[0].importName == "Example Import"
  assert result[1].date == "2023-01-15"
  assert result[1].value == 15.0


def test_get_observations_as_records():
  """Test that get_observations_as_records correctly converts data into records."""
  # Minimal input setup for byVariable and facets
  mock_data = ByVariable.model_validate({
      "variable1":
          Variable(
              byEntity={
                  "entity1": {
                      "orderedFacets": [
                          OrderedFacet(
                              facetId="facet1",
                              observations=[
                                  Observation(date="2023-01-01", value=10.0),
                                  Observation(date="2023-01-15", value=15.0),
                              ],
                          )
                      ]
                  }
              })
  })

  mock_facets = {"facet1": Facet(importName="ImportName")}

  # Create an ObservationResponse instance
  response = ObservationResponse(byVariable=mock_data, facets=mock_facets)

  # Call the method and get the result
  result = response.to_observation_records()

  # Expected output
  expected = [
      {
          "date": "2023-01-01",
          "entity": "entity1",
          "variable": "variable1",
          "value": 10.0,
          "facetId": "facet1",
          "importName": "ImportName",
      },
      {
          "date": "2023-01-15",
          "entity": "entity1",
          "variable": "variable1",
          "value": 15.0,
          "facetId": "facet1",
          "importName": "ImportName",
      },
  ]

  # Assert the results
  assert result.model_dump(exclude_none=True) == expected


### ----- Test Resolve Response ----- ###


def test_resolve_response_model_validation():
  """Test that ResolveResponse.model_validate parses entities and candidates."""
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
  response = ResolveResponse.model_validate(json_data)

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
  response = ResolveResponse.model_validate(input_data)

  # Convert back to dictionary using the json property
  result = response.to_dict(exclude_none=False)

  # Assert that the resulting dictionary matches the original input
  assert result == input_data


def test_resolve_response_dict_exclude_none():
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
  expected_data = {
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
                      "dcid": "dcid2"
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

  # Create ResolveResponse from the dictionary
  response = ResolveResponse.model_validate(input_data)

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
  response = ResolveResponse.model_validate(input_data)

  # Convert back to dictionary using the json property
  result = response.to_json(exclude_none=False)

  # Assert that the resulting dictionary matches the original input
  assert result == expected


def test_get_facets_metadata():
  """Test that get_facets_metadata correctly extracts and structures facet metadata."""
  payload = {
      "byVariable": {
          "variable1": {
              "byEntity": {
                  "entity1": {
                      "orderedFacets": [{
                          "facetId": "facet1",
                          "earliestDate": "2023",
                          "latestDate": "2025",
                          "obsCount": 5,
                          "observations": []
                      }]
                  },
                  "entity2": {
                      "orderedFacets": [{
                          "facetId": "facet2",
                          "earliestDate": "2021",
                          "latestDate": "2021",
                          "obsCount": 3,
                          "observations": []
                      }]
                  },
              }
          },
          "variable2": {
              "byEntity": {
                  "entity3": {
                      "orderedFacets": [{
                          "facetId": "facet1",
                          "earliestDate": "2000",
                          "latestDate": "2013",
                          "obsCount": 7,
                          "observations": []
                      }]
                  }
              }
          },
      },
      "facets": {
          "facet1": {
              "unit": "USD",
              "importName": "Import Source"
          },
          "facet2": {
              "unit": "Year",
              "importName": "Another Source"
          },
      },
  }

  # build ObservationResponse
  response = ObservationResponse.model_validate(payload)

  # Call the method
  result = response.get_facets_metadata()

  # Expected structure
  expected = {
      "variable1": {
          "facet1": {
              "earliestDate": {
                  "entity1": "2023"
              },
              "latestDate": {
                  "entity1": "2025"
              },
              "obsCount": {
                  "entity1": 5
              },
              "unit": "USD",
              "importName": "Import Source",
              "measurementMethod": None,
              "observationPeriod": None,
              "provenanceUrl": None,
          },
          "facet2": {
              "earliestDate": {
                  "entity2": "2021"
              },
              "latestDate": {
                  "entity2": "2021"
              },
              "obsCount": {
                  "entity2": 3
              },
              "unit": "Year",
              "importName": "Another Source",
              "measurementMethod": None,
              "observationPeriod": None,
              "provenanceUrl": None,
          },
      },
      "variable2": {
          "facet1": {
              "earliestDate": {
                  "entity3": "2000"
              },
              "latestDate": {
                  "entity3": "2013"
              },
              "obsCount": {
                  "entity3": 7
              },
              "unit": "USD",
              "importName": "Import Source",
              "measurementMethod": None,
              "observationPeriod": None,
              "provenanceUrl": None,
          }
      },
  }

  assert result == expected


def test_find_matching_facet_id(monkeypatch):
  """Tests that find_matching_facet_id correctly finds facet IDs matching a given property and value."""
  mock_response = ObservationResponse(byVariable={}, facets={})
  mock_metadata = {
      "statvar1": {
          "facet1": {
              "measurementMethod": "Census"
          },
          "facet2": {
              "measurementMethod": "Survey"
          },
      },
      "statvar2": {
          "facet3": {
              "unit": "USD"
          },
      },
  }
  monkeypatch.setattr(
      ObservationResponse,
      "get_facets_metadata",
      lambda self: mock_metadata,
  )

  result = mock_response.find_matching_facet_id("measurementMethod", "Census")
  assert result == ["facet1"]

  result = mock_response.find_matching_facet_id("measurementMethod",
                                                ["Census", "Survey"])
  assert result == ["facet1", "facet2"]

  result = mock_response.find_matching_facet_id("unit", "USD")
  assert result == ["facet3"]

  result = mock_response.find_matching_facet_id("measurementMethod",
                                                "Nonexistent")
  assert result == []
