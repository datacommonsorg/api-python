from datacommons_client.models.resolve import Candidate
from datacommons_client.models.resolve import Entity


def test_candidate_from_json():
  """Test the Candidate.from_json method with full data."""
  json_data = {"dcid": "dcid123", "dominantType": "Place"}
  candidate = Candidate.from_json(json_data)
  assert candidate.dcid == "dcid123"
  assert candidate.dominantType == "Place"


def test_candidate_from_json_partial():
  """Test Candidate.from_json with missing optional dominantType."""
  json_data = {"dcid": "dcid456"}
  candidate = Candidate.from_json(json_data)
  assert candidate.dcid == "dcid456"
  assert candidate.dominantType is None


def test_entity_from_json():
  """Test the Entity.from_json method with multiple candidates."""
  json_data = {
      "node":
          "test_query",
      "candidates": [
          {
              "dcid": "dcid123",
              "dominantType": "Place"
          },
          {
              "dcid": "dcid456",
              "dominantType": "Event"
          },
      ],
  }
  entity = Entity.from_json(json_data)
  assert entity.node == "test_query"
  assert len(entity.candidates) == 2
  assert entity.candidates[0].dcid == "dcid123"
  assert entity.candidates[0].dominantType == "Place"
  assert entity.candidates[1].dcid == "dcid456"
  assert entity.candidates[1].dominantType == "Event"


def test_entity_from_json_empty_candidates():
  """Test Entity.from_json with no candidates."""
  json_data = {"node": "test_query", "candidates": []}
  entity = Entity.from_json(json_data)
  assert entity.node == "test_query"
  assert len(entity.candidates) == 0


def test_entity_from_json_missing_node():
  """Test Entity.from_json with missing node."""
  json_data = {"candidates": [{"dcid": "dcid123", "dominantType": "Place"}]}
  entity = Entity.from_json(json_data)
  assert entity.node is None
  assert len(entity.candidates) == 1
  assert entity.candidates[0].dcid == "dcid123"
  assert entity.candidates[0].dominantType == "Place"


def test_entity_from_json_empty():
  """Test Entity.from_json with empty data."""
  json_data = {}
  entity = Entity.from_json(json_data)
  assert entity.node is None
  assert len(entity.candidates) == 0
