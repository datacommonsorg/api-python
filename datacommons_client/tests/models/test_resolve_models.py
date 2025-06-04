from datacommons_client.models.resolve import Candidate
from datacommons_client.models.resolve import Entity


def test_candidate_model_validation():
  """Test that Candidate.model_validate parses full data correctly."""
  json_data = {"dcid": "dcid123", "dominantType": "Place"}
  candidate = Candidate.model_validate(json_data)
  assert candidate.dcid == "dcid123"
  assert candidate.dominantType == "Place"


def test_candidate_model_validation_partial():
  """Test Candidate.model_validate with missing optional dominantType."""
  json_data = {"dcid": "dcid456"}
  candidate = Candidate.model_validate(json_data)
  assert candidate.dcid == "dcid456"
  assert candidate.dominantType is None


def test_entity_model_validation():
  """Test that Entity.model_validate handles multiple candidates."""
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
  entity = Entity.model_validate(json_data)
  assert entity.node == "test_query"
  assert len(entity.candidates) == 2
  assert entity.candidates[0].dcid == "dcid123"
  assert entity.candidates[0].dominantType == "Place"
  assert entity.candidates[1].dcid == "dcid456"
  assert entity.candidates[1].dominantType == "Event"


def test_entity_model_validation_empty_candidates():
  """Test Entity.model_validate with no candidates."""
  json_data = {"node": "test_query", "candidates": []}
  entity = Entity.model_validate(json_data)
  assert entity.node == "test_query"
  assert len(entity.candidates) == 0
