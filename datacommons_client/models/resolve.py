from typing import List, Optional, TypeAlias

from pydantic import Field

from datacommons_client.models.base import BaseDCModel

Query: TypeAlias = str
DCID: TypeAlias = str
DominantType: TypeAlias = str


class Candidate(BaseDCModel):
  """Represents a candidate in the resolution response.

    Attributes:
        dcid (DCID): The Data Commons ID for the candidate.
        dominantType (Optional[DominantType]): The dominant type of the candidate,
            if available. This represents the primary type associated with the DCID.
    """

  dcid: DCID = Field(default_factory=str)
  dominantType: Optional[DominantType] = None


class Entity(BaseDCModel):
  """Represents an entity with its resolution candidates.

    Attributes:
        node (Query): The query string or node being resolved.
        candidates (List[Candidate]): A list of candidates that match the query.
    """

  node: Query
  candidates: list[Candidate] = Field(default_factory=list)