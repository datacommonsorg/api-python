from unittest import mock

import pytest

from datacommons_client.utils.decorators import requires_pandas

try:
  import pandas as pd

  PANDAS_AVAILABLE = True
except ImportError:
  PANDAS_AVAILABLE = False


@requires_pandas
def function_requiring_pandas():
  return "Pandas is available"


def test_requires_pandas_with_pandas():
  """Test that the function executes normally when Pandas is available."""
  if PANDAS_AVAILABLE:
    assert function_requiring_pandas() == "Pandas is available"


def test_requires_pandas_without_pandas(monkeypatch):
  """Test that the decorator raises ImportError when Pandas is not available."""
  # Simulate Pandas being unavailable
  monkeypatch.setattr("datacommons_client.utils.decorators.pd", None)
  with pytest.raises(ImportError, match="Pandas is required for this method"):
    function_requiring_pandas()


def test_importerror_handling(monkeypatch):
  """Test that the ImportError block is executed when Pandas is not installed."""

  # Simulate pandas not being available
  with mock.patch.dict("sys.modules", {"pandas": None}):
    import importlib

    # Reload the module so that a new check of Pandas is performed
    import datacommons_client.utils.decorators
    importlib.reload(datacommons_client.utils.decorators)

  # Ensure pd is set to None
  assert datacommons_client.utils.decorators.pd is None
