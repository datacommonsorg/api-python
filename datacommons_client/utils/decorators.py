from functools import wraps

try:
  import pandas as pd
except ImportError:
  pd = None


def requires_pandas(func):
  """Decorator to check if Pandas is available before executing a method."""

  @wraps(func)
  def wrapper(*args, **kwargs):
    if pd is None:
      raise ImportError("Pandas is required for this method")
    return func(*args, **kwargs)

  return wrapper
