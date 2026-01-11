# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Generator

_API_KEY_CONTEXT_VAR: ContextVar[str | None] = ContextVar("api_key",
                                                          default=None)


@contextmanager
def use_api_key(api_key: str | None) -> Generator[None, None, None]:
  """Context manager to set the API key for the current execution context.

    If api_key is None or empty, this context manager does nothing, allowing
    the underlying client to use its default API key.

    Args:
        api_key: The API key to use. If None or empty, no change is made.

    Example:
        from datacommons_client import use_api_key
        # ...
        client = DataCommonsClient(api_key="default-key")

        # Uses "default-key"
        client.observation.fetch(...)

        with use_api_key("temp-key"):
            # Uses "temp-key"
            client.observation.fetch(...)

        # Back to "default-key"
        client.observation.fetch(...)
    """
  if not api_key:
    yield
    return

  token = _API_KEY_CONTEXT_VAR.set(api_key)
  try:
    yield
  finally:
    _API_KEY_CONTEXT_VAR.reset(token)
