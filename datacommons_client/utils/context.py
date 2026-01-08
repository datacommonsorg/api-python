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

from contextvars import ContextVar
from contextlib import contextmanager
from typing import Optional, Generator

_API_KEY_CONTEXT_VAR: ContextVar[Optional[str]] = ContextVar("api_key", default=None)

@contextmanager
def use_api_key(api_key: str) -> Generator[None, None, None]:
    """Context manager to set the API key for the current execution context.

    Args:
        api_key: The API key to use.
    """
    token = _API_KEY_CONTEXT_VAR.set(api_key)
    try:
        yield
    finally:
        _API_KEY_CONTEXT_VAR.reset(token)
