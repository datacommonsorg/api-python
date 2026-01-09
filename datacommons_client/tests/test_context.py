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

from datacommons_client.utils.context import _API_KEY_CONTEXT_VAR
from datacommons_client.utils.context import use_api_key


def test_use_api_key_sets_var():
  """Test that use_api_key sets the context variable."""
  assert _API_KEY_CONTEXT_VAR.get() is None
  with use_api_key("test-key"):
    assert _API_KEY_CONTEXT_VAR.get() == "test-key"
  assert _API_KEY_CONTEXT_VAR.get() is None


def test_use_api_key_nested():
  """Test nested usage of use_api_key."""
  with use_api_key("outer"):
    assert _API_KEY_CONTEXT_VAR.get() == "outer"
    with use_api_key("inner"):
      assert _API_KEY_CONTEXT_VAR.get() == "inner"
    assert _API_KEY_CONTEXT_VAR.get() == "outer"
  assert _API_KEY_CONTEXT_VAR.get() is None


def test_use_api_key_none():
  """Test that use_api_key with None/empty does not set the variable."""
  assert _API_KEY_CONTEXT_VAR.get() is None
  with use_api_key(None):
    assert _API_KEY_CONTEXT_VAR.get() is None
  with use_api_key(""):
    assert _API_KEY_CONTEXT_VAR.get() is None
