# Copyright 2017 Google Inc.
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
""" Data Commons Python API unit tests.

Unit tests setting the API Key.
"""
import unittest

import datacommons.key as key

_KEY = "test-api-key"


class TestApiKey(unittest.TestCase):
    """Unit test for setting or not setting the API Key."""

    def test_set_api_key(self):
        key.set_api_key(_KEY)
        self.assertEqual(key.get_api_key(), _KEY)


if __name__ == '__main__':
    unittest.main()
