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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch
import datacommons as dc
import datacommons.utils as utils

import os
import json
import unittest
import six.moves.urllib as urllib

_TEST_API_KEY = 'TEST-API-KEY'

_SPARQL_NO_KEY = 'query_no_key'
_SPARQL_W_KEY = 'query_w_key'

_SEND_REQ_NO_KEY = 'https://send_request_no_key.com'
_SEND_REQ_W_KEY = 'https://send_request_w_key.com'



def request_mock(*args, **kwargs):
  """ A mock urlopen call sent in the urllib package. """
  # Create the mock response object.
  class MockResponse:
    def __init__(self, json_data):
      self.json_data = json_data

    def read(self):
      return self.json_data

  req = args[0]

  if req.get_full_url() == _SEND_REQ_NO_KEY or json.loads(req.data) == {'sparql': _SPARQL_NO_KEY}:
    assert 'X-api-key' not in req.headers
  else:
    assert req.get_header('X-api-key') == _TEST_API_KEY

  if req.get_full_url() == utils._API_ROOT + utils._API_ENDPOINTS['query']:
    # Return a dummy response that will parse into [] by query()
    return MockResponse(json.dumps({
      'header': [
        '?name',
        '?dcid'
      ],
    }))
  else:
    # Return a dummy response that will parse into {} by _send_request()
    return MockResponse(json.dumps({'payload': json.dumps({})}))


class TestApiKey(unittest.TestCase):
  """Unit test for setting or not setting the API Key."""
  @patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
  def test_query_no_api_key(self, urlopen):
    if os.getenv(utils._ENV_VAR_API_KEY):
      del os.environ[utils._ENV_VAR_API_KEY]
    # Issue a dummy SPARQL query that tells the mock to not expect a key
    self.assertEqual(dc.query(_SPARQL_NO_KEY), [])

  @patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
  def test_send_request_no_api_key(self, urlopen):
    if os.getenv(utils._ENV_VAR_API_KEY):
      del os.environ[utils._ENV_VAR_API_KEY]
    # Issue a dummy url that tells the mock to not expect a key
    self.assertEqual(utils._send_request(_SEND_REQ_NO_KEY, {'foo': ['bar']}), {})

  @patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
  def test_query_w_api_key(self, urlopen):
    """ Handles row-less response. """
    # Set the API key	
    dc.set_api_key('make_sure_I_am_replaced')
    dc.set_api_key(_TEST_API_KEY)
    # Issue a dummy SPARQL query that tells the mock to expect a key
    self.assertEqual(dc.query(_SPARQL_W_KEY), [])

  @patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
  def test_send_request_w_api_key(self, urlopen):
    """ Handles row-less response. """
    # Set the API key	
    dc.set_api_key(_TEST_API_KEY)
    # Issue a dummy url that tells the mock to expect a key
    self.assertEqual(utils._send_request(_SEND_REQ_W_KEY), {})


if __name__ == '__main__':
  unittest.main()
