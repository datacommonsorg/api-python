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

from unittest import mock

import datacommons as dc
import datacommons.utils as utils

import json
import unittest
import urllib


def request_mock(*args, **kwargs):
  """ A mock urlopen call sent in the urllib package. """
  # Create the mock response object.
  class MockResponse:
    def __init__(self, json_data):
      self.json_data = json_data

    def read(self):
      return self.json_data

  req = args[0]
  data = json.loads(req.data)
  # If the API key does not match, then return 403 Forbidden	
  api_key = req.get_header('X-api-key')
  if api_key != 'TEST-API-KEY':
    return urllib.error.HTTPError(None, 403, None, None, None)

  if req.full_url != utils._API_ROOT + utils._API_ENDPOINTS['query']:
    raise ValueError('Unexpected request url %s. Expected query endpoint.', req.full_url)
  # Return a dummy response
  return MockResponse(json.dumps({
    'header': [
      '?name',
      '?dcid'
    ],
  }))


class TestQuery(unittest.TestCase):
  """Unit test for setting the API Key."""
  @mock.patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
  def test_no_rows(self, urlopen):
    """ Handles row-less response. """
    # Set the API key	
    dc.set_api_key('TEST-API-KEY')

    # Create a SPARQL query
    query_string = ('''
SELECT  ?name ?dcid
WHERE {
  ?a typeOf Place .
  ?a name ?name .
  ?a dcid ("geoId/06") .
  ?a dcid ?dcid
}
''')
    # Issue the query
    self.assertEqual(dc.query(query_string), [])

if __name__ == '__main__':
  unittest.main()
