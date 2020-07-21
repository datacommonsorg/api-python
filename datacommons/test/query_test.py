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
""" Data Commons Python Client API unit tests.

Unit tests for the SPARQL query wrapper.
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

  # The accepted query.
  accepted_query = ('''
SELECT  ?name ?dcid
WHERE {
  ?a typeOf Place .
  ?a name ?name .
  ?a dcid ("geoId/06" "geoId/21" "geoId/24") .
  ?a dcid ?dcid
}
''')
  req = args[0]
  data = json.loads(req.data)

  # If the API key does not match, then return 403 Forbidden
  api_key = req.get_header('X-api-key')
  if api_key != 'TEST-API-KEY':
    return urllib.error.HTTPError(None, 403, None, None, None)

  # Mock responses for urlopen requests to query.
  if req.full_url == utils._API_ROOT + utils._API_ENDPOINTS['query']\
    and data['sparql'] == accepted_query:
    return MockResponse(json.dumps({
      'header': [
        '?name',
        '?dcid'
      ],
      'rows': [
        {
          'cells': [
            {
              'value': 'California'
            },
            {
              'value': 'geoId/06'
            }
          ]
        },
        {
          'cells': [
            {
              'value': 'Kentucky'
            },
            {
              'value': 'geoId/21'
            }
          ]
        },
        {
          'cells': [
            {
              'value': 'Maryland'
            },
            {
              'value': 'geoId/24'
            }
          ]
        }
      ]
    }))

  # Otherwise, return an empty response and a 404.
  return urllib.error.HTTPError(None, 404, None, None, None)


class TestQuery(unittest.TestCase):
  """ Unit tests for the Query object. """

  @mock.patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
  def test_rows(self, urlopen):
    """ Sending a valid query returns the correct response. """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    # Create the SPARQL query
    query_string = ('''
SELECT  ?name ?dcid
WHERE {
  ?a typeOf Place .
  ?a name ?name .
  ?a dcid ("geoId/06" "geoId/21" "geoId/24") .
  ?a dcid ?dcid
}
''')
    selector = lambda row: row['?name'] != 'California'

    # Issue the query
    results = dc.query(query_string)
    selected_results = dc.query(query_string, select=selector)

    # Execute the query and iterate through the results.
    for idx, row in enumerate(results):
      if idx == 0:
        self.assertDictEqual(row, {'?name': 'California', '?dcid': 'geoId/06'})
      if idx == 1:
        self.assertDictEqual(row, {'?name': 'Kentucky', '?dcid': 'geoId/21'})
      if idx == 2:
        self.assertDictEqual(row, {'?name': 'Maryland', '?dcid': 'geoId/24'})

    # Verify that the select function works.
    for idx, row in enumerate(selected_results):
      if idx == 0:
        self.assertDictEqual(row, {'?name': 'Kentucky', '?dcid': 'geoId/21'})
      if idx == 1:
        self.assertDictEqual(row, {'?name': 'Maryland', '?dcid': 'geoId/24'})


if __name__ == '__main__':
  unittest.main()
