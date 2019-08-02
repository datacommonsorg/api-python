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

import unittest


def post_request_mock(*args, **kwargs):
  """ A mock POST requests sent in the requests package. """
  # Create the mock response object.
  class MockResponse:
    def __init__(self, json_data, status_code):
      self.json_data = json_data
      self.status_code = status_code

    def json(self):
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
  req = kwargs['json']

  # Mock responses for post requests to query.
  if args[0] == utils._API_ROOT + utils._API_ENDPOINTS['query']\
    and req['sparql'] == accepted_query:
    return MockResponse({
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
    }, 200)

  # Otherwise, return an empty response and a 404.
  return MockResponse({}, 404)


class TestQuery(unittest.TestCase):
  """ Unit tests for the Query object. """

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_rows(self, post_mock):
    """ Sending a valid query returns the correct response. """
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
    query = dc.Query(sparql=query_string)

    # Execute the query and iterate through the results.
    for idx, row in enumerate(query.rows()):
      if idx == 0:
        self.assertDictEqual(row, {'?name': 'California', '?dcid': 'geoId/06'})
      if idx == 1:
        self.assertDictEqual(row, {'?name': 'Kentucky', '?dcid': 'geoId/21'})
      if idx == 2:
        self.assertDictEqual(row, {'?name': 'Maryland', '?dcid': 'geoId/24'})

    # Verify that the select function works.
    selector = lambda row: row['?name'] != 'California'
    for idx, row in enumerate(query.rows(select=selector)):
      if idx == 0:
        self.assertDictEqual(row, {'?name': 'Kentucky', '?dcid': 'geoId/21'})
      if idx == 1:
        self.assertDictEqual(row, {'?name': 'Maryland', '?dcid': 'geoId/24'})


if __name__ == '__main__':
  unittest.main()
