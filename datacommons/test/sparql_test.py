# Copyright 2022 Google Inc.
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

Unit tests for the SPARQL query wrapper.
"""

import unittest
from unittest.mock import patch

import datacommons

_QUERY1 = ('''
SELECT  ?name ?dcid
WHERE {
  ?a typeOf Place .
  ?a name ?name .
  ?a dcid ("geoId/06" "geoId/21" "geoId/24") .
  ?a dcid ?dcid
}
''')

_QUERY2 = ('''
SELECT  ?name ?dcid
WHERE {
  ?a typeOf Place .
  ?a name ?name .
  ?a dcid ("geoId/DNE") .
  ?a dcid ?dcid
}
''')


def _post_mock(path, data):
    """ A mock function for _post. """
    if path == "/query" and data['sparql'] == _QUERY1:
        return {
            'header': ['?name', '?dcid'],
            'rows': [{
                'cells': [{
                    'value': 'California'
                }, {
                    'value': 'geoId/06'
                }]
            }, {
                'cells': [{
                    'value': 'Kentucky'
                }, {
                    'value': 'geoId/21'
                }]
            }, {
                'cells': [{
                    'value': 'Maryland'
                }, {
                    'value': 'geoId/24'
                }]
            }]
        }
    if path == "/query" and data['sparql'] == _QUERY2:
        return {
            'header': ['?name', '?dcid'],
        }

    # Otherwise, return an empty response and a 404.
    return Exception('mock exception')


class TestQuery(unittest.TestCase):
    """ Unit tests for the Query object. """

    @patch('datacommons.sparql._post')
    def test_rows(self, _post):
        """ Sending a valid query returns the correct response. """
        _post.side_effect = _post_mock
        # Create the SPARQL query
        selector = lambda row: row['?name'] != 'California'
        # Issue the query
        results = datacommons.query(_QUERY1)
        selected_results = datacommons.query(_QUERY2, select=selector)
        # Execute the query and iterate through the results.
        for idx, row in enumerate(results):
            if idx == 0:
                self.assertDictEqual(row, {
                    '?name': 'California',
                    '?dcid': 'geoId/06'
                })
            if idx == 1:
                self.assertDictEqual(row, {
                    '?name': 'Kentucky',
                    '?dcid': 'geoId/21'
                })
            if idx == 2:
                self.assertDictEqual(row, {
                    '?name': 'Maryland',
                    '?dcid': 'geoId/24'
                })

        # Verify that the select function works.
        for idx, row in enumerate(selected_results):
            if idx == 0:
                self.assertDictEqual(row, {
                    '?name': 'Kentucky',
                    '?dcid': 'geoId/21'
                })
            if idx == 1:
                self.assertDictEqual(row, {
                    '?name': 'Maryland',
                    '?dcid': 'geoId/24'
                })

    @patch('datacommons.sparql._post')
    def test_no_rows(self, _post):
        """ Handles row-less response. """
        _post.side_effect = _post_mock
        # Issue the query
        self.assertEqual(datacommons.query(_QUERY2), [])


if __name__ == '__main__':
    unittest.main()
