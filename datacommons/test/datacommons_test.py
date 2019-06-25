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
"""Test for DataCommons API."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

import datacommons
import mock
import pandas as pd
from pandas.util.testing import assert_frame_equal


class MockQuery(object):

  def __init__(self):
    self._req_resp = {}

  def add_query(self, request, response):
    self._req_resp[request['query']] = response

  def select_request(self, request):
    self._request = request

  def execute(self):
    return self._req_resp[self._request['query']]


class MockGetPropType(object):

  def execute(self):
    return {
        'type_info': [{
            'node_type': 'State',
            'prop_name': 'containedInPlace',
            'prop_type': 'Country'
        }, {
            'node_type': 'State',
            'prop_name': 'name',
            'prop_type': 'Text'
        }]
    }


class MockResource(object):

  def __init__(self):
    self._query_method = MockQuery()
    self._get_prop_type_method = MockGetPropType()

  def add_query(self, request, response):
    self._query_method.add_query(request, response)

  def query(self, body):
    self._query_method.select_request(body)
    return self._query_method

  def get_prop_type(self, body):
    del body
    return self._get_prop_type_method


class AppTest(unittest.TestCase):

  def test_query(self):
    # Setup handler mocks.
    mock_resource = MockResource()
    datacommons._auth.do_auth = mock.Mock(return_value=mock_resource)

    # Initialize, and validate.
    dc = datacommons.Client()
    self.assertEqual(True, dc._inited)
    self.assertEqual({
        'State': {
            'name': 'Text',
            'containedInPlace': 'Country'
        }
    }, dc._prop_type)
    self.assertEqual({
        'Country': {
            'containedInPlace': 'State'
        }
    }, dc._inv_prop_type)

    # Setup mock query.
    continent_query = ('SELECT ?name ?area_sq_mi, typeOf ?c Continent, '
                       'name ?c ?name, area ?c ?area, dcid ?c "X123"')
    mock_resource.add_query({
        'options': {
            'row_count_limit': 100
        },
        'query': continent_query
    }, {
        'rows': [{
            'cells': [{
                'string_value': 'Oceania'
            }, {
                'string_value': '3292000'
            }]
        }, {
            'cells': [{
                'string_value': 'North America'
            }, {
                'string_value': '3292000'
            }]
        }],
        'header': ['name', 'area_sq_mi']
    })
    expected_df = pd.DataFrame({
        'name': ['Oceania', 'North America'],
        'area_sq_mi': ['3292000', '3292000']
    })
    expected_df = expected_df[['name', 'area_sq_mi']]

    assert_frame_equal(expected_df, dc.query(continent_query))


if __name__ == '__main__':
  unittest.main()
