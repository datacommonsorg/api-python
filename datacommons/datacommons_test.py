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

import datacommons
import mock
import pandas as pd
import unittest

from pandas.util.testing import assert_frame_equal


class MockMethod(object):

  def __init__(self, request, response):
    self._request = request
    self._response = response

  def execute(self):
    return self._response


class MockResource(object):

  def __init__(self, parent):
    self._parent = parent

  def setup(self, request, response):
    self._method = MockMethod(request, response)

  def query(self, body):
    self._parent.assertEqual(self._method._request, body)
    return self._method


class AppTest(unittest.TestCase):

  def test_query(self):
    # Setup handler mocks.
    mock_resource = MockResource(self)
    datacommons._auth.do_auth = mock.Mock(return_value=mock_resource)

    # Initialize, and validate.
    dc = datacommons.Client()
    self.assertEqual(True, dc._inited)

    # Setup mock query.
    continent_query = ('SELECT ?name ?area_sq_mi, typeOf ?c Continent, '
                       'name ?c ?name, area ?c ?area, dcid ?c "X123"')
    mock_resource.setup({
        'query': continent_query
    }, {
        'rows': [{
            'cells': [{
                'value': ['Oceania']
            }, {
                'value': ['3292000']
            }]
        }],
        'header': ['name', 'area_sq_mi']
    })
    expected_df = pd.DataFrame({
        'name': [['Oceania']],
        'area_sq_mi': [['3292000']]
    })
    expected_df = expected_df[['name', 'area_sq_mi']]

    assert_frame_equal(expected_df, dc.Query(continent_query))


if __name__ == '__main__':
  unittest.main()
