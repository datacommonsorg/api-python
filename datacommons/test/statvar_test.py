# Copyright 2020 Google Inc.
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

Unit tests for StatVar methods in the Data Commons Python API.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from unittest import mock

import datacommons as dc
import datacommons.utils as utils
import json
import unittest
import six.moves.urllib as urllib


def request_mock(*args, **kwargs):
    """A mock urlopen requests sent in the requests package."""

    # Create the mock response object.
    class MockResponse:

        def __init__(self, json_data):
            self.json_data = json_data

        def read(self):
            return self.json_data

    req = args[0]

    # Mock responses for urlopen requests to get_stat_value.
    stat_value_url_base = utils._API_ROOT + utils._API_ENDPOINTS[
        'get_stat_value']
    stat_series_url_base = utils._API_ROOT + utils._API_ENDPOINTS[
        'get_stat_series']
    if req.full_url == stat_value_url_base + '?place=geoId/06&stat_var=Count_Person':
        # Response returned when querying with basic args.
        return MockResponse(json.dumps({'value': 123}))
    if req.full_url == stat_value_url_base + '?place=geoId/06&stat_var=Count_Person&date=2010':
        # Response returned when querying with observationDate.
        return MockResponse(json.dumps({'value': 133}))
    if (req.full_url == stat_value_url_base +
            '?place=geoId/06&stat_var=Count_Person&' +
            'date=2010&measurement_method=CensusPEPSurvey&' +
            'observation_period=P1Y&unit=RealPeople&scaling_factor=100'):
        # Response returned when querying with above optional params.
        return MockResponse(json.dumps({'value': 103}))
    if req.full_url == stat_series_url_base + '?place=geoId/06&stat_var=Count_Person':
        # Response returned when querying with basic args.
        return MockResponse(json.dumps({'series': {'2000': 1, '2001': 2}}))
    if (req.full_url == stat_series_url_base +
            '?place=geoId/06&stat_var=Count_Person&' +
            'measurement_method=CensusPEPSurvey&observation_period=P1Y&' +
            'unit=RealPeople&scaling_factor=100'):

        # 'CensusPEPSurvey', 'P1Y', 'RealPeople', 100
        # Response returned when querying with above optional params.
        return MockResponse(json.dumps({'series': {'2000': 3, '2001': 42}}))
    if (req.full_url == stat_series_url_base +
            '?place=geoId/06&stat_var=Count_Person&' +
            'measurement_method=DNE'):

        # 'CensusPEPSurvey', 'P1Y', 'RealPeople', 100
        # Response returned when data not available for options.
        # /stat/series?place=geoId/06&stat_var=Count_Person&measurement_method=DNE
        return MockResponse(json.dumps({'series': {}}))
    # Otherwise, return an empty response and a 404.
    return urllib.error.HTTPError


class TestGetStatValue(unittest.TestCase):
    """Unit tests for get_stat_value."""

    @mock.patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
    def test_basic(self, urlopen):
        """Calling get_stat_value with minimal and proper args."""
        # Call get_stat_value

        self.assertEqual(dc.get_stat_value('geoId/06', 'Count_Person'), 123)

    @mock.patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
    def test_opt_args(self, urlopen):
        """Calling get_stat_value with optional args returns specific data."""
        # Call get_stat_value for specific obs
        self.assertEqual(dc.get_stat_value('geoId/06', 'Count_Person', '2010'),
                         133)

        # Call get_stat_value with all optional args
        stat = dc.get_stat_value('geoId/06', 'Count_Person', '2010',
                                 'CensusPEPSurvey', 'P1Y', 'RealPeople', 100)
        self.assertEqual(stat, 103)


class TestGetStatSeries(unittest.TestCase):
    """Unit tests for get_stat_series."""

    @mock.patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
    def test_basic(self, urlopen):
        """Calling get_stat_value with minimal and proper args."""
        # Call get_stat_series
        stats = dc.get_stat_series('geoId/06', 'Count_Person')
        self.assertEqual(stats, {'2000': 1, '2001': 2})

    @mock.patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
    def test_opt_args(self, urlopen):
        """Calling get_stat_value with optional args returns specific data."""

        # Call get_stat_series with all optional args
        stats = dc.get_stat_series('geoId/06', 'Count_Person',
                                   'CensusPEPSurvey', 'P1Y', 'RealPeople', 100)
        self.assertEqual(stats, {'2000': 3, '2001': 42})

        # Call get_stat_series with non-satisfiable optional args
        stats = dc.get_stat_series('geoId/06', 'Count_Person', 'DNE')
        self.assertEqual(stats, {})


if __name__ == '__main__':
    unittest.main()
