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
""" Data Commons base Python Client API.

Unit tests for Population and Observation methods in the Data Commons Python
Client API.
"""

from pandas.util.testing import assert_series_equal
from unittest import mock

import datacommons as dc
import datacommons.utils as utils
import pandas as pd

import json
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

  # Get the request json and allowed constraining properties
  req = kwargs['json']
  constrained_props = [
    {
      'property': 'placeOfBirth',
      'value': 'BornInOtherStateInTheUnitedStates'
    },
    {
      'property': 'age',
      'value': 'Years5To17'
    }
  ]

  # Mock responses for post requests to get_populations.
  if args[0] == utils._API_ROOT + utils._API_ENDPOINTS['get_populations']\
    and req['population_type'] == 'Person'\
    and req['pvs'] == constrained_props:
    if req['dcids'] == ['geoId/06085', 'geoId/4805000']:
      # Response returned when querying for multiple valid dcids.
      res_json = json.dumps([
        {
          'dcid': 'geoId/06085',
          'population': 'dc/p/crgfn8blpvl35'
        },
        {
          'dcid': 'geoId/4805000',
          'population': 'dc/p/f3q9whmjwbf36'
        }
      ])
      return MockResponse({'payload': res_json}, 200)
    if req['dcids'] == ['geoId/06085', 'dc/MadDcid']:
      # Response returned when querying for a dcid that does not exist.
      res_json = json.dumps([
        {
          'dcid': 'geoId/06085',
          'population': 'dc/p/crgfn8blpvl35'
        },
      ])
      return MockResponse({'payload': res_json}, 200)
    if req['dcids'] == ['dc/MadDcid', 'dc/MadderDcid'] or req['dcids'] == []:
      # Response returned when both given dcids do not exist or no dcids are
      # provided to the method.
      res_json = json.dumps([])
      return MockResponse({'payload': res_json}, 200)
  if args[0] == utils._API_ROOT + utils._API_ENDPOINTS['get_observations']\
    and req['measured_property'] == 'count'\
    and req['stats_type'] == 'measuredValue'\
    and req['observation_date'] == '2018-12'\
    and req['observation_period'] == 'P1M'\
    and req['measurement_method'] == 'BLSSeasonallyAdjusted':
    if req['dcids'] == ['dc/p/x6t44d8jd95rd', 'dc/p/lr52m1yr46r44', 'dc/p/fs929fynprzs']:
      # Response returned when querying for multiple valid dcids.
      res_json = json.dumps([
        {
          'dcid': 'dc/p/x6t44d8jd95rd',
          'observation': '18704962.000000'
        },
        {
          'dcid': 'dc/p/lr52m1yr46r44',
          'observation': '3075662.000000'
        },
        {
          'dcid': 'dc/p/fs929fynprzs',
          'observation': '1973955.000000'
        }
      ])
      return MockResponse({'payload': res_json}, 200)
    if req['dcids'] == ['dc/p/x6t44d8jd95rd', 'dc/MadDcid']:
      # Response returned when querying for a dcid that does not exist.
      res_json = json.dumps([
        {
          'dcid': 'dc/p/x6t44d8jd95rd',
          'observation': '18704962.000000'
        },
      ])
      return MockResponse({'payload': res_json}, 200)
    if req['dcids'] == ['dc/MadDcid', 'dc/MadderDcid'] or req['dcids'] == []:
      # Response returned when both given dcids do not exist or no dcids are
      # provided to the method.
      res_json = json.dumps([])
      return MockResponse({'payload': res_json}, 200)

  # Otherwise, return an empty response and a 404.
  return MockResponse({}, 404)


class TestGetPopulations(unittest.TestCase):
  """ Unit tests for get_populations. """

  _constraints = {
    'placeOfBirth': 'BornInOtherStateInTheUnitedStates',
    'age': 'Years5To17'
  }

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_multiple_dcids(self, post_mock):
    """ Calling get_populations with proper dcids returns valid results. """
    # Call get_populations
    populations = dc.get_populations(['geoId/06085', 'geoId/4805000'], 'Person',
                                     constraining_properties=self._constraints)
    self.assertDictEqual(populations, {
      'geoId/06085': 'dc/p/crgfn8blpvl35',
      'geoId/4805000': 'dc/p/f3q9whmjwbf36'
    })


  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_bad_dcids(self, post_mock):
    """ Calling get_populations with dcids that do not exist returns empty
    results.
    """
    # Call get_populations
    pops_1 = dc.get_populations(['geoId/06085', 'dc/MadDcid'], 'Person',
                                constraining_properties=self._constraints)
    pops_2 = dc.get_populations(['dc/MadDcid', 'dc/MadderDcid'], 'Person',
                                constraining_properties=self._constraints)

    # Verify the results
    self.assertDictEqual(pops_1, {'geoId/06085': 'dc/p/crgfn8blpvl35'})
    self.assertDictEqual(pops_2, {})

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_no_dcids(self, post_mock):
    """ Calling get_populations with no dcids returns empty results. """
    pops = dc.get_populations(
      [], 'Person', constraining_properties=self._constraints)
    self.assertDictEqual(pops, {})

  # ---------------------------- PANDAS UNIT TESTS ----------------------------

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_series_multiple_dcids(self, post_mock):
    """ Calling get_populations with a Pandas Series and proper dcids returns
    a Pandas Series with valid results.
    """
    # Get the input and expected output
    dcids = pd.Series(['geoId/06085', 'geoId/4805000'])
    expected = pd.Series(['dc/p/crgfn8blpvl35', 'dc/p/f3q9whmjwbf36'])

    # Call get_populations
    actual = dc.get_populations(
      dcids, 'Person', constraining_properties=self._constraints)
    assert_series_equal(actual, expected)

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_series_bad_dcids(self, post_mock):
    """ Calling get_populations with a Pandas Series and dcids that do not exist
    returns empty results.
    """
    # Get input and expected output
    dcids_1 = pd.Series(['geoId/06085', 'dc/MadDcid'])
    dcids_2 = pd.Series(['dc/MadDcid', 'dc/MadderDcid'])
    expected_1 = pd.Series(['dc/p/crgfn8blpvl35', ''])
    expected_2 = pd.Series(['', ''])

    # Call get_populations
    actual_1 = dc.get_populations(
      dcids_1, 'Person', constraining_properties=self._constraints)
    actual_2 = dc.get_populations(
      dcids_2, 'Person', constraining_properties=self._constraints)

    # Assert that the results are correct
    assert_series_equal(actual_1, expected_1)
    assert_series_equal(actual_2, expected_2)

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_series_no_dcids(self, post_mock):
    """ Calling get_populations with no dcids returns empty results. """
    dcids = pd.Series([])
    expected = pd.Series([])

    # Call get_populations
    actual = dc.get_populations(
      dcids, 'Person', constraining_properties=self._constraints)
    assert_series_equal(actual, expected)


class TestGetObservations(unittest.TestCase):
  """ Unit tests for get_observations. """

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_multiple_dcids(self, post_mock):
    """ Calling get_observations with proper dcids returns valid results. """
    dcids = ['dc/p/x6t44d8jd95rd', 'dc/p/lr52m1yr46r44', 'dc/p/fs929fynprzs']
    expected = {
      'dc/p/lr52m1yr46r44': 3075662.0,
      'dc/p/fs929fynprzs': 1973955.0,
      'dc/p/x6t44d8jd95rd': 18704962.0
    }
    actual = dc.get_observations(dcids, 'count', 'measuredValue', '2018-12',
                                 observation_period='P1M',
                                 measurement_method='BLSSeasonallyAdjusted')
    self.assertDictEqual(actual, expected)

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_bad_dcids(self, post_mock):
    """ Calling get_observations with dcids that do not exist returns empty
    results.
    """
    # Get the input
    dcids_1 = ['dc/p/x6t44d8jd95rd', 'dc/MadDcid']
    dcids_2 = ['dc/MadDcid', 'dc/MadderDcid']

    # Call get_observations
    actual_1 = dc.get_observations(dcids_1, 'count', 'measuredValue', '2018-12',
                                   observation_period='P1M',
                                   measurement_method='BLSSeasonallyAdjusted')
    actual_2 = dc.get_observations(dcids_2, 'count', 'measuredValue', '2018-12',
                                   observation_period='P1M',
                                   measurement_method='BLSSeasonallyAdjusted')

    # Verify the results
    self.assertDictEqual(actual_1, {'dc/p/x6t44d8jd95rd': 18704962.0})
    self.assertDictEqual(actual_2, {})

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_no_dcids(self, post_mock):
    """ Calling get_observations with no dcids returns empty results. """
    actual = dc.get_observations([], 'count', 'measuredValue', '2018-12',
                                 observation_period='P1M',
                                 measurement_method='BLSSeasonallyAdjusted')
    self.assertDictEqual(actual, {})

  # ---------------------------- PANDAS UNIT TESTS ----------------------------

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_series_multiple_dcids(self, post_mock):
    """ Calling get_observations with a Pandas Series and proper dcids returns
    a Pandas Series with valid results.
    """
    dcids = pd.Series(
      ['dc/p/x6t44d8jd95rd', 'dc/p/lr52m1yr46r44', 'dc/p/fs929fynprzs'])
    expected = pd.Series([18704962.0, 3075662.0, 1973955.0])
    actual = dc.get_observations(dcids, 'count', 'measuredValue', '2018-12',
                                 observation_period='P1M',
                                 measurement_method='BLSSeasonallyAdjusted')
    assert_series_equal(actual, expected)

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_series_bad_dcids(self, post_mock):
    """ Calling get_observations with a Pandas Series and dcids that do not
    exist returns empty results.
    """
    # Get the input and expected output
    dcids_1 = pd.Series(['dc/p/x6t44d8jd95rd', 'dc/MadDcid'])
    dcids_2 = pd.Series(['dc/MadDcid', 'dc/MadderDcid'])
    expected_1 = pd.Series([18704962.0, float('NaN')])
    expected_2 = pd.Series([float('NaN'), float('NaN')])

    # Call get_observations
    actual_1 = dc.get_observations(dcids_1, 'count', 'measuredValue', '2018-12',
                                   observation_period='P1M',
                                   measurement_method='BLSSeasonallyAdjusted')
    actual_2 = dc.get_observations(dcids_2, 'count', 'measuredValue', '2018-12',
                                   observation_period='P1M',
                                   measurement_method='BLSSeasonallyAdjusted')

    # Verify the results
    assert_series_equal(actual_1, expected_1)
    assert_series_equal(actual_2, expected_2)

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_series_no_dcids(self, post_mock):
    """ Calling get_observations with no dcids returns empty results. """
    dcids = pd.Series([])
    expected = pd.Series([])
    actual = dc.get_observations(dcids, 'count', 'measuredValue', '2018-12',
                                 observation_period='P1M',
                                 measurement_method='BLSSeasonallyAdjusted')
    assert_series_equal(actual, expected)


if __name__ == '__main__':
  unittest.main()
