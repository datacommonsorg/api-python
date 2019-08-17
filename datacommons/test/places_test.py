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

Unit tests for Place methods in the Data Commons Python Client API.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from pandas.util.testing import assert_series_equal
import base64
from unittest import mock

import datacommons as dc
import datacommons.utils as utils
import pandas as pd

import json
import unittest
import zlib


def post_request_mock(*args, **kwargs):
  """ A mock POST requests sent in the requests package. """
  # Create the mock response object.
  class MockResponse:
    def __init__(self, json_data, status_code):
      self.json_data = json_data
      self.status_code = status_code

    def json(self):
      return self.json_data

  # Get the request json
  req = kwargs['json']
  headers = kwargs['headers']

  # If the API key does not match, then return 403 Forbidden
  if 'x-api-key' not in headers or headers['x-api-key'] != 'TEST-API-KEY':
    return MockResponse({}, 403)

  # Mock responses for post requests to get_places_in.
  if args[0] == utils._API_ROOT + utils._API_ENDPOINTS['get_places_in']:
    if req['dcids'] == ['geoId/06085', 'geoId/24031']\
      and req['place_type'] == 'City':
      # Response returned when querying for multiple valid dcids.
      res_json = json.dumps([
        {
          'dcid': 'geoId/06085',
          'place': 'geoId/0649670',
        },
        {
          'dcid': 'geoId/24031',
          'place': 'geoId/2467675',
        },
        {
          'dcid': 'geoId/24031',
          'place': 'geoId/2476650',
        },
      ])
      return MockResponse({'payload': res_json}, 200)
    if req['dcids'] == ['geoId/06085', 'dc/MadDcid']\
      and req['place_type'] == 'City':
      # Response returned when querying for a dcid that does not exist.
      res_json = json.dumps([
        {
          'dcid': 'geoId/06085',
          'place': 'geoId/0649670',
        },
      ])
      return MockResponse({'payload': res_json}, 200)
    if req['dcids'] == ['dc/MadDcid', 'dc/MadderDcid']\
      and req['place_type'] == 'City':
      # Response returned when both given dcids do not exist.
      res_json = json.dumps([])
      return MockResponse({'payload': res_json}, 200)
    if req['dcids'] == [] and req['place_type'] == 'City':
      res_json = json.dumps([])
      # Response returned when no dcids are given.
      return MockResponse({'payload': res_json}, 200)

  # Otherwise, return an empty response and a 404.
  return MockResponse({}, 404)


def get_request_mock(*args, **kwargs):
  """ A mock GET requests sent in the requests package. """
  # Create the mock response object.
  class MockResponse:
    def __init__(self, json_data, status_code):
      self.json_data = json_data
      self.status_code = status_code

    def json(self):
      return self.json_data

  headers = kwargs['headers']

  # If the API key does not match, then return 403 Forbidden
  if 'x-api-key' not in headers or headers['x-api-key'] != 'TEST-API-KEY':
    return MockResponse({}, 403)

  # Mock responses for get requests to get_pop_obs.
  if args[0] == utils._API_ROOT + utils._API_ENDPOINTS['get_pop_obs'] + '?dcid=geoId/06085':
    # Response returned when querying for a city in the graph.
    res_json = json.dumps({
      'name': 'Mountain View',
      'placeType': 'City',
      'populations': {
        'dc/p/013ldrstf6lnf': {
          'numConstraints': 6,
          'observations': [
            {
              'marginOfError': 119,
              'measuredProp': 'count',
              'measuredValue': 225,
              'measurementMethod': 'CenusACS5yrSurvey',
              'observationDate': '2014'
            }, {
              'marginOfError': 108,
              'measuredProp': 'count',
              'measuredValue': 180,
              'measurementMethod': 'CenusACS5yrSurvey',
              'observationDate': '2012'
            }
          ],
          'popType': 'Person',
          'propertyValues': {
            'age': 'Years16Onwards',
            'gender': 'Male',
            'income': 'USDollar30000To34999',
            'incomeStatus': 'WithIncome',
            'race': 'USC_HispanicOrLatinoRace',
            'workExperience': 'USC_NotWorkedFullTime'
          }
        }
      }
    })
    return MockResponse({'payload': base64.b64encode(zlib.compress(res_json.encode('utf-8')))}, 200)

  # Otherwise, return an empty response and a 404.
  return MockResponse({}, 404)


class TestGetPlacesIn(unittest.TestCase):
  """ Unit stests for get_places_in. """

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_multiple_dcids(self, post_mock):
    """ Calling get_places_in with proper dcids returns valid results. """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    # Call get_places_in
    places = dc.get_places_in(['geoId/06085', 'geoId/24031'], 'City')
    self.assertDictEqual(places, {
      'geoId/06085': ['geoId/0649670'],
      'geoId/24031': ['geoId/2467675', 'geoId/2476650']
    })

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_bad_dcids(self, post_mock):
    """ Calling get_places_in with dcids that do not exist returns empty
    results.
    """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    # Call get_places_in with one dcid that does not exist
    bad_dcids_1 = dc.get_places_in(['geoId/06085', 'dc/MadDcid'], 'City')
    self.assertDictEqual(bad_dcids_1, {
      'geoId/06085': ['geoId/0649670'],
      'dc/MadDcid': []
    })

    # Call get_places_in when both dcids do not exist
    bad_dcids_2 = dc.get_places_in(['dc/MadDcid', 'dc/MadderDcid'], 'City')
    self.assertDictEqual(bad_dcids_2, {
      'dc/MadDcid': [],
      'dc/MadderDcid': []
    })

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_no_dcids(self, post_mock):
    """ Calling get_places_in with no dcids returns empty results. """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    # Call get_places_in with no dcids.
    bad_dcids = dc.get_places_in(['dc/MadDcid', 'dc/MadderDcid'], 'City')
    self.assertDictEqual(bad_dcids, {
      'dc/MadDcid': [],
      'dc/MadderDcid': []
    })

  # ---------------------------- PANDAS UNIT TESTS ----------------------------

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_series_multiple_dcids(self, post_mock):
    """ Calling get_places_in with a Pandas Series and proper dcids returns
    a Pandas Series with valid results.
    """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    # Get the input dcids and expected output
    dcids = pd.Series(['geoId/06085', 'geoId/24031'])
    expected = pd.Series(
      [['geoId/0649670'], ['geoId/2467675', 'geoId/2476650']])

    # Call get_places_in
    actual = dc.get_places_in(dcids, 'City')
    assert_series_equal(actual, expected)

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_series_bad_dcids(self, post_mock):
    """ Calling get_places_in with a Pandas Series and dcids that do not exist
    returns empty results.
    """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    # Get the input dcids and expected output
    bad_dcids_1 = pd.Series(['geoId/06085', 'dc/MadDcid'])
    bad_dcids_2 = pd.Series(['dc/MadDcid', 'dc/MadderDcid'])
    expected_1 = pd.Series([['geoId/0649670'], []])
    expected_2 = pd.Series([[], []])

    # Call get_places_in
    actual_1 = dc.get_places_in(bad_dcids_1, 'City')
    actual_2 = dc.get_places_in(bad_dcids_2, 'City')

    # Assert that the answers are correct
    assert_series_equal(actual_1, expected_1)
    assert_series_equal(actual_2, expected_2)

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_series_no_dcids(self, post_mock):
    """ Calling get_places_in with no dcids returns empty results. """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    # Get the input and expected output
    bad_dcids = pd.Series([])
    expected = pd.Series([])

    # Test get_places_in
    actual = dc.get_places_in(bad_dcids, 'City')
    assert_series_equal(actual, expected)


class TestGetPopObs(unittest.TestCase):
  """ Unit stests for get_pop_Obs. """

  @mock.patch('requests.get', side_effect=get_request_mock)
  def test_valid_dcid(self, get_mock):
    """ Calling get_pop_obs with valid dcid returns valid results. """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    # Call get_places_in
    popobs = dc.get_pop_obs('geoId/06085')
    self.assertDictEqual(popobs, {
      'name': 'Mountain View',
      'placeType': 'City',
      'populations': {
        'dc/p/013ldrstf6lnf': {
          'numConstraints': 6,
          'observations': [
            {
              'marginOfError': 119,
              'measuredProp': 'count',
              'measuredValue': 225,
              'measurementMethod': 'CenusACS5yrSurvey',
              'observationDate': '2014'
            }, {
              'marginOfError': 108,
              'measuredProp': 'count',
              'measuredValue': 180,
              'measurementMethod': 'CenusACS5yrSurvey',
              'observationDate': '2012'
            }
          ],
          'popType': 'Person',
          'propertyValues': {
            'age': 'Years16Onwards',
            'gender': 'Male',
            'income': 'USDollar30000To34999',
            'incomeStatus': 'WithIncome',
            'race': 'USC_HispanicOrLatinoRace',
            'workExperience': 'USC_NotWorkedFullTime'
          }
        }
      }
    })

if __name__ == '__main__':
  unittest.main()
