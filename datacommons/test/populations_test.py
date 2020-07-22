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

Unit tests for Population and Observation methods in the Data Commons Python
Client API.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import base64
from unittest import mock

import datacommons as dc
import datacommons.utils as utils
import json
import unittest
import urllib
import zlib


def request_mock(*args, **kwargs):
  """ A mock urlopen request sent in the requests package. """
  # Create the mock response object.
  class MockResponse:
    def __init__(self, json_data):
      self.json_data = json_data

    def read(self):
      return self.json_data

  # Get the request json and allowed constraining properties
  req = args[0]
  if req.data:
    data = json.loads(req.data)

  api_key = req.get_header('X-api-key')
  if api_key != 'TEST-API-KEY':
    return urllib.error.HTTPError(None, 403, None, None, None)

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

  # Mock responses for urlopen request to get_populations.
  if req.full_url == utils._API_ROOT + utils._API_ENDPOINTS['get_populations']\
    and data['population_type'] == 'Person'\
    and data['pvs'] == constrained_props:
    if data['dcids'] == ['geoId/06085', 'geoId/4805000']:
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
      return MockResponse(json.dumps({'payload': res_json}))
    if data['dcids'] == ['geoId/06085', 'dc/MadDcid']:
      # Response returned when querying for a dcid that does not exist.
      res_json = json.dumps([
        {
          'dcid': 'geoId/06085',
          'population': 'dc/p/crgfn8blpvl35'
        },
      ])
      return MockResponse(json.dumps({'payload': res_json}))
    if data['dcids'] == ['dc/MadDcid', 'dc/MadderDcid'] or data['dcids'] == []:
      # Response returned when both given dcids do not exist or no dcids are
      # provided to the method.
      res_json = json.dumps([])
      return MockResponse(json.dumps({'payload': res_json}))

  # Mock responses for urlopen request to get_observations
  if req.full_url == utils._API_ROOT + utils._API_ENDPOINTS['get_observations']\
    and data['measured_property'] == 'count'\
    and data['stats_type'] == 'measuredValue'\
    and data['observation_date'] == '2018-12'\
    and data['observation_period'] == 'P1M'\
    and data['measurement_method'] == 'BLSSeasonallyAdjusted':
    if data['dcids'] == ['dc/p/x6t44d8jd95rd', 'dc/p/lr52m1yr46r44', 'dc/p/fs929fynprzs']:
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
      return MockResponse(json.dumps({'payload': res_json}))
    if data['dcids'] == ['dc/p/x6t44d8jd95rd', 'dc/MadDcid']:
      # Response returned when querying for a dcid that does not exist.
      res_json = json.dumps([
        {
          'dcid': 'dc/p/x6t44d8jd95rd',
          'observation': '18704962.000000'
        },
      ])
      return MockResponse(json.dumps({'payload': res_json}))
    if data['dcids'] == ['dc/MadDcid', 'dc/MadderDcid'] or data['dcids'] == []:
      # Response returned when both given dcids do not exist or no dcids are
      # provided to the method.
      res_json = json.dumps([])
      return MockResponse(json.dumps({'payload': res_json}))

  # Mock responses for urlopen request to get_place_obs
  if req.full_url == utils._API_ROOT + utils._API_ENDPOINTS['get_place_obs']\
    and data['place_type'] == 'City'\
    and data['observation_date'] == '2017'\
    and data['population_type'] == 'Person'\
    and data['pvs'] == constrained_props:
    res_json = json.dumps({
      'places': [
        {
          'name': 'Marcus Hook borough',
          'place': 'geoId/4247344',
          'populations': {
            'dc/p/pq6frs32sfvk': {
              'observations': [
                {
                  'marginOfError': 39,
                  'measuredProp': 'count',
                  'measuredValue': 67,
                }
              ],
            }
          }
        }
      ]
    })
    return MockResponse(json.dumps(
      {'payload': base64.encodebytes(zlib.compress(res_json.encode('utf-8'))).decode('ascii')}))

  # Mock responses for get requests to get_pop_obs.
  if req.full_url == utils._API_ROOT + utils._API_ENDPOINTS['get_pop_obs'] + '?dcid=geoId/06085':
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
              'measurementMethod': 'CensusACS5yrSurvey',
              'observationDate': '2014'
            }, {
              'marginOfError': 108,
              'measuredProp': 'count',
              'measuredValue': 180,
              'measurementMethod': 'CensusACS5yrSurvey',
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
    return MockResponse(json.dumps(
      {'payload': base64.encodebytes(zlib.compress(res_json.encode('utf-8'))).decode('ascii')}))

  # Otherwise, return an empty response and a 404.
  return urllib.error.HTTPError(None, 404, None, None, None)

class TestGetPopulations(unittest.TestCase):
  """ Unit tests for get_populations. """

  _constraints = {
    'placeOfBirth': 'BornInOtherStateInTheUnitedStates',
    'age': 'Years5To17'
  }

  @mock.patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
  def test_multiple_dcids(self, urlopen):
    """ Calling get_populations with proper dcids returns valid results. """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    # Call get_populations
    populations = dc.get_populations(['geoId/06085', 'geoId/4805000'], 'Person',
                                     constraining_properties=self._constraints)
    self.assertDictEqual(populations, {
      'geoId/06085': 'dc/p/crgfn8blpvl35',
      'geoId/4805000': 'dc/p/f3q9whmjwbf36'
    })


  @mock.patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
  def test_bad_dcids(self, urlopen):
    """ Calling get_populations with dcids that do not exist returns empty
    results.
    """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    # Call get_populations
    pops_1 = dc.get_populations(['geoId/06085', 'dc/MadDcid'], 'Person',
                                constraining_properties=self._constraints)
    pops_2 = dc.get_populations(['dc/MadDcid', 'dc/MadderDcid'], 'Person',
                                constraining_properties=self._constraints)

    # Verify the results
    self.assertDictEqual(pops_1, {'geoId/06085': 'dc/p/crgfn8blpvl35'})
    self.assertDictEqual(pops_2, {})

  @mock.patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
  def test_no_dcids(self, urlopen):
    """ Calling get_populations with no dcids returns empty results. """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    pops = dc.get_populations(
      [], 'Person', constraining_properties=self._constraints)
    self.assertDictEqual(pops, {})

class TestGetObservations(unittest.TestCase):
  """ Unit tests for get_observations. """

  @mock.patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
  def test_multiple_dcids(self, urlopen):
    """ Calling get_observations with proper dcids returns valid results. """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

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

  @mock.patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
  def test_bad_dcids(self, urlopen):
    """ Calling get_observations with dcids that do not exist returns empty
    results.
    """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

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

  @mock.patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
  def test_no_dcids(self, urlopen):
    """ Calling get_observations with no dcids returns empty results. """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    actual = dc.get_observations([], 'count', 'measuredValue', '2018-12',
                                 observation_period='P1M',
                                 measurement_method='BLSSeasonallyAdjusted')
    self.assertDictEqual(actual, {})


class TestGetPopObs(unittest.TestCase):
  """ Unit tests for get_pop_obs. """

  @mock.patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
  def test_valid_dcid(self, urlopen):
    """ Calling get_pop_obs with valid dcid returns valid results. """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    # Call get_pop_obs
    pop_obs = dc.get_pop_obs('geoId/06085')
    self.assertDictEqual(pop_obs, {
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
              'measurementMethod': 'CensusACS5yrSurvey',
              'observationDate': '2014'
            }, {
              'marginOfError': 108,
              'measuredProp': 'count',
              'measuredValue': 180,
              'measurementMethod': 'CensusACS5yrSurvey',
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

class TestGetPlaceObs(unittest.TestCase):
  """ Unit tests for get_place_obs. """

  @mock.patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
  def test_valid(self, urlopen):
    """ Calling get_place_obs with valid parameters returns a valid result. """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    # Call get_place_obs
    pvs = {
      'placeOfBirth': 'BornInOtherStateInTheUnitedStates',
      'age': 'Years5To17'
    }
    place_obs = dc.get_place_obs(
      'City', '2017', 'Person', constraining_properties=pvs)
    self.assertListEqual(place_obs, [
      {
        'name': 'Marcus Hook borough',
        'place': 'geoId/4247344',
        'populations': {
          'dc/p/pq6frs32sfvk': {
            'observations': [
              {
                'marginOfError': 39,
                'measuredProp': 'count',
                'measuredValue': 67,
              }
            ],
          }
        }
      }
    ])


if __name__ == '__main__':
  unittest.main()
