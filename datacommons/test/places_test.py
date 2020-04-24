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

from unittest import mock

import datacommons as dc
import datacommons.utils as utils
import json
import unittest
import urllib


def request_mock(*args, **kwargs):
  """ A mock urlopen requests sent in the requests package. """
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

  # Mock responses for urlopen requests to get_places_in.
  if req.full_url == utils._API_ROOT + utils._API_ENDPOINTS['get_places_in']:
    if (data['dcids'] == ['geoId/06085', 'geoId/24031']
      and data['place_type'] == 'City'):
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
      return MockResponse(json.dumps({'payload': res_json}))
    if (data['dcids'] == ['geoId/06085', 'dc/MadDcid']
      and data['place_type'] == 'City'):
      # Response returned when querying for a dcid that does not exist.
      res_json = json.dumps([
        {
          'dcid': 'geoId/06085',
          'place': 'geoId/0649670',
        },
      ])
      return MockResponse(json.dumps({'payload': res_json}))
    if data['dcids'] == ['dc/MadDcid', 'dc/MadderDcid']\
      and data['place_type'] == 'City':
      # Response returned when both given dcids do not exist.
      res_json = json.dumps([])
      return MockResponse(json.dumps({'payload': res_json}))
    if data['dcids'] == [] and data['place_type'] == 'City':
      res_json = json.dumps([])
      # Response returned when no dcids are given.
      return MockResponse(json.dumps({'payload': res_json}))


  # Mock responses for urlopen requests to get_stats.
  if req.full_url == utils._API_ROOT + utils._API_ENDPOINTS['get_stats']:
    if (data['place'] == ['geoId/05', 'geoId/06'] and
        data['stats_var'] == 'dc/0hyp6tkn18vcb'):
      # Response returned when querying for multiple valid dcids.
      res_json = json.dumps({
          'geoId/05': {
              'data': {
                  '2011': 18136,
                  '2012': 17279,
                  '2013': 17459,
                  '2014': 16966,
                  '2015': 17173,
                  '2016': 17041,
                  '2017': 17783,
                  '2018': 18003
              },
              'place_name': 'Arkansas'
          },
          'geoId/06': {
              'data': {
                  '2011': 316667,
                  '2012': 324116,
                  '2013': 331853,
                  '2014': 342818,
                  '2015': 348979,
                  '2016': 354806,
                  '2017': 360645,
                  '2018': 366331
              },
              'place_name': 'California'
          }
      })
      return MockResponse(json.dumps({'payload': res_json}))
    if (data['place'] == ['geoId/05', 'dc/MadDcid'] and
        data['stats_var'] == 'dc/0hyp6tkn18vcb'):
      # Response returned when querying for a dcid that does not exist.
      res_json = json.dumps({
          'geoId/05': {
              'data': {
                  '2011': 18136,
                  '2012': 17279,
                  '2013': 17459,
                  '2014': 16966,
                  '2015': 17173,
                  '2016': 17041,
                  '2017': 17783,
                  '2018': 18003
              },
              'place_name': 'Arkansas'
          }
      })
      return MockResponse(json.dumps({'payload': res_json}))
    if (data['place'] == ['dc/MadDcid', 'dc/MadderDcid'] and
        data['stats_var'] == 'dc/0hyp6tkn18vcb'):
      # Response returned when both given dcids do not exist.
      res_json = json.dumps([])
      return MockResponse(json.dumps({'payload': res_json}))
    if data['place'] == [] and data['stats_var'] == 'dc/0hyp6tkn18vcb':
      res_json = json.dumps([])
      # Response returned when no dcids are given.
      return MockResponse(json.dumps({'payload': res_json}))

  # Otherwise, return an empty response and a 404.
  return urllib.error.HTTPError

class TestGetPlacesIn(unittest.TestCase):
  """ Unit stests for get_places_in. """

  @mock.patch('urllib.request.urlopen', side_effect=request_mock)
  def test_multiple_dcids(self, urlopen):
    """ Calling get_places_in with proper dcids returns valid results. """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    # Call get_places_in
    places = dc.get_places_in(['geoId/06085', 'geoId/24031'], 'City')
    self.assertDictEqual(places, {
      'geoId/06085': ['geoId/0649670'],
      'geoId/24031': ['geoId/2467675', 'geoId/2476650']
    })

  @mock.patch('urllib.request.urlopen', side_effect=request_mock)
  def test_bad_dcids(self, urlopen):
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

  @mock.patch('urllib.request.urlopen', side_effect=request_mock)
  def test_no_dcids(self, urlopen):
    """ Calling get_places_in with no dcids returns empty results. """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    # Call get_places_in with no dcids.
    bad_dcids = dc.get_places_in(['dc/MadDcid', 'dc/MadderDcid'], 'City')
    self.assertDictEqual(bad_dcids, {
      'dc/MadDcid': [],
      'dc/MadderDcid': []
    })


class TestGetStats(unittest.TestCase):
  """ Unit stests for get_stats. """

  @mock.patch('urllib.request.urlopen', side_effect=request_mock)
  def test_multiple_dcids(self, urlopen):
    """ Calling get_stats with proper dcids returns valid results. """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    # Call get_stats
    stats = dc.get_stats(['geoId/05', 'geoId/06'], 'dc/0hyp6tkn18vcb')
    self.assertDictEqual(
        stats, {
            'geoId/05': {
                'data': {
                    '2011': 18136,
                    '2012': 17279,
                    '2013': 17459,
                    '2014': 16966,
                    '2015': 17173,
                    '2016': 17041,
                    '2017': 17783,
                    '2018': 18003
                },
                'place_name': 'Arkansas'
            },
            'geoId/06': {
                'data': {
                    '2011': 316667,
                    '2012': 324116,
                    '2013': 331853,
                    '2014': 342818,
                    '2015': 348979,
                    '2016': 354806,
                    '2017': 360645,
                    '2018': 366331
                },
                'place_name': 'California'
            }
        })

  @mock.patch('urllib.request.urlopen', side_effect=request_mock)
  def test_bad_dcids(self, urlopen):
    """ Calling get_stats with dcids that do not exist returns empty
      results.
    """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    # Call get_stats with one dcid that does not exist
    bad_dcids_1 = dc.get_stats(['geoId/05', 'dc/MadDcid'], 'dc/0hyp6tkn18vcb')
    self.assertDictEqual(
        bad_dcids_1, {
            'geoId/05': {
                'data': {
                    '2011': 18136,
                    '2012': 17279,
                    '2013': 17459,
                    '2014': 16966,
                    '2015': 17173,
                    '2016': 17041,
                    '2017': 17783,
                    '2018': 18003
                },
                'place_name': 'Arkansas'
            }
        })

    # Call get_stats when both dcids do not exist
    bad_dcids_2 = dc.get_stats(['dc/MadDcid', 'dc/MadderDcid'],
                               'dc/0hyp6tkn18vcb')
    self.assertFalse(bad_dcids_2)

  @mock.patch('urllib.request.urlopen', side_effect=request_mock)
  def test_no_dcids(self, urlopen):
    """ Calling get_stats with no dcids returns empty results. """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    # Call get_stats with no dcids.
    no_dcids = dc.get_stats([], 'dc/0hyp6tkn18vcb')
    self.assertFalse(no_dcids)


if __name__ == '__main__':
  unittest.main()
