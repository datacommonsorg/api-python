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
""" Data Commons Python API unit tests.

Unit tests for Place methods in the Data Commons Python API.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

import datacommons as dc
import datacommons.utils as utils
import json
import unittest
import six.moves.urllib as urllib


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

  # Mock responses for urlopen requests to get_places_in.
  if req.get_full_url() == utils._API_ROOT + utils._API_ENDPOINTS['get_places_in']:
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
  if req.get_full_url() == utils._API_ROOT + utils._API_ENDPOINTS['get_stats']:
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
    if (data['place'] == ['geoId/00'] and
        data['stats_var'] == 'dc/0hyp6tkn18vcb'):
      # No data for the request
      res_json = json.dumps({
          'geoId/00': None
      })
      return MockResponse(json.dumps({'payload': res_json}))
    if ((data['place'] == ['geoId/05', 'dc/MadDcid'] or
         data['place'] == ['geoId/05']) and
        data['stats_var'] == 'dc/0hyp6tkn18vcb'):
      # Response ignores dcid that does not exist.
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
    if (data['place'] == ['geoId/06'] and
        data['stats_var'] == 'dc/0hyp6tkn18vcb'):
      res_json = json.dumps({
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
    if (data['place'] == ['dc/MadDcid', 'dc/MadderDcid'] and
        data['stats_var'] == 'dc/0hyp6tkn18vcb'):
      # Response returned when both given dcids do not exist.
      res_json = json.dumps({})
      return MockResponse(json.dumps({'payload': res_json}))
    if data['place'] == [] and data['stats_var'] == 'dc/0hyp6tkn18vcb':
      res_json = json.dumps({})
      # Response returned when no dcids are given.
      return MockResponse(json.dumps({'payload': res_json}))
    if (data['place'] == ['geoId/48'] and
      data['stats_var'] == 'dc/0hyp6tkn18vcb'):
      if (data.get('measurement_method') == 'MM1' and
          data.get('unit') == 'Inch' and
          data.get('observation_period') == 'P1Y'):
        res_json = json.dumps({
          'geoId/48': {
            'data': {
              '2015': 1,
              '2016': 1,
            },
            'place_name': 'Texas'
          }
        })
      elif data.get('measurement_method') == 'MM1':
        res_json = json.dumps({
          'geoId/48': {
            'data': {
              '2015': 2,
              '2016': 2,
            },
            'place_name': 'Texas'
          }
        })
      else:
        res_json = json.dumps({
          'geoId/48': {
            'data': {
              '2015': 3,
              '2016': 3,
            },
            'place_name': 'Texas'
          }
        })

    return MockResponse(json.dumps({'payload': res_json}))

  # Otherwise, return an empty response and a 404.
  return urllib.error.HTTPError

class TestGetPlacesIn(unittest.TestCase):
  """ Unit stests for get_places_in. """

  @patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
  def test_multiple_dcids(self, urlopen):
    """ Calling get_places_in with proper dcids returns valid results. """
    # Call get_places_in
    places = dc.get_places_in(['geoId/06085', 'geoId/24031'], 'City')
    self.assertDictEqual(places, {
      'geoId/06085': ['geoId/0649670'],
      'geoId/24031': ['geoId/2467675', 'geoId/2476650']
    })

  @patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
  def test_bad_dcids(self, urlopen):
    """ Calling get_places_in with dcids that do not exist returns empty
      results.
    """
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

  @patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
  def test_no_dcids(self, urlopen):
    """ Calling get_places_in with no dcids returns empty results. """
    # Call get_places_in with no dcids.
    bad_dcids = dc.get_places_in(['dc/MadDcid', 'dc/MadderDcid'], 'City')
    self.assertDictEqual(bad_dcids, {
      'dc/MadDcid': [],
      'dc/MadderDcid': []
    })


class TestGetStats(unittest.TestCase):
  """ Unit stests for get_stats. """

  @patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
  def test_multiple_dcids(self, urlopen):
    """ Calling get_stats with proper dcids returns valid results. """
    # Call get_stats
    stats = dc.get_stats(['geoId/05', 'geoId/06'], 'dc/0hyp6tkn18vcb', 'all')
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

    # Call get_stats for latest obs
    stats = dc.get_stats(['geoId/05', 'geoId/06'], 'dc/0hyp6tkn18vcb', 'latest')
    self.assertDictEqual(
        stats, {
            'geoId/05': {
                'data': {
                    '2018': 18003
                },
                'place_name': 'Arkansas'
            },
            'geoId/06': {
                'data': {
                    '2018': 366331
                },
                'place_name': 'California'
            }
        })

    # Call get_stats for specific obs
    stats = dc.get_stats(['geoId/05', 'geoId/06'], 'dc/0hyp6tkn18vcb', ['2013', '2018'])
    self.assertDictEqual(
        stats, {
            'geoId/05': {
                'data': {
                    '2013': 17459,
                    '2018': 18003
                },
                'place_name': 'Arkansas'
            },
            'geoId/06': {
                'data': {
                    '2013': 331853,
                    '2018': 366331
                },
                'place_name': 'California'
            }
        })

    # Call get_stats -- dates must be in interable
    stats = dc.get_stats(['geoId/05', 'geoId/06'], 'dc/0hyp6tkn18vcb', '2018')
    self.assertDictEqual(
        stats, {
            'geoId/05': {
                'data': {
                },
                'place_name': 'Arkansas'
            },
            'geoId/06': {
                'data': {
                },
                'place_name': 'California'
            }
        })

  @patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
  def test_opt_args(self, urlopen):
    """ Calling get_stats with mmethod, unit, and obs period returns specific data.
    """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    # Call get_stats with all optional args
    stats = dc.get_stats(['geoId/48'], 'dc/0hyp6tkn18vcb', 'latest', 'MM1',
                         'Inch', 'P1Y')
    self.assertDictEqual(
      stats, {
        'geoId/48': {
          'data': {
            '2016': 1
          },
          'place_name': 'Texas'
        }
      })

    # Call get_stats with mmethod specified
    stats = dc.get_stats(['geoId/48'], 'dc/0hyp6tkn18vcb', 'latest', 'MM1')
    self.assertDictEqual(
      stats, {
        'geoId/48': {
          'data': {
            '2016': 2
          },
          'place_name': 'Texas'
        }
      })

    # Call get_stats without optional args
    stats = dc.get_stats(['geoId/48'], 'dc/0hyp6tkn18vcb', 'latest')
    self.assertDictEqual(
      stats, {
        'geoId/48': {
          'data': {
            '2016': 3
          },
          'place_name': 'Texas'
        }
      })

  @patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
  def test_bad_dcids(self, urlopen):
    """ Calling get_stats with dcids that do not exist returns empty
      results.
    """
    # Call get_stats with one dcid that does not exist
    bad_dcids_1 = dc.get_stats(['geoId/05', 'dc/MadDcid'], 'dc/0hyp6tkn18vcb')
    self.assertDictEqual(
        bad_dcids_1, {
            'geoId/05': {
                'data': {
                    '2018': 18003
                },
                'place_name': 'Arkansas'
            }
        })

    # Call get_stats when both dcids do not exist
    bad_dcids_2 = dc.get_stats(['dc/MadDcid', 'dc/MadderDcid'],
                               'dc/0hyp6tkn18vcb')
    self.assertDictEqual({}, bad_dcids_2)

  @patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
  def test_no_dcids(self, urlopen):
    """ Calling get_stats with no dcids returns empty results. """
    # Call get_stats with no dcids.
    no_dcids = dc.get_stats([], 'dc/0hyp6tkn18vcb')
    self.assertDictEqual({}, no_dcids)

  @patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
  def test_no_data(self, urlopen):
    """ Calling get_stats with for None data. """
    # Call get_stats with no dcids.
    result = dc.get_stats(['geoId/00'], 'dc/0hyp6tkn18vcb')
    self.assertDictEqual({}, result)

  @patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
  def test_batch_request(self, mock_urlopen):
    """ Make multiple calls to REST API when number of geos exceeds the batch size. """
    save_batch_size = dc.utils._QUERY_BATCH_SIZE
    dc.utils._QUERY_BATCH_SIZE = 1

    self.assertEqual(0, mock_urlopen.call_count)
    stats = dc.get_stats(['geoId/05'], 'dc/0hyp6tkn18vcb', 'latest')
    self.assertDictEqual(
        stats, {
            'geoId/05': {
                'data': {
                    '2018': 18003
                },
                'place_name': 'Arkansas'
            },
        })
    self.assertEqual(1, mock_urlopen.call_count)

    stats = dc.get_stats(['geoId/05', 'geoId/06'], 'dc/0hyp6tkn18vcb', 'latest')
    self.assertDictEqual(
        stats, {
            'geoId/05': {
                'data': {
                    '2018': 18003
                },
                'place_name': 'Arkansas'
            },
            'geoId/06': {
                'data': {
                    '2018': 366331
                },
                'place_name': 'California'
            }
        })
    self.assertEqual(3, mock_urlopen.call_count)

    dc.utils._QUERY_BATCH_SIZE = save_batch_size


if __name__ == '__main__':
  unittest.main()
