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

Unit tests for core methods in the Data Commons Python Client API.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from pandas.util.testing import assert_series_equal, assert_frame_equal
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

  # Get the request json
  req = kwargs['json']
  headers = kwargs['headers']

  # If the API key does not match, then return 403 Forbidden
  if 'x-api-key' not in headers or headers['x-api-key'] != 'TEST-API-KEY':
    return MockResponse({}, 403)

  # Mock responses for post requests to get_property_labels.
  if args[0] == utils._API_ROOT + utils._API_ENDPOINTS['get_property_labels']:
    if req['dcids'] == ['geoId/0649670']:
      # Response for sending a single dcid to get_property_labels
      out_arcs = ['containedInPlace', 'name', 'geoId', 'typeOf']
      res_json = json.dumps({
        'geoId/0649670': {
          'inLabels': [],
          'outLabels': out_arcs
        }
      })
      return MockResponse({"payload": res_json}, 200)
    elif req['dcids'] == ['State', 'County', 'City']:
      # Response for sending multiple dcids to get_property_labels
      in_arcs = ['typeOf']
      out_arcs = ['name', 'provenance', 'subClassOf', 'typeOf', 'url']
      res_json = json.dumps({
        'City': {'inLabels': in_arcs, 'outLabels': out_arcs},
        'County': {'inLabels': in_arcs, 'outLabels': out_arcs},
        'State': {'inLabels': in_arcs, 'outLabels': out_arcs}
      })
      return MockResponse({'payload': res_json}, 200)
    elif req['dcids'] == ['dc/MadDcid']:
      # Response for sending a dcid that doesn't exist to get_property_labels
      res_json = json.dumps({
        'dc/MadDcid': {
          'inLabels': [],
          'outLabels': []
        }
      })
      return MockResponse({'payload': res_json}, 200)
    elif req['dcids'] == []:
      # Response for sending no dcids to get_property_labels
      res_json = json.dumps({})
      return MockResponse({'payload': res_json}, 200)

  # Mock responses for post requests to get_property_values
  if args[0] == utils._API_ROOT + utils._API_ENDPOINTS['get_property_values']:
    if req['dcids'] == ['geoId/06085', 'geoId/24031']\
      and req['property'] == 'containedInPlace'\
      and req['value_type'] == 'Town':
      # Response for sending a request for getting Towns containedInPlace of
      # Santa Clara County and Montgomery County.
      res_json = json.dumps({
        'geoId/06085': {
          'in': [
            {
              'dcid': 'geoId/0644112',
              'name': 'Los Gatos',
              'provenanceId': 'dc/sm3m2w3',
              'types': [
                'City',
                'Town'
              ]
            },
            {
              'dcid': 'geoId/0643294',
              'name': 'Los Altos Hills',
              'provenanceId': 'dc/sm3m2w3',
              'types': [
                'City',
                'Town'
              ]
            }
          ],
          'out': []
        },
        'geoId/24031': {
          'in': [
            {
              'dcid': 'geoId/2462850',
              'name': 'Poolesville',
              'provenanceId': 'dc/sm3m2w3',
              'types': [
                'City',
                'Town'
              ]
            },
          ],
          'out': []
        }
      })
      return MockResponse({'payload': res_json}, 200)
    if req['dcids'] == ['geoId/06085', 'geoId/24031']\
      and req['property'] == 'name':
      # Response for sending a request for the name of multiple dcids.
      res_json = json.dumps({
        'geoId/06085': {
          'in': [],
          'out': [
            {
              'value': 'Santa Clara County',
              'provenanceId': 'dc/sm3m2w3',
            },
          ]
        },
        'geoId/24031': {
          'in': [],
          'out': [
            {
              'value': 'Montgomery County',
              'provenanceId': 'dc/sm3m2w3',
            },
          ]
        }
      })
      return MockResponse({'payload': res_json}, 200)
    if req['dcids'] == ['geoId/06085', 'geoId/24031']\
      and req['property'] == 'madProperty':
      # Response for sending a request with a property that does not exist.
      res_json = json.dumps({
        'geoId/06085': {
          'in': [],
          'out': []
        },
        'geoId/24031': {
          'in': [],
          'out': []
        }
      })
      return MockResponse({'payload': res_json}, 200)
    if req['dcids'] == ['geoId/06085', 'dc/MadDcid']\
      and req['property'] == 'containedInPlace':
      # Response for sending a request with a single dcid that does not exist.
      res_json = json.dumps({
        'geoId/06085': {
          'in': [
            {
              'dcid': 'geoId/0644112',
              'name': 'Los Gatos',
              'provenanceId': 'dc/sm3m2w3',
              'types': [
                'City',
                'Town'
              ]
            },
          ],
          'out': []
        },
        'dc/MadDcid': {
          'in': [],
          'out': []
        }
      })
      return MockResponse({'payload': res_json}, 200)
    if req['dcids'] == ['dc/MadDcid', 'dc/MadderDcid']:
      # Response for sending a request where both dcids do not exist.
      res_json = json.dumps({
        'dc/MadDcid': {
          'in': [],
          'out': []
        },
        'dc/MadderDcid': {
          'in': [],
          'out': []
        }
      })
      return MockResponse({'payload': res_json}, 200)
    if req['dcids'] == [] and req['property'] == 'containedInPlace':
      # Response for sending a request where no dcids are given.
      res_json = json.dumps({})
      return MockResponse({'payload': res_json}, 200)

  # Mock responses for post requests to get_triples
  if args[0] == utils._API_ROOT + utils._API_ENDPOINTS['get_triples']:
    if req['dcids'] == ['geoId/06085', 'geoId/24031']:
      # Response for sending a request with two valid dcids.
      res_json = json.dumps({
        'geoId/06085': [
          {
            "subjectId": "geoId/06085",
            "predicate": "name",
            "objectValue": "Santa Clara County"
          },
          {
            "subjectId": "geoId/0649670",
            "subjectName": "Mountain View",
            "subjectTypes": [
              "City"
            ],
            "predicate": "containedInPlace",
            "objectId": "geoId/06085",
            "objectName": "Santa Clara County"
          },
          {
            "subjectId": "geoId/06085",
            "predicate": "containedInPlace",
            "objectId": "geoId/06",
            "objectName": "California"
          },
        ],
        'geoId/24031': [
          {
            "subjectId": "geoId/24031",
            "predicate": "name",
            "objectValue": "Montgomery County"
          },
          {
            "subjectId": "geoId/2467675",
            "subjectName": "Rockville",
            "subjectTypes": [
              "City"
            ],
            "predicate": "containedInPlace",
            "objectId": "geoId/24031",
            "objectName": "Montgomery County"
          },
          {
            "subjectId": "geoId/24031",
            "predicate": "containedInPlace",
            "objectId": "geoId/24",
            "objectName": "Maryland"
          },
        ]
      })
      return MockResponse({'payload': res_json}, 200)
    if req['dcids'] == ['geoId/06085', 'dc/MadDcid']:
      # Response for sending a request where one dcid does not exist.
      res_json = json.dumps({
        'geoId/06085': [
          {
            "subjectId": "geoId/06085",
            "predicate": "name",
            "objectValue": "Santa Clara County"
          },
          {
            "subjectId": "geoId/0649670",
            "subjectName": "Mountain View",
            "subjectTypes": [
              "City"
            ],
            "predicate": "containedInPlace",
            "objectId": "geoId/06085",
            "objectName": "Santa Clara County"
          },
          {
            "subjectId": "geoId/06085",
            "predicate": "containedInPlace",
            "objectId": "geoId/06",
            "objectName": "California"
          },
        ],
        'dc/MadDcid': []
      })
      return MockResponse({'payload': res_json}, 200)
    if req['dcids'] == ['dc/MadDcid', 'dc/MadderDcid']:
      # Response for sending a request where both dcids do not exist.
      res_json = json.dumps({
        'dc/MadDcid': [],
        'dc/MadderDcid': []
      })
      return MockResponse({'payload': res_json}, 200)
    if req['dcids'] == []:
      # Response for sending a request where no dcids are given.
      res_json = json.dumps({})
      return MockResponse({'payload': res_json}, 200)

  # Otherwise, return an empty response and a 404.
  return MockResponse({}, 404)


class TestGetPropertyLabels(unittest.TestCase):
  """ Unit tests for get_property_labels. """

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_single_dcid(self, post_mock):
    """ Calling get_property_labels with a single dcid returns a valid
    result.
    """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    # Test for outgoing property labels
    out_props = dc.get_property_labels(['geoId/0649670'])
    self.assertDictEqual(out_props,
      {'geoId/0649670': ["containedInPlace", "name", "geoId", "typeOf"]})

    # Test with out=False
    in_props = dc.get_property_labels(['geoId/0649670'], out=False)
    self.assertDictEqual(in_props, {'geoId/0649670': []})

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_multiple_dcids(self, post_mock):
    """ Calling get_property_labels returns valid results with multiple
    dcids.
    """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    dcids = ['State', 'County', 'City']
    expected_in = ["typeOf"]
    expected_out = ["name", "provenance", "subClassOf", "typeOf", "url"]

    # Test for outgoing property labels
    out_props = dc.get_property_labels(dcids)
    self.assertDictEqual(out_props, {
      'State': expected_out,
      'County': expected_out,
      'City': expected_out,
    })

    # Test for incoming property labels
    in_props = dc.get_property_labels(dcids, out=False)
    self.assertDictEqual(in_props, {
      'State': expected_in,
      'County': expected_in,
      'City': expected_in,
    })

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_bad_dcids(self, post_mock):
    """ Calling get_property_labels with dcids that do not exist returns empty
    results.
    """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    # Test for outgoing property labels
    out_props = dc.get_property_labels(['dc/MadDcid'])
    self.assertDictEqual(out_props, {'dc/MadDcid': []})

    # Test for incoming property labels
    in_props = dc.get_property_labels(['dc/MadDcid'], out=False)
    self.assertDictEqual(in_props, {'dc/MadDcid': []})

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_no_dcids(self, post_mock):
    """ Calling get_property_labels with no dcids returns empty results. """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    # Test for outgoing property labels
    out_props = dc.get_property_labels([])
    self.assertDictEqual(out_props, {})

    # Test for incoming property labels
    in_props = dc.get_property_labels([], out=False)
    self.assertDictEqual(in_props, {})


class TestGetPropertyValues(unittest.TestCase):
  """ Unit tests for get_property_values. """

  # --------------------------- STANDARD UNIT TESTS ---------------------------

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_multiple_dcids(self, post_mock):
    """ Calling get_property_values with multiple dcids returns valid
    results.
    """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    dcids = ['geoId/06085', 'geoId/24031']

    # Get the containedInPlace Towns for Santa Clara and Montgomery County.
    towns = dc.get_property_values(
      dcids, 'containedInPlace', out=False, value_type='Town')
    self.assertDictEqual(towns, {
      'geoId/06085': ['geoId/0643294', 'geoId/0644112'],
      'geoId/24031': ['geoId/2462850']
    })

    # Get the name of Santa Clara and Montgomery County.
    names = dc.get_property_values(dcids, 'name')
    self.assertDictEqual(names, {
      'geoId/06085': ['Santa Clara County'],
      'geoId/24031': ['Montgomery County']
    })

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_bad_dcids(self, post_mock):
    """ Calling get_property_values with dcids that do not exist returns empty
    results.
    """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    bad_dcids_1 = ['geoId/06085', 'dc/MadDcid']
    bad_dcids_2 = ['dc/MadDcid', 'dc/MadderDcid']

    # Get entities containedInPlace of Santa Clara County and a dcid that does
    # not exist.
    contained_1 = dc.get_property_values(bad_dcids_1, 'containedInPlace', out=False)
    self.assertDictEqual(contained_1, {
      'geoId/06085': ['geoId/0644112'],
      'dc/MadDcid': []
    })

    # Get entities containedInPlace for two dcids that do not exist.
    contained_2 = dc.get_property_values(bad_dcids_2, 'containedInPlace')
    self.assertDictEqual(contained_2, {
      'dc/MadDcid': [],
      'dc/MadderDcid': []
    })

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_bad_property(self, post_mock):
    """ Calling get_property_values with a property that does not exist returns
    empty results.
    """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    # Get propery values for a property that does not exist.
    prop_vals = dc.get_property_values(
      ['geoId/06085', 'geoId/24031'], 'madProperty')
    self.assertDictEqual(prop_vals, {
      'geoId/06085': [],
      'geoId/24031': []
    })

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_no_dcids(self, post_mock):
    """ Calling get_property_values with no dcids returns empty results. """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    # Get property values with an empty list of dcids.
    prop_vals = dc.get_property_values([], 'containedInPlace')
    self.assertDictEqual(prop_vals, {})

  # ---------------------------- PANDAS UNIT TESTS ----------------------------

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_series(self, post_mock):
    """ Calling get_property_values with a Pandas Series returns the correct
    results.
    """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    # The given and expected series.
    dcids = pd.Series(['geoId/06085', 'geoId/24031'])
    expected = pd.Series([
      ['geoId/0643294', 'geoId/0644112'],
      ['geoId/2462850']
    ])

    # Call get_property_values with the series as input
    actual = dc.get_property_values(
      dcids, 'containedInPlace', out=False, value_type='Town')
    assert_series_equal(actual, expected)

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_series_bad_dcids(self, post_mock):
    """ Calling get_property_values with a Pandas Series and dcids that does not
    exist resturns an empty result.
    """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    # The given and expected series
    bad_dcids_1 = pd.Series(['geoId/06085', 'dc/MadDcid'])
    bad_dcids_2 = pd.Series(['dc/MadDcid', 'dc/MadderDcid'])
    expected_1 = pd.Series([['geoId/0644112'], []])
    expected_2 = pd.Series([[], []])

    # Call get_property_values with series as input
    actual_1 = dc.get_property_values(bad_dcids_1, 'containedInPlace', out=False)
    actual_2 = dc.get_property_values(bad_dcids_2, 'containedInPlace', out=False)

    # Assert the results are correct
    assert_series_equal(actual_1, expected_1)
    assert_series_equal(actual_2, expected_2)

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_series_bad_property(self, post_mock):
    """ Calling get_property_values with a Pandas Series and a property that
    does not exist returns an empty result.
    """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    # The input and expected series
    dcids = pd.Series(['geoId/06085', 'geoId/24031'])
    expected = pd.Series([[], []])

    # Call get_property_values and assert the results are correct.
    actual = dc.get_property_values(dcids, 'madProperty')
    assert_series_equal(actual, expected)

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_series_no_dcid(self, post_mock):
    # The input and expected series
    dcids = pd.Series([])
    expected = pd.Series([])

    # Call get_property_values and assert the results are correct.
    actual = dc.get_property_values(dcids, 'containedInPlace')
    assert_series_equal(actual, expected)

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_dataframe(self, post_mock):
    """ Calling get_property_values with a Pandas DataFrame returns the correct
    results.
    """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    # The given and expected series.
    dcids = pd.DataFrame({'dcids': ['geoId/06085', 'geoId/24031']})
    expected = pd.Series([
      ['geoId/0643294', 'geoId/0644112'],
      ['geoId/2462850']
    ])

    # Call get_property_values with the series as input
    actual = dc.get_property_values(
        dcids, 'containedInPlace', out=False, value_type='Town')
    assert_series_equal(actual, expected)

class TestGetTriples(unittest.TestCase):
  """ Unit tests for get_triples. """

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_multiple_dcids(self, post_mock):
    """ Calling get_triples with proper dcids returns valid results. """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    # Call get_triples
    triples = dc.get_triples(['geoId/06085', 'geoId/24031'])
    self.assertDictEqual(triples, {
      'geoId/06085': [
        ('geoId/06085', 'name', 'Santa Clara County'),
        ('geoId/0649670', 'containedInPlace', 'geoId/06085'),
        ('geoId/06085', 'containedInPlace', 'geoId/06'),
      ],
      'geoId/24031': [
        ('geoId/24031', 'name', 'Montgomery County'),
        ('geoId/2467675', 'containedInPlace', 'geoId/24031'),
        ('geoId/24031', 'containedInPlace', 'geoId/24'),
      ]
    })

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_bad_dcids(self, post_mock):
    """ Calling get_triples with dcids that do not exist returns empty
    results.
    """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    # Call get_triples where one dcid does not exist
    triples_1 = dc.get_triples(['geoId/06085', 'dc/MadDcid'])
    self.assertDictEqual(triples_1, {
      'geoId/06085': [
        ('geoId/06085', 'name', 'Santa Clara County'),
        ('geoId/0649670', 'containedInPlace', 'geoId/06085'),
        ('geoId/06085', 'containedInPlace', 'geoId/06'),
      ],
      'dc/MadDcid': []
    })

    # Call get_triples where both dcids do not exist
    triples_1 = dc.get_triples(['dc/MadDcid', 'dc/MadderDcid'])
    self.assertDictEqual(triples_1, {
      'dc/MadDcid': [],
      'dc/MadderDcid': []
    })

  @mock.patch('requests.post', side_effect=post_request_mock)
  def test_no_dcids(self, post_mock):
    """ Calling get_triples with no dcids returns empty results. """
    # Set the API key
    dc.set_api_key('TEST-API-KEY')

    # Call get_triples with no dcids
    triples_1 = dc.get_triples([])
    self.assertDictEqual(triples_1, {})


if __name__ == '__main__':
  unittest.main()
