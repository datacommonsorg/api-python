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

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

import datacommons as dc
import datacommons.utils as utils
import math
import json
import unittest
import six
import six.moves.urllib as urllib

# Reusable parts of REST API /stat/all response.
CA_COUNT_PERSON = {
    "isDcAggregate":
        "true",
    "sourceSeries": [{
        "val": {
            "1990": 23640,
            "1991": 24100,
            "1993": 25090,
        },
        "observationPeriod": "P1Y",
        "importName": "WorldDevelopmentIndicators",
        "provenanceDomain": "worldbank.org"
    }, {
        "val": {
            "1790": 3929214,
            "1800": 5308483,
            "1810": 7239881,
        },
        "measurementMethod": "WikidataPopulation",
        "importName": "WikidataPopulation",
        "provenanceDomain": "wikidata.org"
    }, {
        "val": {
            "1890": 28360,
            "1891": 24910,
            "1892": 25070,
        },
        "measurementMethod": "OECDRegionalStatistics",
        "observationPeriod": "P1Y",
        "importName": "OECDRegionalDemography",
        "provenanceDomain": "oecd.org"
    }]
}

CA_COUNT_PERSON_MALE = {
    "sourceSeries": [{
        "val": {
            "1990": 12000,
            "1991": 14000,
            "1992": 14000,
        },
        "measurementMethod": "WikidataPopulation",
        "importName": "WikidataPopulation",
        "provenanceDomain": "wikidata.org"
    },]
}

HU22_COUNT_PERSON = {
    "sourceSeries": [{
        "val": {
            "1990": 2360,
            "1991": 2410,
            "1992": 2500,
        },
        "measurementMethod": "OECDRegionalStatistics",
        "observationPeriod": "P1Y",
        "importName": "OECDRegionalDemography",
        "provenanceDomain": "oecd.org"
    }]
}

HU22_COUNT_PERSON_MALE = {
    "sourceSeries": [{
        "val": {
            "1990": 1360,
            "1991": 1410,
            "1992": 1500,
        },
        "measurementMethod": "OECDRegionalStatistics",
        "observationPeriod": "P1Y",
        "importName": "OECDRegionalDemography",
        "provenanceDomain": "oecd.org"
    }]
}

CA_MEDIAN_AGE_PERSON = {
    "sourceSeries": [{
        "val": {
            "1990": 12,
            "1991": 24,
            "1992": 24,
        },
        "measurementMethod": "WikidataPopulation",
        "importName": "WikidataPopulation",
        "provenanceDomain": "wikidata.org"
    }]
}


def request_mock(*args, **kwargs):
    """A mock urlopen requests sent in the requests package."""

    # Create the mock response object.
    class MockResponse:

        def __init__(self, json_data):
            self.json_data = json_data

        def read(self):
            return self.json_data

    req = args[0]

    stat_value_url_base = utils._API_ROOT + utils._API_ENDPOINTS[
        'get_stat_value']
    stat_series_url_base = utils._API_ROOT + utils._API_ENDPOINTS[
        'get_stat_series']
    stat_all_url_base = utils._API_ROOT + utils._API_ENDPOINTS['get_stat_all']

    # Mock responses for urlopen requests to get_stat_value.
    if req.get_full_url(
    ) == stat_value_url_base + '?place=geoId/06&stat_var=Count_Person':
        # Response returned when querying with basic args.
        return MockResponse(json.dumps({"value": 123}))
    if req.get_full_url(
    ) == stat_value_url_base + '?place=geoId/06&stat_var=Count_Person&date=2010':
        # Response returned when querying with observationDate.
        return MockResponse(json.dumps({"value": 133}))
    if (req.get_full_url() == stat_value_url_base +
            '?place=geoId/06&stat_var=Count_Person&' +
            'date=2010&measurement_method=CensusPEPSurvey&' +
            'observation_period=P1Y&unit=RealPeople&scaling_factor=100'):
        # Response returned when querying with above optional params.
        return MockResponse(json.dumps({"value": 103}))

    # Mock responses for urlopen requests to get_stat_series.
    if req.get_full_url(
    ) == stat_series_url_base + '?place=geoId/06&stat_var=Count_Person':
        # Response returned when querying with basic args.
        return MockResponse(json.dumps({"series": {"2000": 1, "2001": 2}}))
    if (req.get_full_url() == stat_series_url_base +
            '?place=geoId/06&stat_var=Count_Person&' +
            'measurement_method=CensusPEPSurvey&observation_period=P1Y&' +
            'unit=RealPeople&scaling_factor=100'):

        # Response returned when querying with above optional params.
        return MockResponse(json.dumps({"series": {"2000": 3, "2001": 42}}))
    if (req.get_full_url() == stat_series_url_base +
            '?place=geoId/06&stat_var=Count_Person&' +
            'measurement_method=DNE'):

        # Response returned when data not available for optional parameters.
        # /stat/series?place=geoId/06&stat_var=Count_Person&measurement_method=DNE
        return MockResponse(json.dumps({"series": {}}))

    # Mock responses for urlopen requests to get_stat_all.
    if req.get_full_url() == stat_all_url_base:
        data = json.loads(req.data)
        if (data['places'] == ['geoId/06', 'nuts/HU22'] and
                data['stat_vars'] == ['Count_Person', 'Count_Person_Male']):
            # Response returned when querying with above params.
            # Response with data for all Place+StatVar combos.
            full_resp = {
                "placeData": {
                    "geoId/06": {
                        "statVarData": {
                            "Count_Person": CA_COUNT_PERSON,
                            "Count_Person_Male": CA_COUNT_PERSON_MALE,
                        }
                    },
                    "nuts/HU22": {
                        "statVarData": {
                            "Count_Person": HU22_COUNT_PERSON,
                            "Count_Person_Male": HU22_COUNT_PERSON_MALE
                        }
                    }
                }
            }
            return MockResponse(json.dumps(full_resp))

        if (data['places'] == ['geoId/06', 'nuts/HU22'] and
                data['stat_vars'] == ['Count_Person', 'Median_Age_Person']):
            # Response returned when querying with above params.
            # Median Age missing for HU22.
            resp = {
                "placeData": {
                    "geoId/06": {
                        "statVarData": {
                            "Count_Person": CA_COUNT_PERSON,
                            "Median_Age_Person": CA_MEDIAN_AGE_PERSON
                        }
                    },
                    "nuts/HU22": {
                        "statVarData": {
                            "Count_Person": HU22_COUNT_PERSON,
                            "Median_Age_Person": {}
                        }
                    }
                }
            }
            return MockResponse(json.dumps(resp))

        if (data['places'] == ['badPlaceId', 'nuts/HU22'] and
                data['stat_vars'] == ['Count_Person', 'badStatVarId']):
            # Response returned when querying with above params.
            # Bad DCIDs for place or statvar.
            resp = {
                "placeData": {
                    "badPlaceId": {
                        "statVarData": {
                            "Count_Person": {},
                            "badStatVarId": {}
                        }
                    },
                    "nuts/HU22": {
                        "statVarData": {
                            "Count_Person": HU22_COUNT_PERSON,
                            "badStatVarId": {}
                        }
                    }
                }
            }
            return MockResponse(json.dumps(resp))

    # Otherwise, return an empty response and a 404.
    return urllib.error.HTTPError(None, 404, None, None, None)


class TestGetStatValue(unittest.TestCase):
    """Unit tests for get_stat_value."""

    @patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
    def test_basic(self, urlopen):
        """Calling get_stat_value with minimal and proper args."""
        # Call get_stat_value

        self.assertEqual(dc.get_stat_value('geoId/06', 'Count_Person'), 123)

    @patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
    def test_opt_args(self, urlopen):
        """Calling get_stat_value with optional args returns specific data."""
        # Call get_stat_value for specific obs
        self.assertEqual(dc.get_stat_value('geoId/06', 'Count_Person', '2010'),
                         133)

        # Call get_stat_value with all optional args
        stat = dc.get_stat_value('geoId/06', 'Count_Person', '2010',
                                 'CensusPEPSurvey', 'P1Y', 'RealPeople', 100)
        self.assertEqual(stat, 103)

        # Call get_stat_series with bogus required args
        stat = dc.get_stat_value('foofoo', 'barrbar')
        self.assertTrue(math.isnan(stat))

class TestGetStatSeries(unittest.TestCase):
    """Unit tests for get_stat_series."""

    @patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
    def test_basic(self, urlopen):
        """Calling get_stat_value with minimal and proper args."""
        # Call get_stat_series
        stats = dc.get_stat_series('geoId/06', 'Count_Person')
        self.assertEqual(stats, {"2000": 1, "2001": 2})

    @patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
    def test_opt_args(self, urlopen):
        """Calling get_stat_value with optional args returns specific data."""

        # Call get_stat_series with all optional args
        stats = dc.get_stat_series('geoId/06', 'Count_Person',
                                   'CensusPEPSurvey', 'P1Y', 'RealPeople', 100)
        self.assertEqual(stats, {"2000": 3, "2001": 42})

        # Call get_stat_series with bogus required args
        stats = dc.get_stat_series('foofoofoo', 'barfoobar')
        self.assertEqual(stats, {})

        # Call get_stat_series with non-satisfiable optional args
        stats = dc.get_stat_series('geoId/06', 'Count_Person', 'DNE')
        self.assertEqual(stats, {})


class TestGetStatAll(unittest.TestCase):
    """Unit tests for get_stat_all."""

    @patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
    def test_basic(self, urlopen):
        """Calling get_stat_all with proper args."""
        # Expecting at least one TS per Place+StatVar
        stats = dc.get_stat_all(['geoId/06', 'nuts/HU22'],
                                ['Count_Person', 'Count_Person_Male'])
        exp = {
            "geoId/06": {
                "Count_Person": CA_COUNT_PERSON,
                "Count_Person_Male": CA_COUNT_PERSON_MALE,
            },
            "nuts/HU22": {
                "Count_Person": HU22_COUNT_PERSON,
                "Count_Person_Male": HU22_COUNT_PERSON_MALE
            }
        }
        self.assertDictEqual(stats, exp)
        # Expecting proper handling of no TS for Place+StatVar combo
        stats = dc.get_stat_all(['geoId/06', 'nuts/HU22'],
                                ['Count_Person', 'Median_Age_Person'])
        exp = {
            "geoId/06": {
                "Count_Person": CA_COUNT_PERSON,
                "Median_Age_Person": CA_MEDIAN_AGE_PERSON
            },
            "nuts/HU22": {
                "Count_Person": HU22_COUNT_PERSON,
                "Median_Age_Person": {}
            }
        }
        self.assertDictEqual(stats, exp)

    @patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
    def test_bad_dcids(self, urlopen):
        stats = dc.get_stat_all(['badPlaceId', 'nuts/HU22'],
                                ['Count_Person', 'badStatVarId'])
        exp = {
            "badPlaceId": {
                "Count_Person": {},
                "badStatVarId": {}
            },
            "nuts/HU22": {
                "Count_Person": HU22_COUNT_PERSON,
                "badStatVarId": {}
            }
        }
        self.assertDictEqual(stats, exp)


if __name__ == '__main__':
    unittest.main()
