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

Unit tests for StatVar methods in the Data Commons Pandas API.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

import datacommons_pandas.df_builder as dcpd
import datacommons_pandas.utils as utils
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

        if (data['places'] == ['geoId/06', 'nuts/HU22'] and
                data['stat_vars'] == ['Count_Person']):
            # Response returned when querying with above params.
            resp = {
                "placeData": {
                    "geoId/06": {
                        "statVarData": {
                            "Count_Person": CA_COUNT_PERSON,
                        }
                    },
                    "nuts/HU22": {
                        "statVarData": {
                            "Count_Person": HU22_COUNT_PERSON,
                        }
                    }
                }
            }
            return MockResponse(json.dumps(resp))

        if (data['places'] == ['geoId/06'] and
                data['stat_vars'] == ['Count_Person']):
            # Response returned when querying with above params.
            resp = {
                "placeData": {
                    "geoId/06": {
                        "statVarData": {
                            "Count_Person": CA_COUNT_PERSON,
                        }
                    }
                }
            }
            return MockResponse(json.dumps(resp))

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
    # Otherwise, return an empty response and a 404.
    return urllib.error.HTTPError


class TestPdTimeSeries(unittest.TestCase):
    """Unit tests for _time_series_pd_input."""

    @patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
    def test_basic(self, urlopen):
        """Calling _time_series_pd_input with proper args."""
        rows = dcpd._time_series_pd_input(['geoId/06', 'nuts/HU22'],
                                          'Count_Person')
        exp = [{
            "1890": 28360,
            "1891": 24910,
            "1892": 25070,
            "place": "geoId/06"
        }, {
            "1991": 2410,
            "1990": 2360,
            "1992": 2500,
            "place": "nuts/HU22"
        }]
        six.assertCountEqual(self, rows, exp)

    @patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
    def test_one_place(self, urlopen):
        """Calling _time_series_pd_input with single place."""
        rows = dcpd._time_series_pd_input(['geoId/06'], 'Count_Person')
        exp = [{
            "1990": 23640,
            "1991": 24100,
            "1993": 25090,
            "place": "geoId/06"
        }]
        self.assertEqual(rows, exp)


class TestPdCovariates(unittest.TestCase):
    """Unit tests for _covariate_pd_input."""

    @patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
    def test_basic(self, urlopen):
        """Calling _covariate_pd_input with proper args."""
        rows = dcpd._covariate_pd_input(['geoId/06', 'nuts/HU22'],
                                        ['Count_Person', 'Median_Age_Person'])
        exp = [{
            "place": "geoId/06",
            "Median_Age_Person": 24,
            "Count_Person": 25070
        }, {
            "place": "nuts/HU22",
            "Count_Person": 2500
        }]
        six.assertCountEqual(self, rows, exp)

    @patch('six.moves.urllib.request.urlopen', side_effect=request_mock)
    def test_one_each(self, urlopen):
        """Calling _covariate_pd_input with single place and var."""
        rows = dcpd._covariate_pd_input(['geoId/06'], ['Count_Person'])
        exp = [{"place": "geoId/06", "Count_Person": 25090}]
        self.assertEqual(rows, exp)


if __name__ == '__main__':
    unittest.main()
