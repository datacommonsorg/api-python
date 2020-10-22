# Copyright 2020 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Data Commons Python API Stat Module.

Provides functions for getting data on StatisticalVariables from Data Commons Graph.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import six

import datacommons.utils as utils

# stat_var specific batch size.
_STAT_BATCH_SIZE = 2000


def get_stat_value(place,
                   stat_var,
                   date=None,
                   measurement_method=None,
                   observation_period=None,
                   unit=None,
                   scaling_factor=None):
    """Returns a value for `place` based on the `stat_var`.

    Args:
      place (`str`): The dcid of Place to query for.
      stat_var (`str`): The dcid of the StatisticalVariable.
      date (`str`): Optional, the preferred date of observation
        in ISO 8601 format. If not specified, returns the latest observation.
      measurement_method (`str`): Optional, the dcid of the preferred
        `measurementMethod` value.
      observation_period (`str`): Optional, the preferred
        `observationPeriod` value.
      unit (`str`): Optional, the dcid of the preferred `unit` value.
      scaling_factor (`int`): Optional, the preferred `scalingFactor` value.
    Returns:
      A `float` the value of `stat_var` for `place`, filtered
      by optional args. If no data, returns nan.

    Raises:
      ValueError: If the payload returned by the Data Commons REST API is
        malformed.

    Examples:
      >>> get_stat_value("geoId/05", "Count_Person")
          366331
    """
    url = utils._API_ROOT + utils._API_ENDPOINTS['get_stat_value']
    url += '?place={}&stat_var={}'.format(place, stat_var)
    if date:
        url += '&date={}'.format(date)
    if measurement_method:
        url += '&measurement_method={}'.format(measurement_method)
    if observation_period:
        url += '&observation_period={}'.format(observation_period)
    if unit:
        url += '&unit={}'.format(unit)
    if scaling_factor:
        url += '&scaling_factor={}'.format(scaling_factor)

    try:
        res_json = utils._send_request(url, post=False, use_payload=False)
    except ValueError:
        return float('nan')
    if 'value' not in res_json:
        return float('nan')
    return res_json['value']


def get_stat_series(place,
                    stat_var,
                    measurement_method=None,
                    observation_period=None,
                    unit=None,
                    scaling_factor=None):
    """Returns a `dict` mapping dates to value of `stat_var` for `place`.

    Args:
      place (`str`): The dcid of Place to query for.
      stat_var (`str`): The dcid of the StatisticalVariable.
      measurement_method (`str`): Optional, the dcid of the preferred
        `measurementMethod` value.
      observation_period (`str`): Optional, the preferred
        `observationPeriod` value.
      unit (`str`): Optional, the dcid of the preferred `unit` value.
      scaling_factor (`int`): Optional, the preferred `scalingFactor` value.
    Returns:
      A `dict` mapping dates to value of `stat_var` for `place`,
      representing a time series that satisfies all input parameters.

    Raises:
      ValueError: If the payload returned by the Data Commons REST API is
        malformed.

    Examples:
      >>> get_stat_series("geoId/05", "Count_Person")
          {"1962":17072000,"2009":36887615,"1929":5531000,"1930":5711000}
    """
    url = utils._API_ROOT + utils._API_ENDPOINTS['get_stat_series']
    url += '?place={}&stat_var={}'.format(place, stat_var)
    if measurement_method:
        url += '&measurement_method={}'.format(measurement_method)
    if observation_period:
        url += '&observation_period={}'.format(observation_period)
    if unit:
        url += '&unit={}'.format(unit)
    if scaling_factor:
        url += '&scaling_factor={}'.format(scaling_factor)
    
    try:
        res_json = utils._send_request(url, post=False, use_payload=False)
    except ValueError:
        return {}

    if 'series' not in res_json:
        return {}
    return res_json['series']


def get_stat_all(places, stat_vars):
    """Returns a nested `dict` of all time series for `places` and `stat_vars`.

    Args:
      places (`Iterable` of `str`): The dcids of Places to query for.
      stat_vars (`Iterable` of `str`): The dcids of the StatisticalVariables.
    Returns:
      A nested `dict` mapping Places to StatisticalVariables and all available
      time series for each Place and StatisticalVariable pair.

    Raises:
      ValueError: If the payload returned by the Data Commons REST API is
        malformed.

    Examples:
      >>> get_stat_all(["geoId/05", "geoId/06"], ["Count_Person", "Count_Person_Male"])
      {
        "geoId/05": {
          "Count_Person": {
            "sourceSeries": [
              {
                "val": {
                  "2010": 1633,
                  "2011": 1509,
                  "2012": 1581,
                },
                "observationPeriod": "P1Y",
                "importName": "Wikidata",
                "provenanceDomain": "wikidata.org"
              },
              {
                "val": {
                  "2010": 1333,
                  "2011": 1309,
                  "2012": 131,
                },
                "observationPeriod": "P1Y",
                "importName": "CensusPEPSurvey",
                "provenanceDomain": "census.gov"
              }
            ],
            }
          },
          "Count_Person_Male": {
            "sourceSeries": [
              {
                "val": {
                  "2010": 1633,
                  "2011": 1509,
                  "2012": 1581,
                },
                "observationPeriod": "P1Y",
                "importName": "CensusPEPSurvey",
                "provenanceDomain": "census.gov"
              }
            ],
          }
        },
        "geoId/02": {
          "Count_Person": {},
          "Count_Person_Male": {
              "sourceSeries": [
                {
                  "val": {
                    "2010": 13,
                    "2011": 13,
                    "2012": 322,
                  },
                  "observationPeriod": "P1Y",
                  "importName": "CensusPEPSurvey",
                  "provenanceDomain": "census.gov"
                }
              ]
            }
        }
      }
    """
    url = utils._API_ROOT + utils._API_ENDPOINTS['get_stat_all']
    # Cast iterable-like to list.
    places = list(places)
    stat_vars = list(stat_vars)

    # Aiming for _STAT_BATCH_SIZE entries total.
    # _STAT_BATCH_SIZE = num places x num stat_vars, so aim for
    # _STAT_BATCH_SIZE/len(stat_vars) places per batch.
    places_per_batch = _STAT_BATCH_SIZE // len(stat_vars)
    # Get number of batches via an arithmetic ceiling trick:
    # 11//10 rounds down to 1.
    # -11//10 rounds down to -2.
    # We can divide with, then remove the negative to get the ceiling.
    batches = -(-len(places) // places_per_batch)
    res = {}
    for i in range(batches):
        req_json = {
            'stat_vars': stat_vars,
            'places': places[i * places_per_batch:(i + 1) * places_per_batch]
        }
        # Send the request
        res_json = utils._send_request(url,
                                       req_json=req_json,
                                       use_payload=False)
        if 'placeData' not in res_json:
            # The REST API spec will always return a dictionary under
            # placeData, even if no places exist or have no
            # data. If no Places are provided, REST will return an
            # error, which will have been caught and passed on in
            # _send_request.
            raise ValueError("Unexpected response from REST stat/all API.")

        # Unnest the REST response for keys that have single-element values.
        place_statvar_series = collections.defaultdict(dict)
        for place_dcid, place in res_json['placeData'].items():
            stat_var_data = place.get('statVarData')
            if not stat_var_data:
                # The REST API spec will always return a dictionary under
                # statVarData, even if no StatVars exist or have no
                # data. If no StatVars are provided, REST will return an
                # error, which will have been caught and passed on in
                # _send_request.
                raise ValueError("Unexpected response from REST stat/all API.")
            for stat_var_dcid, stat_var in stat_var_data.items():
                place_statvar_series[place_dcid][stat_var_dcid] = stat_var
        res.update(dict(place_statvar_series))

    return res
