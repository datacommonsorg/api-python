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
      by optional args.

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

    res_json = utils._send_request(url, post=False, use_payload=False)

    if 'value' not in res_json:
        raise ValueError('No data in response.')
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
      filtered by optional args.

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

    res_json = utils._send_request(url, post=False, use_payload=False)

    if 'series' not in res_json:
        raise ValueError('No data in response.')
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
    req_json = {'stat_vars': stat_vars, 'places': places}

    # Send the request
    res_json = utils._send_request(url, req_json=req_json, use_payload=False)

    if 'placeData' not in res_json:
        raise ValueError('No data in response.')

    # Unnest the REST response for keys that have single-element values.
    place_statvar_series = collections.defaultdict(dict)
    for place_dcid, place in res_json['placeData'].items():
        for stat_var_dcid, stat_var in place['statVarData'].items():
            place_statvar_series[place_dcid][stat_var_dcid] = stat_var
    return dict(place_statvar_series)


# Pandas Helpers
# These functions are wrapper functions that create Python data structures
# that are easily converted to Pandas DataFrames (and Series).


def _get_first_time_series(stat_var_data):
    """Helper function to return one time series."""
    return stat_var_data['sourceSeries'][0]['val']


def time_series_pd_input_options(places, stat_var):
    """Returns a `dict` mapping StatVarObservation options to `list` of `dict` of time series for each Place.
    """
    res = collections.defaultdict(list)
    stat_all = get_stat_all(places, [stat_var])
    for place, place_data in stat_all.items():
        if not place_data:
            continue
        stat_var_data = place_data[stat_var]
        if not stat_var_data:
            continue
        for source_series in stat_var_data['sourceSeries']:
            time_series = source_series['val']
            # Hashable SVO options.
            svo_options = (('measurementMethod',
                            source_series.get('measurementMethod')),
                           ('observationPeriod',
                            source_series.get('observationPeriod')),
                           ('unit', source_series.get('unit')),
                           ('scalingFactor',
                            source_series.get('scalingFactor')))
            res[svo_options].append(dict({'place': place}, **time_series))
    return dict(res)


def time_series_pd_input(places, stat_var):
    """Returns a `list` of `dict` per element of `places` based on the `stat_var`.

    Data Commons will pick a set of StatVarObservation options that covers the
    maximum number of queried places.

    Args:
      places (`str` or `iterable` of `str`): The dcids of Places to query for.
      stat_var (`str`): The dcid of the StatisticalVariable.
    Returns:
      A `list` of `dict`, one per element of `places`. Each `dict` consists of
      the time series and place identifier.

    Raises:
      ValueError: If the payload returned by the Data Commons REST API is
        malformed.

    Examples:
      >>> time_series_pd_input(["geoId/29", "geoId/33"], "Count_Person")
          [
            {'2020-03-07': 20, '2020-03-08': 40, 'place': 'geoId/29'},
            {'2020-08-21': 428, '2020-08-22': 429, 'place': 'geoId/33'}
          ]
    """
    try:
        if isinstance(places, six.string_types):
            places = [places]
        else:
            places = list(places)
    except:
        raise ValueError(
            'Parameter `places` must be a string object or list-like object.')
    if not isinstance(stat_var, six.string_types):
        raise ValueError('Parameter `stat_var` must be a string.')

    rows_dict = time_series_pd_input_options(places, stat_var)
    most_geos = []
    max_geos_so_far = 0
    latest_date = []
    max_date_so_far = ''
    for svo, rows in rows_dict.items():
        current_geos = len(rows)
        if current_geos > max_geos_so_far:
            max_geos_so_far = current_geos
            most_geos = [svo]
            # Reset tiebreaker stats. Recompute after this if-else block.
            latest_date = []
            max_date_so_far = ''
        elif current_geos == max_geos_so_far:
            most_geos.append(svo)
        else:
            # Do not compute tiebreaker stats if not in most_geos.
            continue
        for row in rows:
            dates = set(row.keys())
            dates.remove('place')
            row_max_date = max(dates)
            if row_max_date > max_date_so_far:
                max_date_so_far = row_max_date
                latest_date = [svo]
            elif row_max_date == max_date_so_far:
                latest_date.append(svo)
    for svo in most_geos:
        if svo in latest_date:
            return rows_dict[svo]
