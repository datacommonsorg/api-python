# Copyright 2017 Google Inc.
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
""" Data Commons Python API Places Module.

Provides convenience functions for working with Places in the Data Commons
Graph. This submodule implements the ability to access :obj:`Place`'s
within a collection of nodes identified by dcid.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datacommons.utils as utils


def get_places_in(dcids, place_type):
  """ Returns :obj:`Place`s contained in :code:`dcids` of type
    :code:`place_type`.

  Args:
    dcids (:obj:`iterable` of :obj:`str`): Dcids to get contained in places.
    place_type (:obj:`str`): The type of places contained in the given dcids to
    filter by.

  Returns:
    The returned :obj:`Place`'s are formatted as a :obj:`dict` from a given
    dcid to a list of places identified by dcids of the given `place_type`.

  Raises:
    ValueError: If the payload returned by the Data Commons REST API is
    malformed.

  Examples:
    We would like to get all Counties contained in
    `California <https://browser.datacommons.org/kg?dcid=geoId/06>`_. Specifying
    the :code:`dcids` as a :obj:`list` result in the following.

    >>> get_places_in(["geoId/06"], "County")
    {
      'geoId/06': [
        'geoId/06041',
        'geoId/06089',
        'geoId/06015',
        'geoId/06023',
        'geoId/06067',
        ...
        # and 53 more
      ]
    }
  """
  dcids = filter(lambda v: v==v, dcids)  # Filter out NaN values
  dcids = list(dcids)
  url = utils._API_ROOT + utils._API_ENDPOINTS['get_places_in']
  payload = utils._send_request(url, req_json = {
    'dcids': dcids,
    'place_type': place_type,
  })

  # Create the results and format it appropriately
  result = utils._format_expand_payload(payload, 'place', must_exist=dcids)
  return result

def get_stats(dcids, stats_var, obs_dates='latest', measurement_method=None,
              unit=None, obs_period=None):
  """ Returns :obj:`TimeSeries` for :code:`dcids` \
    based on the :code:`stats_var`.

  Args:
    dcids (:obj:`iterable` of :obj:`str`): Dcids of places to query for.
    stats_var (:obj:`str`): The dcid of the :obj:StatisticalVariable.
    obs_dates (:obj:`str` or :obj:`iterable` of :obj:`str`):
      Which observation to return.
      Can be 'latest', 'all', or an iterable of dates in 'YYYY-MM-DD' format.
    measurement_method (:obj:`str`): Optional, the dcid of the preferred
      `measurementMethod` value.
    unit (:obj:`str`): Optional, the dcid of the preferred `unit` value.
    obs_period (:obj:`str`): Optional, the dcid of the preferred
      `observationPeriod` value.
  Returns:
    A :obj:`dict` mapping the :obj:`Place` identified by the given :code:`dcid`
    to its place name and the :obj:`TimeSeries` associated with the
    :obj:`StatisticalVariable` identified by the given :code:`stats_var`
    and filtered by :code:`obs_dates` and optional args.
    See example below for more detail about how the returned :obj:`dict` is
    structured.

  Raises:
    ValueError: If the payload returned by the Data Commons REST API is
      malformed.

  Examples:
    We would like to get the :obj:`TimeSeries` of the number of males
    at least 25 years old that attended 12th grade but did not receive
    a high school diploma
    (`dc/0hyp6tkn18vcb <https://browser.datacommons.org/kg?dcid=dc/0hyp6tkn18vcb>`_)
    in `Arkansas <https://browser.datacommons.org/kg?dcid=geoId/05>`_
    and `California <https://browser.datacommons.org/kg?dcid=geoId/06>`_.

    >>> get_stats(["geoId/05", "geoId/06"], "dc/0hyp6tkn18vcb")
    {
      'geoId/05': {
        'place_name': 'Arkansas'
        'data': {
          '2011':18136,
          '2012':17279,
          '2013':17459,
          '2014':16966,
          '2015':17173,
          '2016':17041,
          '2017':17783,
          '2018':18003
        },
      },
      'geoId/05': {
        'place_name': 'California'
        'data': {
          '2011':316667,
          '2012':324116,
          '2013':331853,
          '2014':342818,
          '2015':348979,
          '2016':354806,
          '2017':360645,
          '2018':366331
        },
      },
    }
  """
  dcids = filter(lambda v: v==v, dcids)  # Filter out NaN values
  dcids = list(dcids)
  url = utils._API_ROOT + utils._API_ENDPOINTS['get_stats']
  batches =  -(-len(dcids) // utils._QUERY_BATCH_SIZE)  # Ceil to get # of batches.
  res = {}
  for i in range(batches):
    req_json = {
      'place': dcids[i * utils._QUERY_BATCH_SIZE:(i+1) * utils._QUERY_BATCH_SIZE],
      'stats_var': stats_var,
    }
    if measurement_method:
      req_json['measurement_method'] = measurement_method
    if unit:
      req_json['unit'] = unit
    if obs_period:
      req_json['observation_period'] = obs_period
    payload = utils._send_request(url, req_json)
    if obs_dates == 'all':
      res.update(payload)
    elif obs_dates == 'latest':
      for geo, stats in payload.items():
        if not stats:
          continue
        time_series = stats.get('data')
        if not time_series: continue
        max_date = max(time_series)
        max_date_stat = time_series[max_date]
        time_series.clear()
        time_series[max_date] = max_date_stat
        res[geo] = stats
    elif obs_dates:
      obs_dates = set(obs_dates)
      for geo, stats in payload.items():
        if not stats:
          continue
        time_series = stats.get('data')
        if not time_series: continue
        for date in list(time_series):
          if date not in obs_dates:
            time_series.pop(date)
        res[geo] = stats
  return res


def get_related_places(dcids, population_type, measured_property,
    measurement_method, stat_type, constraining_properties={},
    within_place='', per_capita=False, same_place_type=False):
  """ Returns :obj:`Place`s related to :code:`dcids` for the given constraints.

  Args:
    dcids (:obj:`iterable` of :obj:`str`): Dcids to get related places.
    population_type (:obj:`str`): The type of statistical population.
    measured_property (:obj:`str`): The measured property.
    measurement_method(:obj:`str`): The measurement method for the observation.
    stat_type (:obj:`str`): The statistical type for the observation.
    constraining_properties (:obj:`map` from :obj:`str` to :obj:`str`, optional):
      A map from constraining property to the value that the
      :obj:`StatisticalPopulation` should be constrained by.
    within_place(:obj:`str`): Optional, the DCID of the place that all the
      related places are contained in.
    per_capita(:obj:`bool`): Optional, whether to take into account
      `PerCapita` when compute the relatedness.
    same_place_type(:obj:`bool`): Optional, whether to require all the
      related places under the same place type.

  Returns:
    The returned :obj:`Place`'s are formatted as a :obj:`dict` from a given
    dcid to a list of related places for the given constraints.

  Raises:
    ValueError: If the payload returned by the Data Commons REST API is
    malformed.

  Examples:
    We would like to get all related places of
    `Santa Clara county <https://browser.datacommons.org/kg?dcid=geoId/06085>`
    Specifying the :code:`dcids` as a :obj:`list` result in the following.

    >>> get_related_places(["geoId/06"], "Person", {
    "age": "Years21To64",
    "gender": "Female"
    }, "count", "CenusACS5yrSurvey", "measuredValue")
    {
      'geoId/06085': [
        'geoId/06041',
        'geoId/06089',
        'geoId/06015',
        'geoId/06023',
      ]
    }
  """
  dcids = filter(lambda v: v==v, dcids)  # Filter out NaN values
  dcids = list(dcids)
  url = utils._API_ROOT + utils._API_ENDPOINTS['get_related_places']
  pvs = []
  for p in constraining_properties:
    pvs.append({'property': p, 'value': constraining_properties[p]})
  req_json = {
    'dcids': dcids,
    'populationType': population_type,
    'pvs': pvs,
    'measuredProperty': measured_property,
    'statType': '',  # TODO: Set to stat_type when having it in BT data.
    'measurementMethod': measurement_method,
    'withinPlace': within_place,
    'perCapita': per_capita,
    'samePlaceType': same_place_type,
  }
  payload = utils._send_request(url, req_json=req_json)
  return payload
