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

Provides functions for getting data on StatVars from Data Commons Graph.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from datacommons.utils import _API_ROOT, _API_ENDPOINTS, _ENV_VAR_API_KEY

import json
import os
import six.moves.urllib.error
import six.moves.urllib.request

import datacommons.utils as utils


def _send_get_stat_req(url):

    headers = {'Content-Type': 'application/json'}
    if os.environ.get(_ENV_VAR_API_KEY):
        headers['x-api-key'] = os.environ[_ENV_VAR_API_KEY]

    req = six.moves.urllib.request.Request(url, headers=headers)

    try:
        res = six.moves.urllib.request.urlopen(req)
    except six.moves.urllib.error.HTTPError as e:
        raise ValueError('Response error {}:\n{}'.format(e.code, e.read()))

    # Verify then store the results.
    res_json = json.loads(res.read())
    return res_json


def get_stat_value(place,
                   stat_var,
                   date=None,
                   measurement_method=None,
                   observation_period=None,
                   unit=None,
                   scaling_factor=None):
    """Returns a value for :code:`place` based on the :code:`stat_var`.

    Args:
      place (:obj:`iterable` of :obj:`str`): The dcid of `Place` to query for.
      stat_var (:obj:`str`): The dcid of the `StatisticalVariable`.
      obs_date (:obj:`str`): Optional, the preferred date of observation
        in ISO 8601 format. If not specified, returns the latest observation.
      measurement_method (:obj:`str`): Optional, the dcid of the preferred
        `measurementMethod` value.
      observation_period (:obj:`str`): Optional, the preferred
        `observationPeriod` value.
      unit (:obj:`str`): Optional, the dcid of the preferred `unit` value.
      scaling_factor (:obj:`int`): Optional, the preferred `scalingFactor` value.
    Returns:
      A :obj:`double` the value of :code:`stat_var` for :code:`place`, filtered
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

    res_json = _send_get_stat_req(url)

    if 'value' not in res_json:
        raise ValueError('No value in response.')
    return res_json['value']


def get_stat_series(place,
                    stat_var,
                    measurement_method=None,
                    observation_period=None,
                    unit=None,
                    scaling_factor=None):
    """Returns a :obj:`dict` for :code:`place` based on the :code:`stat_var`.

    Args:
      place (:obj:`iterable` of :obj:`str`): The dcid of `Place` to query for.
      stat_var (:obj:`str`): The dcid of the `StatisticalVariable`.
      measurement_method (:obj:`str`): Optional, the dcid of the preferred
        `measurementMethod` value.
      observation_period (:obj:`str`): Optional, the preferred
        `observationPeriod` value.
      unit (:obj:`str`): Optional, the dcid of the preferred `unit` value.
      scaling_factor (:obj:`int`): Optional, the preferred `scalingFactor` value.
    Returns:
      A :obj:`dict` mapping dates to value of :code:`stat_var` for :code:`place`,
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

    res_json = _send_get_stat_req(url)

    if 'series' not in res_json:
        raise ValueError('No response.')
    return res_json['series']
