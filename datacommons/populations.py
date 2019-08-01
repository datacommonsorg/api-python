# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Data Commons Populations wrapper functions."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datacommons.utils as utils
import pandas as pd

import requests


def get_populations(dcids, population_type, pv_map={}):
  """ Returns a list of populations associated with the given dcids.

  The pv_map must be a dictionary mapping p (property) to v (value) in a pv
  pair defining a population.

  If the dcids field is a list, then the return value is a dictionary mapping
  dcid to the list of values associated with the given property.

  If the dcids field is a Pandas Series, then the return value is a Series where
  the i-th cell is the list of values associated with the given property for the
  i-th dcid.

  Args:
    dcids: List of dcids to get populations for.
    population_type: The type of entity associated with the population
    pv_map: pv-pairs defining the population.
  """
  # Convert the dcids field and format the request to GetPopulations
  dcids, req_dcids = utils._convert_dcids_type(dcids)
  pv_params = [{'property': k, 'value': v} for k, v in pv_map.items()]
  url = utils._API_ROOT + utils._API_ENDPOINTS['get_populations']
  res = requests.post(url, json={
    'dcids': req_dcids,
    'population_type': population_type,
    'pvs': pv_params,
  })
  payload = utils._format_response(res)

  # Create the results and format it appropriately
  result = utils._format_expand_payload(
    payload, 'population', must_exist=dcids)
  flattened = utils._flatten_results(result)
  if isinstance(dcids, pd.Series):
    return pd.Series([flattened[dcid] for dcid in dcids])
  return flattened


def get_observations(dcids,
                     measured_property,
                     stats_type,
                     observation_date,
                     observation_period=None,
                     measurement_method=None):
  """ Returns a list of observations associated with the given dcids.

  If the dcids field is a list, then the return value is a dictionary mapping
  dcid to the list of values associated with the given property.

  If the dcids field is a Pandas Series, then the return value is a Series where
  the i-th cell is the list of values associated with the given property for the
  i-th dcid. If no observation is returned, then the cell holds NaN.

  Args:
    dcids: List of dcids to get observations for.
    measured_property: The measured property.
    stats_type: The statistical type for the observation.
    observation_date: The assoociated observation date in ISO8601 format.
    observation_period: The observation period.
    measurement_method: The measurement method.
    reload: A flag that sends the query without hitting cache when set.
  """
  # Convert the dcids field and format the request to GetObservation
  dcids, req_dcids = utils._convert_dcids_type(dcids)
  req_json = {
    'dcids': req_dcids,
    'measured_property': measured_property,
    'stats_type': stats_type,
    'observation_date': observation_date,
  }
  if observation_period:
    req_json['observation_period'] = observation_period
  if measurement_method:
    req_json['measurement_method'] = measurement_method

  # Issue the request to GetObservation
  url = utils._API_ROOT + utils._API_ENDPOINTS['get_observations']
  res = requests.post(url, json=req_json)
  payload = utils._format_response(res)

  # Create the results and format it appropriately
  result = utils._format_expand_payload(
    payload, 'observation', must_exist=dcids)
  flattened = utils._flatten_results(result)
  if isinstance(dcids, pd.Series):
    series = pd.Series([flattened[dcid] for dcid in dcids])
    return series.apply(pd.to_numeric, errors='coerce')
  return flattened
