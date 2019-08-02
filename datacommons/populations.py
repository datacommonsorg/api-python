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

import datacommons.utils as utils
import pandas as pd

import requests


def get_populations(dcids, population_type, constraining_properties={}):
  """ Returns StatisticalPopulation dcids located at the given dcids.

  When the dcids are given as a list, the returned property values are formatted
  as a map from given dcid to associated StatatisticalPopulation dcid. The dcid
  will *not* be a member of the dict if a population is not located there.

  When the dcids are given as a Pandas Series, returned StatisticalPopulations
  are formatted as a Pandas Series where the i-th entry corresponds to the
  StatisticalPopulation located in the i-th given dcid. The cells of the Series
  contain a single dcid as the combination of population_type and
  constraining_properties always define a unique StatisticalPopulation if it
  exists.

  Args:
    dcids: A list or Pandas Series of dcids denoting the locations of
      populations to query for.
    population_type: The population type of the `StatisticalPopulation`
    constraining_properties: An optional map from constraining property to the
      value that the `StatisticalPopulation` should be constrained by.
  """
  # Convert the dcids field and format the request to GetPopulations
  dcids, req_dcids = utils._convert_dcids_type(dcids)
  pv = [{'property': k, 'value': v} for k, v in constraining_properties.items()]
  url = utils._API_ROOT + utils._API_ENDPOINTS['get_populations']
  res = requests.post(url, json={
    'dcids': req_dcids,
    'population_type': population_type,
    'pvs': pv,
  })
  payload = utils._format_response(res)

  # Create the results and format it appropriately
  result = utils._format_expand_payload(
    payload, 'population', must_exist=dcids)
  if isinstance(dcids, pd.Series):
    flattened = utils._flatten_results(result, default_value="")
    return pd.Series([flattened[dcid] for dcid in dcids])

  # Drop empty results while flattening
  return utils._flatten_results(result)


def get_observations(dcids,
                     measured_property,
                     stats_type,
                     observation_date,
                     observation_period=None,
                     measurement_method=None):
  """ Returns Observations made of the given dcids.

  When the dcids are given as a list, the returned Observations are formatted
  as a map from given dcid to Observation dcid. The dcid will *not* be a member
  of the dict if a population is there is no available observation for it.

  If the dcids field is a Pandas Series, then the return value is a Series where
  the i-th cell is the list of values associated with the given property for the
  i-th dcid. If no observation is returned, then the cell holds NaN.

  When the dcids are given as a Pandas Series, returned Observations are
  formatted as a Pandas Series where the i-th entry corresponds to the value
  of the observation observing the i-th given dcid. The cells of the Series
  contain a single dcid as the combination of measured_property, stats_type,
  observation_date, and optional parameters always define a unique Observation
  if it exists. If it does not, then the cell will hold NaN.

  Args:
    dcids: A list or Pandas Series of dcids of nodes that are observed by
      observations being queried for.
    measured_property: The measured property.
    stats_type: The statistical type for the observation.
    observation_date: The assoociated observation date in ISO8601 format.
    observation_period: An optional parameter specifying the observation period.
    measurement_method: An optional parameter specifying the measurement method.
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
  if isinstance(dcids, pd.Series):
    flattened = utils._flatten_results(result, default_value="")
    series = pd.Series([flattened[dcid] for dcid in dcids])
    return series.apply(pd.to_numeric, errors='coerce')

  # Convert type and drop empty results while flattening.
  typed_results = {}
  for k, v in utils._flatten_results(result).items():
    try:
      typed_results[k] = float(v)
    except ValueError:
      typed_results[k] = v
  return typed_results
