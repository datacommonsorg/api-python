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
""" Data Commons Python Client API Populations Module.

Provides convenience functions for accessing :obj:`StatisticalPopulation`'s and
:obj:`Observation`'s in the Data Commons knowledge graph. Implements the
following:

- Get :obj:`StatisticalPopulation`'s located at a given collection of nodes.
- Get :obj:`Observation`'s observing a collection of
  :obj:`StatisticalPopulation`'s
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datacommons.utils as utils
import pandas as pd

import requests


def get_populations(dcids, population_type, constraining_properties={}):
  """ Returns :obj:`StatisticalPopulation`'s located at the given :code:`dcids`.

  Args:
    dcids (Union[:obj:`list` of :obj:`str`, :obj:`pandas.Series`]): Dcids
      identifying :obj:`Place`'s of populations to query for. These dcids are
      treated as the property value associated with returned :obj:`Population`'s
      by the property
      `location <https://browser.datacommons.org/kg?dcid=location>`_
    population_type (:obj:`str`): The population type of the
      :obj:`StatisticalPopulation`
    constraining_properties (:obj:`map` from :obj:`str` to :obj:`str`, optional):
      A map from constraining property to the value that the
      :obj:`StatisticalPopulation` should be constrained by.

  Returns:
    When :code:`dcids` is an instance of :obj:`list`, the returned
    :obj:`StatisticalPopulation` are formatted as a :obj:`dict` from a given
    dcid to the unique :obj:`StatisticalPopulation` located at the dcid as
    specified by the `population_type` and `constraining_properties` *if such
    exists*. A given dcid will *NOT* be a member of the :obj:`dict` if such
    a population does not exist.

    When :code:`dcids` is an instance of :obj:`pandas.Series`, the returned
    :obj:`StatisticalPopulation` are formatted as a :obj:`pandas.Series` where
    the `i`-th entry corresponds to populations located at the given dcid
    specified by the `population_type` and `constraining_properties` *if such
    exists*. Otherwise, the cell is empty.

  Raises:
    ValueError: If the payload returned by the Data Commons REST API is
      malformed.

  Examples:
    We would like to get

    - The `population of employed persons in California <https://browser.datacommons.org/kg?dcid=dc/p/x6t44d8jd95rd>`_
    - The `population of employed persons in Kentucky <https://browser.datacommons.org/kg?dcid=dc/p/fs929fynprzs>`_
    - The `population of employed persons in Maryland <https://browser.datacommons.org/kg?dcid=dc/p/lr52m1yr46r44>`_.

    These populations are specified as having a
    `population_type` as :obj:`Person` and the `constraining_properties`
    as `employment <https://browser.datacommons.org/kg?dcid=employment>`_
    = BLS_Employed

    With a :obj:`list` of dcids for our states, we can get the populations we
    want as follows.

    >>> dcids = ["geoId/06", "geoId/21", "geoId/24"]
    >>> pvs = {'employment': 'BLS_Employed'}
    >>> dc.get_populations(dcids, 'Person', constraining_properties=pvs)
    {
      "geoId/06": "dc/p/x6t44d8jd95rd",
      "geoId/21": "dc/p/fs929fynprzs",
      "geoId/24": "dc/p/lr52m1yr46r44"
    }

    We can also specify the :code:`dcids` as a :obj:`pandas.Series` like so.

    >>> import pandas as pd
    >>> dcids = pd.Series(["geoId/06", "geoId/21", "geoId/24"])
    >>> pvs = {'employment': 'BLS_Employed'}
    >>> dc.get_populations(dcids, 'Person', constraining_properties=pvs)
    0    dc/p/x6t44d8jd95rd
    1     dc/p/fs929fynprzs
    2    dc/p/lr52m1yr46r44
    dtype: object
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
  """ Returns values of :obj:`Observation`'s observing the given :code:`dcids`.

  Args:
    dcids (Union[:obj:`list` of :obj:`str`, :obj:`pandas.Series`]): Dcids
      identifying nodes that returning :obj:`Observation`'s observe. These dcids
      are treated as the property value associated with returned
      :obj:`Observation`'s by the property
      `observedNode <https://browser.datacommons.org/kg?dcid=observedNode>`_
    measured_property (:obj:`str`): The measured property.
    stats_type (:obj:`str`): The statistical type for the observation.
    observation_date (:obj:`str`): The associated observation date in ISO8601
      format.
    observation_period (:obj:`str`, optional): An optional parameter specifying
      the observation period.
    measurement_method (:obj:`str`, optional): An optional parameter specifying
      the measurement method.

  Raises:
    ValueError: If the payload returned by the Data Commons REST API is
      malformed.

  Returns:
    When :code:`dcids` is an instance of :obj:`list`, the returned
    :obj:`Observation`'s are formatted as a :obj:`dict` from a given dcid to the
    unique :obj:`Observation` observing the dcid where the observation is
    specified by what is given in the other parameters *if such exists*. A given
    dcid will *NOT* be a member of the :obj:`dict` if such an observation does
    not exist.

    When :code:`dcids` is an instance of :obj:`pandas.Series`, the returned
    :obj:`Observation`'s are formatted as a :obj:`pandas.Series` where the
    `i`-th entry corresponds to observation observing the given dcid as specified
    by the other parameters *if such exists*. Otherwise, the cell holds NaN.

  Examples:
    We would like to get the following for December, 2018:

    - The `total count of employed persons in California <https://browser.datacommons.org/kg?dcid=dc/o/wetnm9026gf73>`_
    - The `total count of employed persons in Kentucky <https://browser.datacommons.org/kg?dcid=dc/o/4nklvdnkfq835>`_
    - The `total count of employed persons in Maryland <https://browser.datacommons.org/kg?dcid=dc/o/nkntbc4vpshn9>`_.

    The observations we want are observations of the populations representing
    employed individuals in each state (to get these, see
    :any:module-datacommons.populations.get_populations). With a list of these
    population dcids, we can get the observations like so.

    >>> dcids = [
    ...   "dc/p/x6t44d8jd95rd",   # Employed individuals in California
    ...   "dc/p/fs929fynprzs",    # Employed individuals in Kentucky
    ...   "dc/p/lr52m1yr46r44"    # Employed individuals in Maryland
    ... ]
    >>> get_observations(dcids, 'count', 'measuredValue', '2018-12',
    ...   observation_period='P1M',
    ...   measurement_method='BLSSeasonallyAdjusted'
    ... )
    {
      "dc/p/x6t44d8jd95rd": 18704962.0,
      "dc/p/fs929fynprzs": 1973955.0,
      "dc/p/lr52m1yr46r44": 3075662.0
    }

    We can also specify the :code:`dcids` as a :obj:`pandas.Series` like so.

    >>> import pandas as pd
    >>> dcids = pd.Series(["dc/p/x6t44d8jd95rd", "dc/p/fs929fynprzs", "dc/p/lr52m1yr46r44"])
    >>> get_observations(dcids, 'count', 'measuredValue', '2018-12',
    ...   observation_period='P1M',
    ...   measurement_method='BLSSeasonallyAdjusted'
    ... )
    0    18704962.0
    1     1973955.0
    2     3075662.0
    dtype: float64
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

  # Drop empty results by calling _flatten_results without default_value, then
  # coerce the type to float if possible.
  typed_results = {}
  for k, v in utils._flatten_results(result).items():
    try:
      typed_results[k] = float(v)
    except ValueError:
      typed_results[k] = v
  return typed_results
