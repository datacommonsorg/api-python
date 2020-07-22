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
""" Data Commons Python API Populations Module.

Provides convenience functions for accessing :obj:`StatisticalPopulation`'s and
:obj:`Observation`'s in the Data Commons Graph. Implements the
following:

- Get :obj:`StatisticalPopulation`'s located at a given collection of nodes.
- Get :obj:`Observation`'s observing a collection of
  :obj:`StatisticalPopulation`'s
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datacommons.utils as utils


def _flatten_results(result, default_value=None):
  """ Formats results to map to a single value or default value if empty. """
  for k in list(result):
    v = result[k]
    if len(v) > 1:
      raise ValueError(
        'Expected one result, but more returned for "{}": {}'.format(k, v))
    if len(v) == 1:
      result[k] = v[0]
    else:
      if default_value is not None:
        result[k] = default_value
      else:
        del result[k]
  return result


def get_populations(dcids, population_type, constraining_properties={}):
  """ Returns :obj:`StatisticalPopulation`'s located at the given :code:`dcids`.

  Args:
    dcids (:obj:`iterable` of :obj:`str`): Dcids
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
    The returned :obj:`StatisticalPopulation` are formatted as a :obj:`dict` from a given
    dcid to the unique :obj:`StatisticalPopulation` located at the dcid as
    specified by the `population_type` and `constraining_properties` *if such
    exists*. A given dcid will *NOT* be a member of the :obj:`dict` if such
    a population does not exist.

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
  """
  # Convert the dcids field and format the request to GetPopulations
  dcids = filter(lambda v: v==v, dcids)  # Filter out NaN values
  dcids = list(dcids)
  pv = [{'property': k, 'value': v} for k, v in constraining_properties.items()]
  url = utils._API_ROOT + utils._API_ENDPOINTS['get_populations']
  payload = utils._send_request(url, req_json={
    'dcids': dcids,
    'population_type': population_type,
    'pvs': pv,
  })

  # Create the results and format it appropriately
  result = utils._format_expand_payload(
    payload, 'population', must_exist=dcids)

  # Drop empty results while flattening
  return _flatten_results(result)


def get_observations(dcids,
                     measured_property,
                     stats_type,
                     observation_date,
                     observation_period=None,
                     measurement_method=None):
  """ Returns values of :obj:`Observation`'s observing the given :code:`dcids`.

  Args:
    dcids (:obj:`iterable` of :obj:`str`): Dcids
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
  """
  dcids = filter(lambda v: v==v, dcids)  # Filter out NaN values
  dcids = list(dcids)
  req_json = {
    'dcids': dcids,
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
  payload = utils._send_request(url, req_json=req_json)

  # Create the results and format it appropriately
  result = utils._format_expand_payload(
    payload, 'observation', must_exist=dcids)

  # Drop empty results by calling _flatten_results without default_value, then
  # coerce the type to float if possible.
  typed_results = {}
  for k, v in _flatten_results(result).items():
    try:
      typed_results[k] = float(v)
    except ValueError:
      typed_results[k] = v
  return typed_results


def get_pop_obs(dcid):
  """ Returns all :obj:`StatisticalPopulation` and :obj:`Observation` \
      of a :obj:`Thing`.

  Args:
    dcid (:obj:`str`): Dcid of the thing.

  Returns:
    A :obj:`dict` of :obj:`StatisticalPopulation` and :obj:`Observation` that
    are associated to the thing identified by the given :code:`dcid`. The given
    dcid is linked to the returned :obj:`StatisticalPopulation`,
    which are the :obj:`observedNode` of the returned :obj:`Observation`.
    See example below for more detail about how the returned :obj:`dict` is
    structured.

  Raises:
    ValueError: If the payload returned by the Data Commons REST API is
      malformed.

  Examples:
    We would like to get all :obj:`StatisticalPopulation` and
    :obj:`Observations` of
    `Santa Clara <https://browser.datacommons.org/kg?dcid=geoId/06085>`_.

    >>> get_pop_obs("geoId/06085")
    {
      'name': 'Santa Clara',
      'placeType': 'County',
      'populations': {
        'dc/p/zzlmxxtp1el87': {
          'popType': 'Household',
          'numConstraints': 3,
          'propertyValues': {
            'householderAge': 'Years45To64',
            'householderRace': 'USC_AsianAlone',
            'income': 'USDollar35000To39999'
          },
          'observations': [
            {
              'marginOfError': 274,
              'measuredProp': 'count',
              'measuredValue': 1352,
              'measurementMethod': 'CensusACS5yrSurvey',
              'observationDate': '2017'
            },
            {
              'marginOfError': 226,
              'measuredProp': 'count',
              'measuredValue': 1388,
              'measurementMethod': 'CensusACS5yrSurvey',
              'observationDate': '2013'
            }
          ],
        },
      },
      'observations': [
        {
          'meanValue': 4.1583,
          'measuredProp': 'particulateMatter25',
          'measurementMethod': 'CDCHealthTracking',
          'observationDate': '2014-04-04',
          'observedNode': 'geoId/06085'
        },
        {
          'meanValue': 9.4461,
          'measuredProp': 'particulateMatter25',
          'measurementMethod': 'CDCHealthTracking',
          'observationDate': '2014-03-20',
          'observedNode': 'geoId/06085'
        }
      ]
    }

    Notice that the return value is a multi-level :obj:`dict`. The top level
    contains the following keys.

    - :code:`name` and :code:`placeType` provides the name and type of the
      :obj:`Place` identified by the given :code:`dcid`.
    - :code:`populations` maps to a :obj:`dict` containing all
      :obj:`StatisticalPopulation` that have the given :code:`dcid` as its
      :obj:`location`.
    - :code:`observations` maps to a :obj:`list` containing all
      :obj:`Observation` that have the given :code:`dcid` as its
      :obj:`observedNode`.

    The :code:`populations` dictionary is keyed by the dcid of each
    :obj:`StatisticalPopulation`. The mapped dictionary contains the following
    keys.

    - :code:`popType` which gives the population type of the
      :obj:`StatisticalPopulation` identified by the key.
    - :code:`numConstraints` which gives the number of constraining properties
      defined for the identified :obj:`StatisticalPopulation`.
    - :code:`propertyValues` which gives a :obj:`dict` mapping a constraining
      property to its value for the identified :obj:`StatisticalPopulation`.
    - :code:`observations` which gives a list of all :obj:`Observation`'s that
      have the identified :obj:`StatisticalPopulation` as their
      :obj:`observedNode`.

    Each :obj:`Observation` is represented by a :code:`dict` that have the keys:

    - :code:`measuredProp`: The property measured by the :obj:`Observation`.
    - :code:`observationDate`: The date when the :obj:`Observation` was made.
    - :code:`observationPeriod` (optional): The period over which the
      :obj:`Observation` was made.
    - :code:`measurementMethod` (optional): A field providing additional
      information on how the :obj:`Observation` was collected.
    - Additional fields that denote values measured by the :obj:`Observation`.
      These may include the following: :code:`measuredValue`, :code:`meanValue`,
      :code:`medianValue`, :code:`maxValue`, :code:`minValue`, :code:`sumValue`,
      :code:`marginOfError`, :code:`stdError`, :code:`meanStdError`, and others.
  """
  url = utils._API_ROOT + utils._API_ENDPOINTS['get_pop_obs'] + '?dcid={}'.format(dcid)
  return utils._send_request(url, compress=True, post=False)

def get_place_obs(
  place_type, observation_date, population_type, constraining_properties={}):
  """ Returns all :obj:`Observation`'s for all places given the place type,
  observation date and the :obj:`StatisticalPopulation` constraints.

  Args:
    place_type (:obj:`str`): The type of places to query
      :obj:`StatisticalPopulation`'s and :obj:`Observation`'s for.
    observation_date (:obj:`str`): The observation date in ISO-8601 format.
    population_type (:obj:`str`): The population type of the
      :obj:`StatisticalPopulation`
    constraining_properties (:obj:`map` from :obj:`str` to :obj:`str`, optional):
      A map from constraining property to the value that the
      :obj:`StatisticalPopulation` should be constrained by.

  Returns:
    A list of dictionaries, with each dictionary containng *all*
    :obj:`Observation`'s of a place that conform to the :obj:`StatisticalPopulation`
    constraints. See examples for more details on how the format of the
    return value is structured.

  Raises:
    ValueError: If the payload is malformed.

  Examples:
    We would like to get all :obj:`StatisticalPopulation` and
    :obj:`Observations` for all places of type :obj:`City` in year 2017 where
    the populations have a population type of :obj:`Person` is specified by the
    following constraining properties.

    - Persons should have `age <https://browser.datacommons.org/kg?dcid=age>`_
      with value `Years5To17 <https://browser.datacommons.org/kg?dcid=Years5To17>`_
    - Persons should have `placeOfBirth <https://browser.datacommons.org/kg?dcid=placeOfBirth>`_
      with value BornInOtherStateInTheUnitedStates.

    >>> props = {
    ...   'age': 'Years5To17',
    ...   'placeOfBirth': 'BornInOtherStateInTheUnitedStates'
    ... }
    >>> get_place_obs('City', '2017', Person', constraining_properties=props)
    [
      {
        'name': 'Marcus Hook borough',
        'place': 'geoId/4247344',
        'populations': {
          'dc/p/pq6frs32sfvk': {
            'observations': [
              {
                'marginOfError': 39,
                'measuredProp': 'count',
                'measuredValue': 67,
                'type': 'Observation'
              },
              # More observations...
            ],
          }
        }
      },
      # Entries for more cities...
    ]

    The value returned by :code:`get_place_obs` is a :obj:`list` of
    :obj:`dict`'s. Each dictionary corresponds to a :obj:`StatisticalPopulation`
    matching the given :code:`population_type` and
    :code:`constraining_properties` for a single place of the given
    :code:`place_type`. The dictionary contains the following keys.

    - :code:`name`: The name of the place being described.
    - :code:`place`: The dcid associated with the place being described.
    - :code:`populations`: A :obj:`dict` mapping :code:`StatisticalPopulation`
      dcids to a a :obj:`dict` with a list of :code:`observations`.

    Each :obj:`Observation` is represented by a :obj:`dict` with the following
    keys.
    - :code:`measuredProp`: The property measured by the :obj:`Observation`.
    - :code:`measurementMethod` (optional): A field identifying how the
      :obj:`Observation` was made
    - Additional fields that denote values measured by the :obj:`Observation`.
      These may include the following: :code:`measuredValue`, :code:`meanValue`,
      :code:`medianValue`, :code:`maxValue`, :code:`minValue`, :code:`sumValue`,
      :code:`marginOfError`, :code:`stdError`, :code:`meanStdError`, and others.
  """
  # Create the json payload and send it to the REST API.
  pv = [{'property': k, 'value': v} for k, v in constraining_properties.items()]
  url = utils._API_ROOT + utils._API_ENDPOINTS['get_place_obs']
  payload = utils._send_request(url, req_json={
    'place_type': place_type,
    'observation_date': observation_date,
    'population_type': population_type,
    'pvs': pv,
  }, compress=True)
  return payload['places']
