# Copyright 2017 Google Inc.
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
""" Data Commons Python Client API Places Module.

Provides convenience functions for working with Places in the Data Commons
knowledge graph. This submodule implements the ability to access :obj:`Place`'s
within a collection of nodes identified by dcid.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datacommons.utils as utils
import pandas as pd

import requests


def get_places_in(dcids, place_type):
  """ Returns :obj:`Place`s contained in :code:`dcids` of type
      :code:`place_type`.

  Args:
    dcids (Union[:obj:`list` of :obj:`str`, :obj:`pandas.Series`]): Dcids to get
      contained in places.
    place_type (:obj:`str`): The type of places contained in the given dcids to
      filter by.

  Returns:
    When :code:`dcids` is an instance of :obj:`list`, the returned
    :obj:`Place`'s are formatted as a :obj:`dict` from a given dcid to a list of
    places identified by dcids of the given `place_type`.

    When :code:`dcids` is an instance of :obj:`pandas.Series`, the returned
    :obj:`Place`'s are formatted as a :obj:`pandas.Series` where the `i`-th
    entry corresponds to places contained in the place identified by the dcid
    in `i`-th cell if :code:`dcids`. The cells of the returned series will always
    contain a :obj:`list` of place dcids of the given `place_type`.

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

    We can also specify the :code:`dcids` as a :obj:`pandas.Series` like so.

    >>> import pandas as pd
    >>> dcids = pd.Series(["geoId/06"])
    >>> get_places_in(dcids, "County")
    0    [geoId/06041, geoId/06089, geoId/06015, geoId/...
    dtype: object

  """
  # Convert the dcids field and format the request to GetPlacesIn
  dcids, req_dcids = utils._convert_dcids_type(dcids)
  url = utils._API_ROOT + utils._API_ENDPOINTS['get_places_in']
  payload = utils._send_request(url, req_json={
    'dcids': req_dcids,
    'place_type': place_type,
  })

  # Create the results and format it appropriately
  result = utils._format_expand_payload(payload, 'place', must_exist=dcids)
  if isinstance(dcids, pd.Series):
    return pd.Series([result[dcid] for dcid in dcids])
  return result


def get_pop_obs(dcid):
  """ Returns all :obj:`StatisticalPopulation` and :obj:`Observation` \
      of a :obj:`Place`.

  Args:
    dcid (:obj:`str`): Dcid of the place.

  Returns:
    A :obj:`dict` of :obj:`StatisticalPopulation` and :obj:`Observation` that
    are associated to the place identified by the given :code:`dcid`. The given
    dcid is the :obj:`location` of the returned :obj:`StatisticalPopulation`,
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
              'measurementMethod': 'CenusACS5yrSurvey',
              'observationDate': '2017'
            },
            {
              'marginOfError': 226,
              'measuredProp': 'count',
              'measuredValue': 1388,
              'measurementMethod': 'CenusACS5yrSurvey',
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

    - :code:`measuredProp`
    - :code:`observationDate`
    - :code:`observationPeriod` (optional)
    - :code:`measurementMethod` (optional)
    - one of: :code:`measuredValue`, :code:`meanValue`, :code:`maxValue`,
      :code:`minValue`, :code:`medianValue`

  """
  url = utils._API_ROOT + utils._API_ENDPOINTS['get_pop_obs'] + '?dcid={}'.format(dcid)
  return utils._send_request(url, compress=True, post=False)