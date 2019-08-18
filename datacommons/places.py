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
  """ Returns :obj:`Place`'s contained in :code:`dcids` of type `place_type`.

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
  """ Returns all :obj:`StatisticalPopulation`'s and :obj:`Observation`'s of a :obj:`Place`.

  Args:
    dcid (:obj:`str`): Dcid of the place.

  Returns:
    "A :obj:`dict` of :obj:`StatisticalPopulation`'s and :obj:`Observation`'s that are associated to the place
    identified by the given :code:`dcid`. The given dcid is the :obj:`location` of the returned :obj:`StatisticalPopulation`'s,
    which are the :obj:`observedNode`'s of the returned :obj:`Observation`'s.
    See example below for more detail about how the returned :obj:`dict` is structured."

  Raises:
    ValueError: If the payload returned by the Data Commons REST API is
      malformed.

  Examples:
    We would like to get :obj:`StatisticalPopulation`'s and :obj:`Observations`'s of
    `Mountain View https://browser.datacommons.org/kg?dcid=geoId/0649670`_.

    >>> get_pop_obs("geoId/0649670")
    {
      'name': 'Mountain View',
      'placeType': 'City',
      'populations': {
        'dc/p/zsb968m3v1f97': {
          'popType': 'Person',
          'numConstraints': 1,
          'propertyValues': {
            'employmentStatus': 'BLS_InLaborForce'
          },
          'observations': [
            {
              'measuredProp': 'count',
              'measuredValue': 49478,
              'measurementMethod': 'BLSSeasonallyUnadjusted',
              'observationDate': '2015-12',
              'observationPeriod': 'P1M'
            },
            {
              'measuredProp': 'count',
              'measuredValue': 51480,
              'measurementMethod': 'BLSSeasonallyUnadjusted',
              'observationDate': '2018-02',
              'observationPeriod': 'P1M'
            },
            {
              'measuredProp': 'count',
              'measuredValue': 48800,
              'measurementMethod': 'BLSSeasonallyUnadjusted',
              'observationDate': '2014-11',
              'observationPeriod': 'P1M'
            }
          ],
        },
      }
    }

    Notice that the return value is a multi-level :obj:`dict`. The top level contains the following keys.

    - :code:`name` and :code:`placeType` provides the name and type of the :obj:`Place` identified by the given :code:`dcid`.
    - :code:`populations` maps to a :obj:`dict` containing all :obj:`StatisticalPopulation`'s that have the given :code:`dcid` as its :obj:`location`.

    The :code:`populations` dictionary is keyed by the dcid of each :obj:`StatisticalPopulation`. The mapped dictionary contains the following keys.

    - :code:`popType` which gives the population type of the :obj:`StatisticalPopulation` identified by the key.
    - :code:`numConstraints` which gives the number of constraining properties defined for the identified :obj:`StatisticalPopulation`.
    - :code:`propertyValues` which gives a :obj:`dict` mapping a constraining property to its value for the identified :obj:`StatisticalPopulation`.
    - :code:`observations` which gives a list of all :obj:`Observation`'s that have the identified :obj:`StatisticalPopulation` as their :obj:`observedNode`. Each :obj:`Observation` is represented by a :code:`dict` that have the keys: <INSERT AND DOCUMENT ALL KEYS HERE>
  """
  url = utils._API_ROOT + utils._API_ENDPOINTS['get_pop_obs'] + '?dcid={}'.format(dcid)
  return utils._send_request(url, compress=True, post=False)