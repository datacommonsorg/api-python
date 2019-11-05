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

import requests


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
  dcids = list(dcids)
  url = utils._API_ROOT + utils._API_ENDPOINTS['get_places_in']
  payload = utils._send_request(url, req_json={
    'dcids': dcids,
    'place_type': place_type,
  })

  # Create the results and format it appropriately
  result = utils._format_expand_payload(payload, 'place', must_exist=dcids)
  return result
