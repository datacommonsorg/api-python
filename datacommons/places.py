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
knowledge graph. Use cases include getting places contained in a list of places.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datacommons.utils as utils
import pandas as pd

import requests


def get_places_in(dcids, place_type):
  """ Returns :obj:`Place`'s contained in `dcids` of type `place_type`.

  When the dcids are given as a list, the returned property values are formatted
  as a map from given dcid to list of place dcids contained in the given dcid.

  When the dcids are given as a Pandas Series, the returned contained in places
  are formatted as a Pandas Series where the i-th entry corresponds to places
  contained in places identified by the i-th given dcid. The cells of the
  returned series will always contain a list of place dcids.

  Args:
    dcids (Union[:obj:`list` of :obj:`str`, :obj:`pandas.Series`]): Dcids to get
      contained in places.
    place_type (:obj:`str`): The type of places contained in the given dcids to
      filter by.

  Returns:
    When `dcids` is an instance of :obj:`list`, the returned :obj:`Place`'s
    are formatted as a :obj:`dict` from a given dcid to a list of places
    identified by dcids of the given `place_type`.

    When `dcids` is an instance of :obj:`pandas.Series`, the returned
    :obj:`Place`'s are formatted as a :obj:`pandas.Series` where the `i`-th
    entry corresponds to places contained in the place identified by the dcid
    in `i`-th cell if `dcids`. The cells of the returned series will always
    contain a :obj:`list` of place dcids of the given `place_type`.

  Raises:
    ValueError: If the payload returned by the Data Commons mixer is malformed.

  Examples:
    We would like to get all Counties contained in
    `California <https://browser.datacommons.org/kg?dcid=geoId/06>`_. Specifying the
    `dcids` as a :obj:`list` resulst in the following.

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

    We can also specify the `dcids` as a :obj:`pandas.Series` like so.

    >>> import pandas as pd
    >>> dcids = pd.Series(["geoId/06"])
    >>> get_places_in(dcids, "County")
    0    [geoId/06041, geoId/06089, geoId/06015, geoId/...
    dtype: object

  """
  # Convert the dcids field and format the request to GetPlacesIn
  dcids, req_dcids = utils._convert_dcids_type(dcids)
  url = utils._API_ROOT + utils._API_ENDPOINTS['get_places_in']
  res = requests.post(url, json={
    'dcids': req_dcids,
    'place_type': place_type,
  })
  payload = utils._format_response(res)

  # Create the results and format it appropriately
  result = utils._format_expand_payload(payload, 'place', must_exist=dcids)
  if isinstance(dcids, pd.Series):
    return pd.Series([result[dcid] for dcid in dcids])
  return result
