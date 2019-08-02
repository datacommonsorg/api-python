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

"""Data Commons Places wrapper functions.
"""

import datacommons.utils as utils
import pandas as pd

import requests


def get_places_in(dcids, place_type):
  """ Returns places that are contained in the given dcids of the given type.

  When the dcids are given as a list, the returned property values are formatted
  as a map from given dcid to list of place dcids contained in the given dcid.

  When the dcids are given as a Pandas Series, the returned contained in places
  are formatted as a Pandas Series where the i-th entry corresponds to places
  contained in places identified by the i-th given dcid. The cells of the
  returned series will always contain a list of place dcids.

  Args:
    dcids: A list or Pandas Series of dcids to get contained in places.
    place_type: The type of places contained in the given dcids to query for.
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
