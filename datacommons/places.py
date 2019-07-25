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

"""DataCommons Places data API Mixin.
"""

from datacommons.utils import format_response, _API_ROOT, _API_ENDPOINTS

import datacommons
import requests

class PlacesMixin:

  def get_places_in(self, seed_col_name, new_col_name, new_col_type, reload=False):
    """ Adds a new column with places contained in seed column entities.

    Args:
      seed_col_name: The column name containing DCIDs to get contained entities.
      new_col_name: The column name for where the results are stored.
      new_col_type: The type of place to query for.
      reload: Send the query without hitting cache.
    """
    self._verify_col_add(new_col_name, seed_col=seed_col_name)
    self._verify_col_type_excludes(seed_col_name, ['Text'])

    # Get the seed column dcids. If no dcids present, append an empty column
    seed_col = self._dataframe[seed_col_name]
    seed_col_type = self._col_types[seed_col_name]

    # Create the request to GetPlaceIn
    url = _API_ROOT + _API_ENDPOINTS['get_place_in']
    res = requests.post(url, json={
      'dcids': list(seed_col),
      'place_type': new_col_type,
      'reload': reload,
    })
    payload = format_response(res)

    # Load the data into a new DCFrame and rename the columns
    type_hint = {'dcid': seed_col_type, 'place': new_col_type}
    labels = {'dcid': seed_col_name, 'place': new_col_name}
    new_frame = datacommons.DCFrame(payload, type_hint=type_hint)
    new_frame.rename(labels)
    self.merge(new_frame)
