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
""" DataCommons utilities library

Contains various functions that can aid in the extension of the DataCommons API.
"""

from collections import defaultdict

import pandas as pd

import base64
import json
import zlib


# --------------------------------- CONSTANTS ---------------------------------


# REST API endpoint root
_API_ROOT = "http://mixergrpc.endpoints.datcom-mixer.cloud.goog"

# REST API endpoint paths
_API_ENDPOINTS = {
  "query": "/query",
  "get_node": "/node",
  "get_property": "/node/property",
  "get_property_value": "/node/property-value",
  "get_triple": "/node/triple",
  "get_place_in": "/expand/place-in"
}

# Database paths
_BIG_QUERY_PATH = 'google.com:datcom-store-dev.dc_v3_clustered'

# The default value to limit to
_MAX_LIMIT = 100


# ------------------------- PANDAS UTILITY FUNCTIONS --------------------------


def flatten_frame(pd_frame):
  """ Expands each cell in a Pandas DataFrame containing a list of values.

  Args:
    pd_frame: The Pandas DataFrame.
  """
  for col in pd_frame:
    if any(isinstance(v, list) for v in pd_frame[col]):
      pd_frame = pd_frame.explode(col)
  pd_frame = pd_frame.reset_index(drop=True)
  return pd_frame

def clean_frame(pd_frame):
  """ A convenience function that cleans a pandas DataFrame.

  The following operations are performed:
  - Columns containing numerical types are converted to floats.
  - Rows with empty values are dropped.

  Args:
    pd_frame: The Pandas DataFrame.
  """
  if len(pd_frame.index) > 0:
    # Convert all numeric columns to numeric types.
    for col in pd_frame:
      col_value = pd_frame[col].iloc[0]
      if isinstance(col_value, str) and col_value.isnumeric():
        pd_frame = pd_frame.astype({col: 'float'})

    # Drop all rows with NaN elements.
    pd_frame = pd_frame.dropna()
  pd_frame = pd_frame.reset_index(drop=True)
  return pd_frame


# ------------------------- INTERNAL HELPER FUNCTIONS -------------------------


def format_response(response, compress=False):
  """ Returns the json payload in a response from the mixer. """
  res_json = response.json()
  if 'code' in res_json and res_json['code'] != 0:
    raise ValueError(res_json['message'])
  elif 'payload' not in res_json:
    return {}

  # If the payload is compressed, decompress and decode it
  payload = res_json['payload']
  if compress:
    payload = zlib.decompress(base64.b64decode(payload), 16 + zlib.MAX_WBITS)
  return json.loads(payload)

def format_expand_payload(payload, new_key, must_exist=[]):
  """ Formats expand type payloads into dicts from dcids to lists of values. """
  # Create the results dictionary from payload
  results = defaultdict(list)
  for entry in payload:
    if 'dcid' in entry and new_key in entry:
      dcid = entry['dcid']
      results[dcid].append(entry[new_key])

  # Ensure all dcids in must_exist have some entry in results.
  for dcid in must_exist:
    results[dcid]
  return dict(results)

def convert_dcids_type(dcids):
  """ Amends dcids list type and creates the approprate request dcids list. """
  # Format the dcids list.
  if isinstance(dcids, str):
    dcids = [dcids]

  # Create the requests dcids list.
  if isinstance(dcids, list):
    req_dcids = dcids
  elif isinstance(dcids, pd.Series):
    req_dcids = list(dcids)
  else:
    raise ValueError(
        'dcids parameter must either be of type string, list or pandas.Series.')
  return dcids, req_dcids
