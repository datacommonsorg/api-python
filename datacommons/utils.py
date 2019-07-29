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

from collections import OrderedDict

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


# ----------------------------- HELPER FUNCTIONS ------------------------------


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
