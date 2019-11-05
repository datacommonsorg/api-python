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
""" Data Commons utilities library

Various functions that can aid in the extension of the DataCommons API.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import defaultdict

import base64
import json
import requests
import os
import zlib


# --------------------------------- CONSTANTS ---------------------------------


# REST API endpoint root
_API_ROOT = "http://api.datacommons.org"

# REST API endpoint paths
_API_ENDPOINTS = {
  'query': '/query',
  'get_property_labels': '/node/property-labels',
  'get_property_values': '/node/property-values',
  'get_triples': '/node/triples',
  'get_places_in': '/node/places-in',
  'get_populations': '/node/populations',
  'get_observations': '/node/observations',
  'get_pop_obs': '/bulk/pop-obs',
  'get_place_obs': '/bulk/place-obs',
}

# The default value to limit to
_MAX_LIMIT = 100

# Environment variable names used by the package
_ENV_VAR_API_KEY = 'DC_API_KEY'         # Name the API key variable


# --------------------------- API UTILITY FUNCTIONS ---------------------------


def set_api_key(api_key):
  """ Sets an environment variable :code:`"DC_API_KEY"` to given :code:`api_key`.

  An API key is required to use the Python Client API. This can be provided to
  the API after importing the library, or set as an environment variable
  :code:`"DC_API_KEY"`.

  For more details about how to get an API key and provide it to the Python
  Client API, please visit :ref:`getting_started`

  Args:
    api_key (:obj:`str`): The api key.
  """
  os.environ[_ENV_VAR_API_KEY] = api_key


# ------------------------- INTERNAL HELPER FUNCTIONS -------------------------


def _send_request(req_url, req_json={}, compress=False, post=True):
  """ Sends a POST request to the given req_url with the given req_json.

  Returns:
    The payload returned by sending the POST request formatted as a Python
    dict.
  """
  # Get the API key
  if not os.environ.get(_ENV_VAR_API_KEY, None):
    raise ValueError(
        'Request error: Must set an API key before using the API! You can '
        'call datacommons.set_api_key or assign the key to an environment '
        'variable named {}'.format(_ENV_VAR_API_KEY))
  headers = {'x-api-key': os.environ[_ENV_VAR_API_KEY]}

  # Send the request and verify the request succeeded
  if post:
    res = requests.post(req_url, headers=headers, json=req_json)
  else:
    res = requests.get(req_url, headers=headers)
  if res.status_code != 200:
    raise ValueError(
        'Response error: An HTTP {} code was returned by the mixer. Printing '
        'response\n\n{}'.format(res.status_code , res.text))

  # Get the JSON
  res_json = res.json()
  if 'payload' not in res_json:
    raise ValueError(
        'Response error: Payload not found. Printing response\n\n'
        '{}'.format(res.text))

  # If the payload is compressed, decompress and decode it
  payload = res_json['payload']
  if compress:
    payload = zlib.decompress(
      base64.b64decode(payload), zlib.MAX_WBITS|32)
  return json.loads(payload)


def _format_expand_payload(payload, new_key, must_exist=[]):
  """ Formats expand type payloads into dicts from dcids to lists of values. """
  # Create the results dictionary from payload
  results = defaultdict(set)
  for entry in payload:
    if 'dcid' in entry and new_key in entry:
      dcid = entry['dcid']
      results[dcid].add(entry[new_key])

  # Ensure all dcids in must_exist have some entry in results.
  for dcid in must_exist:
    results[dcid]
  return {k: sorted(list(v)) for k, v in results.items()}


def _flatten_results(result, default_value=None):
  """ Formats results to map to a single value or default value if empty. """
  flattened = {}
  for k, v in result.items():
    if len(v) > 1:
      raise ValueError(
        'Expected one result, but more returned for "{}": {}'.format(k, v))
    if len(v) == 1:
      flattened[k] = v[0]
    elif default_value is not None:
      flattened[k] = default_value
  return flattened


def _print_header(label):
  """ Prints a pretty header with the given label. """
  print('\n' + '-' * 80)
  print(label)
  print('-' * 80 + '\n')
