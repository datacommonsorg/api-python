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
""" Data Commons Utilities Library.

Various functions that can aid in the extension of the Data Commons API.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import defaultdict

import base64
import json
import os
import six.moves.urllib.error
import six.moves.urllib.request
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
  'get_related_places': '/node/related-places',
  'get_populations': '/node/populations',
  'get_observations': '/node/observations',
  'get_pop_obs': '/bulk/pop-obs',
  'get_place_obs': '/bulk/place-obs',
  'get_stats': '/bulk/stats',
  'get_stat_value': '/stat/value',
  'get_stat_series': '/stat/series',
  'get_stat_all': '/stat/all',
}

# The default value to limit to
_MAX_LIMIT = 100

# Batch size for heavyweight queries.
_QUERY_BATCH_SIZE = 500

# Environment variable names used by the package	
_ENV_VAR_API_KEY = 'DC_API_KEY'	

# --------------------------- API UTILITY FUNCTIONS ---------------------------


def set_api_key(api_key):
  """Sets an environment variable :code:`"DC_API_KEY"` to given :code:`api_key`.

  Users may supply an API key to the Python API, which simply passes it on to
  the REST API for handling. The API key can be provided to the API after
  importing the library, or set as an environment variable
  :code:`"DC_API_KEY"`.

  For more details about how to get an API key and provide it to the Python
  Client API, please visit :ref:`getting_started`.
  Args:
    api_key (:obj:`str`): The API key.
  """	
  os.environ[_ENV_VAR_API_KEY] = api_key


# ------------------------- INTERNAL HELPER FUNCTIONS -------------------------


def _send_request(req_url, req_json={}, compress=False, post=True, use_payload=True):
  """ Sends a POST/GET request to req_url with req_json, default to POST.

  Returns:
    The payload returned by sending the POST/GET request formatted as a dict.
  """
  headers = {
    'Content-Type': 'application/json'
  }

  # Pass along API key if provided
  if os.environ.get(_ENV_VAR_API_KEY):
    headers['x-api-key'] = os.environ[_ENV_VAR_API_KEY]

  # Send the request and verify the request succeeded
  if post:
    req = six.moves.urllib.request.Request(
      req_url,
      data=json.dumps(req_json).encode('utf-8'),
      headers=headers)
  else:
    req = six.moves.urllib.request.Request(req_url, headers=headers)
  try:
    res = six.moves.urllib.request.urlopen(req)
  except six.moves.urllib.error.HTTPError as e:
    raise ValueError(
        'Response error: An HTTP {} code was returned by the REST API. '
        'Printing response\n\n{}'.format(e.code, e.read()))
  if isinstance(res, six.moves.urllib.error.HTTPError):
      raise ValueError(
          'Response error: An HTTP {} code was returned by the REST API. '
          'Printing response\n\n{}'.format(res.code, res.msg))
  # Get the JSON
  res_json = json.loads(res.read())
  if not use_payload:
    return res_json
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
