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

Various functions that can aid in the extension of the DataCommons API.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import defaultdict

import pandas as pd

import base64
import json
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
  'get_observations': '/node/observations'
}

# The default value to limit to
_MAX_LIMIT = 100


# ------------------------- PANDAS UTILITY FUNCTIONS --------------------------


def flatten_frame(pd_frame, cols=[]):
  """ Expands each cell in a Pandas DataFrame containing a list of values.

  Args:
    pd_frame: The Pandas DataFrame.
  """
  if not cols:
    cols = list(pd_frame.columns)
  for col in cols:
    if col not in pd_frame:
      raise ValueError('Column {} is not in data frame.'.format(col))
    if any(isinstance(v, list) for v in pd_frame[col]):
      # TODO: Uncomment after colab supports pandas 0.25
      # pd_frame = pd_frame._explode(col)
      pd_frame = _explode(pd_frame, col)
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
  return pd_frame.dropna().reset_index(drop=True)


# ------------------------- INTERNAL HELPER FUNCTIONS -------------------------


def _format_response(response, compress=False):
  """ Returns the json payload in a response from the mixer. """
  res_json = response.json()
  if 'code' in res_json and res_json['code'] != 0:
    raise ValueError('Response error: {}'.format(res_json['message']))
  elif 'payload' not in res_json:
    raise ValueError('Response error: Payload not found "{}"'.format(response))

  # If the payload is compressed, decompress and decode it
  payload = res_json['payload']
  if compress:
    payload = zlib.decompress(
      base64.b64decode(payload), 16 + zlib.MAX_WBITS)
  return json.loads(payload)


def _format_expand_payload(payload, new_key, must_exist=[]):
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


def _flatten_results(result, default_value=None):
  """ Formats results to map to a single value or default value if empty. """
  flattened = {}
  for k, v in result.items():
    if len(v) > 1:
      raise ValueError(
        'Expected one result, but more returned: {}'.format(v))
    if len(v) == 1:
      flattened[k] = v[0]
    elif default_value is not None:
      flattened[k] = default_value
  return flattened


def _convert_dcids_type(dcids):
  """ Amends dcids list type and creates the approprate request dcids list. """
  # Create the requests dcids list.
  if isinstance(dcids, list):
    req_dcids = dcids
  elif isinstance(dcids, pd.Series):
    req_dcids = list(dcids)
  elif isinstance(dcids, pd.DataFrame):
    # Assume user did df[[col]] instead of df[col]
    # Or user had to use single-col dataframe for Reticulate
    # Take the first column as a series
    dcids = dcids.iloc[:,0]
    req_dcids = list(dcids)
  else:
    raise ValueError(
      'dcids parameter must either be of type list or pandas.Series.')
  return dcids, req_dcids


def _explode(pd_frame, column):
  """ Expands a list inside a Pandas cell. """
  matches = [i for i, n in enumerate(pd_frame.columns) if n == column]
  col_idx = matches[0]

  def helper(d):
    row = list(d.values[0])
    bef = row[:col_idx]
    aft = row[col_idx + 1:]
    col = row[col_idx]
    z = [bef + [c] + aft for c in col]
    return pd.DataFrame(z)

  col_idx += len(pd_frame.index.shape)
  index_names = list(pd_frame.index.names)
  column_names = list(index_names) + list(pd_frame.columns)
  return (pd_frame
          .reset_index()
          .groupby(level=0, as_index=0)
          .apply(helper)
          .rename(columns=lambda i: column_names[i])
          .set_index(index_names))


def _print_header(label):
  """ Prints a pretty header with the given label. """
  print('\n' + '-' * 80)
  print(label)
  print('-' * 80 + '\n')
