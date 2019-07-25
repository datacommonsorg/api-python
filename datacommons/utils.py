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


# ------------------------ SELECT AND PROCESS HELPERS -------------------------


def convert_type(col_names, dtype):
  """ Converts values in a given column to the given type.

  Args:
    col_names: The column or columns to convert
    dtype: Data type or a dictionary from column name to data type.

  Returns: A process function that converts the column to a given type.
  """
  if isinstance(col_names, str):
    col_names = [col_names]
  def process(pd_frame):
    for name in col_names:
      pd_frame[name] = pd.to_numeric(pd_frame[name])
    return pd_frame
  return process

def drop_nan(col_names):
  """ Drops rows containing NAN as a value in columns in col_names.

  Args:
    col_names: single column name or a list of column names.
  """
  if isinstance(col_names, str):
    col_names = [col_names]
  def process(pd_frame):
    return pd_frame.dropna(subset=col_names)
  return process

def delete_column(*cols):
  """ Returns a function that deletes the given column from a frame.

  Args:
    cols: Columns to delete from the data frame.

  Returns:
    A function that deletes columns in the given Pandas DataFrame.
  """
  def process(pd_frame):
    for col in cols:
      if col in pd_frame:
        pd_frame = pd_frame.drop(col, axis=1)
    return pd_frame
  return process

def compose_select(*select_funcs):
  """ Returns a filter function composed of the given selectors.

  Args:
    select_funcs: Functions to compose.

  Returns:
    A filter function which returns True iff all select_funcs return True.
  """
  def select(row):
    return all(select_func(row) for select_func in select_funcs)
  return select

def compose_process(*process_funcs):
  """ Returns a process function composed of the given functions.

  Args:
    process_funcs: Functions to compose.

  Returns:
    A process function which performs each function in the order given.
  """
  def process(pd_frame):
    for process_func in process_funcs:
      pd_frame = process_func(pd_frame)
    return pd_frame
  return process


# ------------------------------- OTHER HELPERS -------------------------------


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
