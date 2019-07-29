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
""" Data Commons base Python Client API.

Contains wrapper functions for get_property_labels, get_property_values, and
get_triples
"""

from collections import defaultdict

import pandas as pd

import datacommons.utils as utils
import requests

# ----------------------------- WRAPPER FUNCTIONS -----------------------------

def get_property_labels(dcid, outgoing=True, reload=False):
  """ Returns a list of properties associated with the given dcid.

  Args:
    dcid: The node to get property labels for.
    outgoing: Whether or not the node is a subject or object.
    reload: Whether or not to send the query to cache.
  """
  # Generate the GetProperty query and send the request
  params = "?dcid={}".format(dcid)
  if reload:
    params += "&reload=true"
  url = utils._API_ROOT + utils._API_ENDPOINTS['get_property'] + params
  res = requests.get(url)
  payload = utils.format_response(res)

  # Return the results based on the orientation
  if outgoing and 'outArcs' in payload:
    return payload['outArcs']
  elif not outgoing and 'inArcs' in payload:
    return payload['inArcs']
  return []

def get_property_values(dcids, prop, outgoing=True, value_type=None, reload=False, limit=utils._MAX_LIMIT):
  """ Returns values associated to given dcids via the given property.

  If the dcids field is a list, then the return value is a dictionary mapping
  dcid to the list of values associated with the given property.

  If the dcids field is a Pandas Series, then the return value is a Series where
  the i-th cell is the list of values associated with the given property for the
  i-th dcid.

  Args:
    dcids: A string, list of, or Pandas DataSeries of dcid.
    prop: The property to get the property values for.
    outgoing: Whether or not the dcids are subjects or objects.
    value_type: Filter returning values by a given type.
    reload: Whether or not to send the query to cache.
    limit: The maximum number of values to return.
  """
  # Convert the dcids field to the correct type
  if isinstance(dcids, str):
    dcids = [dcids]
  if isinstance(dcids, list):
    req_dcids = dcids
  if isinstance(dcids, pd.Series):
    req_dcids = list(dcids)

  # Format the request to GetPropertyValue
  req_json = {
    'dcids': req_dcids,
    'property': prop,
    'outgoing': outgoing,
    'reload': reload,
    'limit': limit
  }
  if value_type:
    req_json['value_type'] = value_type

  # Send the request
  url = utils._API_ROOT + utils._API_ENDPOINTS['get_property_value']
  res = requests.post(url, json=req_json)
  payload = utils.format_response(res)

  # Create the result format for when dcids is provided as a list.
  result = defaultdict(list)
  for dcid in dcids:
    if dcid in payload and prop in payload[dcid]:
      for node in payload[dcid][prop]:
        if 'dcid' in node:
          result[dcid].append(node['dcid'])
        elif 'value' in node:
          result[dcid].append(node['value'])
    else:
      result[dcid] = []

  # Format the result as a Series if a Pandas Series is provided.
  if isinstance(dcids, pd.Series):
    return pd.Series([result[dcid] for dcid in dcids])
  return dict(result)

def get_triples(dcid, reload=False, limit=utils._MAX_LIMIT):
  """ Returns a list of triples where the dcid is either a subject or object.

  The return value is a list of tuples (s, p, o) where s denotes the subject
  entity, p the property, and o the object.

  Args:
    dcid: The node to get triples for.
    reload: Whether or not to send the query to cache.
    limit: The maximum number of values to return.
  """
  # Generate the GetTriple query and send the request.
  params = "?dcid={}&limit_per_arc={}".format(dcid, limit)
  if reload:
    params += "&reload=true"
  url = utils._API_ROOT + utils._API_ENDPOINTS['get_triple'] + params
  res = requests.get(url)
  payload = utils.format_response(res)

  # Create a list of triples and return.
  triples = []
  for t in payload:
    if 'object_id' in t:
      triples.append((t['subject_id'], t['predicate'], t['object_id']))
    elif 'object_value' in t:
      triples.append((t['subject_id'], t['predicate'], t['object_value']))
  return triples
