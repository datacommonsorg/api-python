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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import defaultdict

import pandas as pd

import datacommons.utils as utils
import requests

# ----------------------------- WRAPPER FUNCTIONS -----------------------------


def get_property_labels(dcids, out=True):
  """ Returns the labels of properties defined for the given dcids.

  The return value is a dictionary mapping dcids to lists of property labels.

  Args:
    dcids: A list of nodes identified by their dcids.
    out: Whether or not the property points away from the given list of nodes.
  """
  # Generate the GetProperty query and send the request
  url = utils._API_ROOT + utils._API_ENDPOINTS['get_property_labels']
  res = requests.post(url, json={'dcids': dcids})
  payload = utils._format_response(res)

  # Return the results based on the orientation
  results = {}
  for dcid in dcids:
    if out:
      results[dcid] = payload[dcid]['outLabels']
    else:
      results[dcid] = payload[dcid]['inLabels']
  return results


def get_property_values(dcids,
                        prop,
                        out=True,
                        value_type=None,
                        limit=utils._MAX_LIMIT):
  """ Returns property values of the given dcids along the given property.

  When the dcids are given as a list, the returned property values are formatted
  as a map from given dcid to list of property values.

  When the dcids are given as a Pandas Series, the returned property values
  are formatted as a Pandas Series where the i-th entry corresponds to property
  values associated with the i-th given dcid. The cells of the returned series
  will always contain a list of property values.

  Args:
    dcids: A list or Pandas Series of dcids to get property values for.
    prop: The property to get property values for.
    out: An optional flag that indicates the property is oriented away from the
      given nodes if true.
    value_type: An optional parameter which filters property values by the given
      type.
    limit: An optional parameter which limits the total number of property
      values returned aggregated over all given nodes.
  """
  # Convert the dcids field and format the request to GetPropertyValue
  dcids, req_dcids = utils._convert_dcids_type(dcids)
  req_json = {
    'dcids': req_dcids,
    'property': prop,
    'outgoing': out,
    'limit': limit
  }
  if value_type:
    req_json['value_type'] = value_type

  # Send the request
  url = utils._API_ROOT + utils._API_ENDPOINTS['get_property_values']
  res = requests.post(url, json=req_json)
  payload = utils._format_response(res)

  # Create the result format for when dcids is provided as a list.
  results = defaultdict(list)
  for dcid in dcids:
    # Make sure each dcid is mapped to an empty list.
    results[dcid]

    # Add elements to this list as necessary.
    if dcid in payload and prop in payload[dcid]:
      for node in payload[dcid][prop]:
        if 'dcid' in node:
          results[dcid].append(node['dcid'])
        elif 'value' in node:
          results[dcid].append(node['value'])

  # Format the results as a Series if a Pandas Series is provided.
  if isinstance(dcids, pd.Series):
    return pd.Series([results[dcid] for dcid in dcids])
  return dict(results)


def get_triples(dcids, limit=utils._MAX_LIMIT):
  """ Returns all triples associated with the given dcids.

  The return value is a dictionary mapping given dcids to list of triples. The
  triples are repsented as 3-tuples (s, p, o) where "s" denotes the subject, "p"
  the property, and "o" the object.

  Args:
    dcids: A list of dcids to get triples for.
    limit: The maximum number of triples to get for each combination of property
      and type of property value.
  """
  # Generate the GetTriple query and send the request.
  url = utils._API_ROOT + utils._API_ENDPOINTS['get_triples']
  res = requests.post(url, json={'dcids': dcids, 'limit': limit})
  payload = utils._format_response(res)

  # Create a map from dcid to list of triples.
  results = defaultdict(list)
  for dcid in dcids:
    # Make sure each dcid is mapped to an empty list.
    results[dcid]

    # Add triples as appropriate
    for t in payload[dcid]:
      if 'objectId' in t:
        results[dcid].append(
          (t['subjectId'], t['predicate'], t['objectId']))
      elif 'objectValue' in t:
        results[dcid].append(
          (t['subjectId'], t['predicate'], t['objectValue']))
  return dict(results)
