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
""" Data Commons Python API Core.

Provides primitive operations for working with collections of nodes. For a
collection of nodes identified by their dcids, this submodule implements the
following:

- Getting all property labels
- Getting all property values
- Getting all triples
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import defaultdict

import datacommons.utils as utils

# ----------------------------- WRAPPER FUNCTIONS -----------------------------


def get_property_labels(dcids, out=True):
  """ Returns the labels of properties defined for the given :code:`dcids`.

  Args:
    dcids (:obj:`iterable` of :obj:`str`): A list of nodes identified by their
      dcids.
    out (:obj:`bool`, optional): Whether or not the property points away from
      the given list of nodes.

  Returns:
    A :obj:`dict` mapping dcids to lists of property labels. If `out` is `True`,
    then property labels correspond to edges directed away from given nodes.
    Otherwise, they correspond to edges directed towards the given nodes.

  Raises:
    ValueError: If the payload returned by the Data Commons REST API is
      malformed.

  Examples:
    To get all outgoing property labels for
    `California <https://browser.datacommons.org/kg?dcid=geoId/06>`_ and
    `Colorado <https://browser.datacommons.org/kg?dcid=geoId/08>`_, we can write
    the following.

    >>> get_property_labels(['geoId/06', 'geoId/08'])
    {
      "geoId/06": [
        "containedInPlace",
        "geoId",
        "kmlCoordinates",
        "name",
        "provenance",
        "typeOf"
      ],
      "geoId/08",: [
        "containedInPlace",
        "geoId",
        "kmlCoordinates",
        "name",
        "provenance",
        "typeOf"
      ]
    }

    We can also get incoming property labels by setting `out=False`.

    >>> get_property_labels(['geoId/06', 'geoId/08'], out=False)
    {
      "geoId/06": [
        "addressRegion",
        "containedInPlace",
        "location",
        "overlapsWith"
      ],
      "geoId/08",: [
        "addressRegion",
        "containedInPlace",
        "location",
        "overlapsWith"
      ]
    }
  """
  # Generate the GetProperty query and send the request
  dcids = filter(lambda v: v==v, dcids)  # Filter out NaN values
  dcids = list(dcids)
  url = utils._API_ROOT + utils._API_ENDPOINTS['get_property_labels']
  payload = utils._send_request(url, req_json={'dcids': dcids})

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
  """ Returns property values of given :code:`dcids` along the given property.

  Args:
    dcids (:obj:`iterable` of :obj:`str`): dcids to get property values for.
    prop (:obj:`str`): The property to get property values for.
    out (:obj:`bool`, optional): A flag that indicates the property is directed
      away from the given nodes when set to true.
    value_type (:obj:`str`, optional): A type to filter returned property values
      by.
    limit (:obj:`int`, optional): The maximum number of property values returned
      aggregated over all given nodes.

  Returns:
    Returned property values are formatted as a :obj:`dict` from a given dcid
    to a list of its property values.

  Raises:
    ValueError: If the payload returned by the Data Commons REST API is
      malformed.

  Examples:
    We would like to get the `name` of a list of states specified by their dcid:
    `geoId/06 <https://browser.datacommons.org/kg?dcid=geoId/06>`_,
    `geoId/21 <https://browser.datacommons.org/kg?dcid=geoId/21>`_, and
    `geoId/24 <https://browser.datacommons.org/kg?dcid=geoId/24>`_

    First, let's try specifying the :code:`dcids` as a :obj:`list` of
    :obj:`str`.

    >>> get_property_values(["geoId/06", "geoId/21", "geoId/24"], "name")
    {
      "geoId/06": ["California"],
      "geoId/21": ["Kentucky"],
      "geoId/24": ["Maryland"],
    }
  """
  # Convert the dcids field and format the request to GetPropertyValue
  dcids = filter(lambda v: v==v, dcids)  # Filter out NaN values
  dcids = list(dcids)
  if out:
    direction = 'out'
  else:
    direction = 'in'

  req_json = {
    'dcids': dcids,
    'property': prop,
    'limit': limit,
    'direction': direction
  }
  if value_type:
    req_json['value_type'] = value_type

  # Send the request
  url = utils._API_ROOT + utils._API_ENDPOINTS['get_property_values']
  payload = utils._send_request(url, req_json=req_json)

  # Create the result format for when dcids is provided as a list.
  unique_results = defaultdict(set)
  for dcid in dcids:
    # Get the list of nodes based on the direction given.
    nodes = []
    if out:
      if dcid in payload and 'out' in payload[dcid]:
        nodes = payload[dcid]['out']
    else:
       if dcid in payload and 'in' in payload[dcid]:
          nodes = payload[dcid]['in']

    # Add nodes to unique_results if it is not empty
    for node in nodes:
      if 'dcid' in node:
        unique_results[dcid].add(node['dcid'])
      elif 'value' in node:
        unique_results[dcid].add(node['value'])

  # Make sure each dcid is in the results dict, and convert all sets to lists.
  results = {dcid: sorted(list(unique_results[dcid])) for dcid in dcids}

  return results


def get_triples(dcids, limit=utils._MAX_LIMIT):
  """ Returns all triples associated with the given :code:`dcids`.

  A knowledge graph can be described as a collection of `triples` which are
  3-tuples that take the form `(s, p, o)`. Here `s` and `o` are nodes in the
  graph called the *subject* and *object* respectively while `p` is the property
  label of a directed edge from `s` to `o` (sometimes also called the
  *predicate*).

  Args:
    dcids (:obj:`iterable` of :obj:`str`): A list of dcids to get triples for.
    limit (:obj:`int`, optional): The maximum total number of triples to get.

  Returns:
    A :obj:`dict` mapping dcids to a :obj:`list` of triples `(s, p, o)` where
    `s`, `p`, and `o` are instances of :obj:`str` and either the subject
    or object is the mapped dcid.

  Raises:
    ValueError: If the payload returned by the Data Commons REST API is
      malformed.

  Examples:
    We would like to get five triples associated with
    `California <https://browser.datacommons.org/kg?dcid=geoId/06>`_

    >>> get_triples(["geoId/06"], limit=5)
    {
      "geoId/06": [
        ("geoId/06", "name", "California"),
        ("geoId/06", "typeOf", "State"),
        ("geoId/06", "geoId", "06"),
        ("geoId/0687056", "containedInPlace", "geoId/06"),
        ("geoId/0686440", "containedInPlace", "geoId/06")
      ]
    }
  """
  # Generate the GetTriple query and send the request.
  dcids = filter(lambda v: v==v, dcids)  # Filter out NaN values
  dcids = list(dcids)
  url = utils._API_ROOT + utils._API_ENDPOINTS['get_triples']
  payload = utils._send_request(url, req_json={'dcids': dcids, 'limit': limit})

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
