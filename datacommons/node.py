# Copyright 2022 Google Inc.
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
""" API to request node information.
"""

from typing import Dict, List

from datacommons.requests import _post
from datacommons.utils import _get_direction


def properties(nodes: List[str], is_out: bool = True) -> Dict[str, List[str]]:
  """Retrieves all the properties for a list of nodes.

  Note this only returns the property labels, not the values.
  Args:
      nodes: List of DCIDs.
      is_out: Whether to return out going properties.
  Returns:
      A dict keyed by node DCID, with the values being a list of properties
      for the queried node.
  """
  resp = _post(f'/v1/bulk/properties/{_get_direction(is_out)}', {
      'nodes': nodes,
  })
  result = {}
  for item in resp.get('data', []):
    node, properties = item['node'], item.get('properties', [])
    result[node] = properties
  return result


def property_values(nodes: List[str],
                    property: str,
                    is_out: bool = True) -> Dict[str, List[str]]:
  """Retrieves the property values for a list of nodes.
  Args:
      nodes: List of DCIDs.
      property: The property label to query for.
      is_out: Whether the property is out going.
  Returns:
      A dict keyed by node DCID, with the values being a list of values
      for the queried property.
  """
  resp = _post(f'/v1/bulk/property/values/{_get_direction(is_out)}', {
      'nodes': nodes,
      'property': property,
  })
  result = {}
  for item in resp.get('data', []):
    node, values = item['node'], item.get('values', [])
    result[node] = []
    for v in values:
      if 'dcid' in v:
        result[node].append(v['dcid'])
      else:
        result[node].append(v['value'])
  return result


def triples(nodes: List[str],
            is_out: bool = True) -> Dict[str, Dict[str, List[object]]]:
  """Retrieves the triples for a node.
  Args:
      nodes: List of DCIDs.
      is_out: Whether the returned property is out going for the queried
          nodes.
  Returns:
      A two level dict keyed by node DCID, then by the arc property, with
      a list of values or DCIDs.
  """
  resp = _post(f'/v1/bulk/triples/{_get_direction(is_out)}',
               data={'nodes': nodes})
  result = {}
  for item in resp.get('data', []):
    node, triples = item['node'], item.get('triples', {})
    result[node] = {}
    for property, other_nodes in triples.items():
      result[node][property] = other_nodes.get('nodes', [])
  return result
