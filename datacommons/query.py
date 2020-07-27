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
""" Data Commons Python API Query Module.

Implements functions for sending graph queries to the Data Commons Graph.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from datacommons.utils import _API_ROOT, _API_ENDPOINTS, _ENV_VAR_API_KEY

import json
import os
import six.moves.urllib.error
import six.moves.urllib.request

# ----------------------------- WRAPPER FUNCTIONS -----------------------------


def query(query_string, select=None):
  """ Returns the results of executing a SPARQL query on the Data Commons graph.

  Args:
    query_string (:obj:`str`): The SPARQL query string.
    select (:obj:`func` accepting a row in the query result): A function that
      selects rows to be returned by :code:`query`. This function accepts a row
      in the results of executing :code:`query_string` and return True if and
      only if the row is to be returned by :code:`query`. The row passed in as
      an argument is represented as a :obj:`dict` that maps a query variable in
      :code:`query_string` to its value in the given row.

  Returns:
    A table, represented as a :obj:`list` of rows, resulting from executing the
    given SPARQL query. Each row is a :obj:`dict` mapping query variable to its
    value in the row. If `select` is not `None`, then a row is included in the
    returned :obj:`list` if and only if `select` returns :obj:`True` for that
    row.

  Raises:
    ValueError: If the payload returned by the Data Commons REST API is
      malformed.

  Examples:
    We would like to query for the name associated with three states identified
    by their dcids
    `California <https://browser.datacommons.org/kg?dcid=geoId/06>`_,
    `Kentucky <https://browser.datacommons.org/kg?dcid=geoId/21>`_, and
    `Maryland <https://browser.datacommons.org/kg?dcid=geoId/24>`_.

    >>> query_str = '''
    ... SELECT ?name ?dcid
    ... WHERE {
    ...   ?a typeOf Place .
    ...   ?a name ?name .
    ...   ?a dcid ("geoId/06" "geoId/21" "geoId/24") .
    ...   ?a dcid ?dcid
    ... }
    ... '''
    >>> result = query(query_str)
    >>> for r in result:
    ...   print(r)
    {"?name": "Maryland", "?dcid": "geoId/24"}
    {"?name": "Kentucky", "?dcid": "geoId/21"}
    {"?name": "California", "?dcid": "geoId/06"}

    Optionally, we can specify which rows are returned by setting :code:`select`
    like so. The following returns all rows where the name is "Maryland".

    >>> selector = lambda row: row['?name'] == 'Maryland'
    >>> result = query(query_str, select=selector)
    >>> for r in result:
    ...   print(r)
    {"?name": "Maryland", "?dcid": "geoId/24"}
  """

  req_url = _API_ROOT + _API_ENDPOINTS['query']
  headers = {
    'Content-Type': 'application/json'
  }
  if os.environ.get(_ENV_VAR_API_KEY):
    headers['x-api-key'] = os.environ[_ENV_VAR_API_KEY]

  req = six.moves.urllib.request.Request(
    req_url,
    data=json.dumps({'sparql': query_string}).encode("utf-8"),
    headers=headers)

  try:
    res = six.moves.urllib.request.urlopen(req)
  except six.moves.urllib.error.HTTPError as e:
    raise ValueError('Response error {}:\n{}'.format(e.code, e.read()))

  # Verify then store the results.
  res_json = json.loads(res.read())

  # Iterate through the query results
  header = res_json.get('header')
  if header is None:
    raise ValueError('Ill-formatted response: does not contain a header.')
  result_rows = []
  for row in res_json.get('rows', []):
    # Construct the map from query variable to cell value.
    row_map = {}
    for idx, cell in enumerate(row.get('cells', [])):
      if idx > len(header):
        raise ValueError(
          'Query error: unexpected cell {}'.format(cell))
      if 'value' not in cell:
        raise ValueError(
          'Query error: cell missing value {}'.format(cell))
      cell_var = header[idx]
      row_map[cell_var] = cell['value']
    # Add the row to the result rows if it is selected
    if select is None or select(row_map):
      result_rows.append(row_map)
  return result_rows
