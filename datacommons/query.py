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
""" Data Commons Python Client API Query Module.

Implements a wrapper object for sending SPARQL queries to the Data Commons
knowledge graph.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from datacommons.utils import _API_ROOT, _API_ENDPOINTS

import requests

# -----------------------------------------------------------------------------
# Query Class
# -----------------------------------------------------------------------------


class Query(object):
  """ A wrapper object that performs a SPARQL query on the Data Commons graph.

  Args:
    **kwargs: Valid keyword arguments include the following. At least one
      valid argument must be provided.

      - `sparql` (:obj:`str`): The SPARQL query string.

  Raises:
    ValueError: If an invalid keyword argument is provided.

  Example:
    To construct a :obj:`Query` object, do the following.

    >>> query_str = '''
    ...SELECT  ?name ?dcid
    ...WHERE {
    ...  ?a typeOf Place .
    ...  ?a name ?name .
    ...  ?a dcid ("geoId/06" "geoId/21" "geoId/24") .
    ...  ?a dcid ?dcid
    ...}
    ...'''
    >>> query = dc.Query(sparql=query_str)
  """

  # Valid query languages
  _SPARQL_LANG = 'sparql'
  _VALID_LANG = [_SPARQL_LANG]

  def __init__(self, **kwargs):
    """ Initializes a SPARQL query targeting the Data Commons graph. """
    if self._SPARQL_LANG in kwargs:
      self._query = kwargs[self._SPARQL_LANG]
      self._language = self._SPARQL_LANG
      self._result = None
    else:
      lang_str = ', '.join(self._VALID_LANG)
      raise ValueError(
        'Must provide one of the following languages: {}'.format(lang_str))

  def rows(self, select=None):
    """ Returns the result of executing the query as an iterator over all rows.

    Args:
      select (:obj:`func` accepting a `row` in the query result): A function
        that returns true if and only if a row in the query results should be
        kept. The argument for this function is a :obj:`dict` from query
        variable to its value in a given row.

    Yields:
      Rows from executing the query where each row is a :obj:`dict` mapping
      query variable to its value in the row. If `select` is not `None`, then
      the row is returned if and only if `select` returns :obj:`True`.

    Example:
      The following query asks for names of three states: California_,
      `Kentucky <https://browser.datacommons.org/kg?dcid=geoId/21>`_, and
      `Maryland <https://browser.datacommons.org/kg?dcid=geoId/24>`_.

      >>> query_str = '''
      ... SELECT  ?name ?dcid
      ... WHERE {
      ...   ?a typeOf Place .
      ...   ?a name ?name .
      ...   ?a dcid ("geoId/06" "geoId/21" "geoId/24") .
      ...   ?a dcid ?dcid
      ... }
      ... '''
      >>> query = dc.Query(sparql=query_str)
      >>> for r in query.rows():
      ...   print(r)
      {"?name": "Maryland", "?dcid": "geoId/24"}
      {"?name": "Kentucky", "?dcid": "geoId/21"}
      {"?name": "California", "?dcid": "geoId/06"}
    """
    # Execute the query if the results are empty.
    if not self._result:
      self._execute()

    # Iterate through the query results
    header = self._result['header']
    for row in self._result['rows']:
      # Construct the map from query variable to cell value.
      row_map = {}
      for idx, cell in enumerate(row['cells']):
        if idx > len(header):
          raise RuntimeError(
            'Query error: unexpected cell {}'.format(cell))
        if 'value' not in cell:
          raise RuntimeError(
            'Query error: cell missing value {}'.format(cell))
        cell_var = header[idx]
        row_map[cell_var] = cell['value']

      # Yield the row if it is selected
      if select is None or select(row_map):
        yield row_map

  def _execute(self):
    """ Execute the query.

    Raises:
      RuntimeError: on query failure (see error hint).
    """
    # Create the query request.
    if self._language == self._SPARQL_LANG:
      payload = {'sparql': self._query}
    url = _API_ROOT + _API_ENDPOINTS['query']
    res = requests.post(url, json=payload)

    # Verify then store the results.
    res_json = res.json()
    if 'message' in res_json:
      raise RuntimeError('Query error: {}'.format(res_json['message']))
    self._result = res.json()
