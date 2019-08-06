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


# REST API endpoint root
_API_ROOT = "http://mixergrpc.endpoints.datcom-mixer.cloud.goog"

# REST API endpoint paths
_API_ENDPOINTS = {
  "query": "/query",
  "get_node": "/node",
  "get_property": "/node/property",
  "get_property_value": "/node/property-value",
  "get_triple": "/node/triple"
}

# Database paths
_BIG_QUERY_PATH = 'google.com:datcom-store-dev.dc_v3_clustered'

class Client(object):
  """ The basic DataCommons query client.

  The Client supports querying for property types and performing arbitrary
  datalog queries.
  """

  def __init__(self,
               db_path=_BIG_QUERY_PATH,
               client_id=_SANDBOX_CLIENT_ID,
               client_secret=_SANDBOX_CLIENT_SECRET,
               api_root=_SANDBOX_API_ROOT):
    self._db_path = db_path
    self._service = _auth.do_auth(client_id, client_secret, api_root)
    response = self._service.get_prop_type(body={}).execute()
    self._prop_type = defaultdict(dict)
    self._inv_prop_type = defaultdict(dict)
    for t in response.get('type_info', []):
      self._prop_type[t['node_type']][t['prop_name']] = t['prop_type']
      if t['prop_type'] != 'Text':
        self._inv_prop_type[t['prop_type']][t['prop_name']] = t['node_type']
    self._inited = True

  def property_type(self, ent_type, property, outgoing=True):
    """Returns the type pointed to by the given property and entity type.

    Args:
      ent_type: The entity type
      property: The property relating the given entity type to another.
      outgoing: Whether or not the property points away or towards the given
        entity. By default this is set to true.

    Returns:
      The type of the second enity contained in a triple formed from the given
      entity type and property. Returns none if no such property, entity type
      combination exists.
    """
    if outgoing and property in self._prop_type:
      return self._prop_type[ent_type][property]
    elif not outgoing and property in self._inv_prop_type:
      return self._inv_prop_type[ent_type][property]
    elif not outgoing and property in _PARENT_TYPES:
      return _PARENT_TYPES[property]
    return None

  def query(self, datalog_query, rows=100):
    """ Returns a Pandas DataFrame with the results of the given datalog query.

    Args:
      datalog_query: String representing datalog query in [TODO(shanth): link]
      rows: Max number of returned rows. Set rows to -1 to return all results.

    Returns:
      A Pandas.DataFrame with the selected variables in the query as the
      the column names. If the query returns multiple values for a property then
      the result is flattened into multiple rows.

    Raises:
      RuntimeError: some problem with executing query (hint in the string)
    """
    assert self._inited, 'Initialization was unsuccessful, cannot execute Query'

    # Append the options
    options = {}
    if self._db_path:
      options['db'] = self._db_path
    if rows >= 0:
      options['row_count_limit'] = rows

    # Send the query to the DataCommons query service
    try:
      response = self._service.query_table(body={
          'query': datalog_query,
          'options': options
      }).execute()
    except Exception as e:
      msg = 'Failed to execute query:\n  Query: {}\n  Error: {}'.format(datalog_query, e)
      raise RuntimeError(msg)

    # Format and return the result as a DCFrame
    header = response.get('header', [])
    header = [h.lstrip('?') for h in header]
    rows = response.get('rows', [])
    result_dict = OrderedDict([(h, []) for h in header])
    for row in rows:
      for col in row['cols']:
        result_dict[col['key']].append(col['value'])

    # Create the Pandas DataFrame
    return pd.DataFrame(result_dict).drop_duplicates()


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
      The following query asks for names of three states:
      `California <https://browser.datacommons.org/kg?dcid=geoId/06>`_,
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
