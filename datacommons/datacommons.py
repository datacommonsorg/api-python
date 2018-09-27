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

"""Data Commons Public API.
"""

import collections
import datetime
from . import _auth
import pandas as pd


_CLIENT_ID = ('381568890662-ff9evnle0lj0oqttr67p2h6882d9ensr'
              '.apps.googleusercontent.com')
_CLIENT_SECRET = '77HJA4S5m48Z98UKkW_o-jAY'
_API_ROOT = 'https://datcom-api-sandbox.appspot.com'

EPOCH_TIME = datetime.datetime(1970, 1, 1)


class Client(object):
  """Provides Data Commons API."""

  def __init__(self,
               client_id=_CLIENT_ID,
               client_secret=_CLIENT_SECRET,
               api_root=_API_ROOT):
    self._service = _auth.do_auth(client_id, client_secret, api_root)

    response = self._service.get_prop_type(body={}).execute()
    self._prop_type = collections.defaultdict(dict)
    for t in response.get('type_info', []):
      self._prop_type[t['node_type']][t['prop_name']] = t['prop_type']
    self._inited = True

  def Query(self, datalog_query, max_rows=100):
    """Performs a query returns results as a table.

    Args:
      datalog_query: string representing datalog query in [TODO(shanth): link]
      max_rows: max number returned rows.

    Returns:
      A pandas.DataFrame with the selected variables in the query as the
      the column names and each cell containing a list of values.

    Raises:
      RuntimeError: some problem with executing query (hint in the string)
    """
    assert self._inited, 'Initialization was unsuccessful, cannot execute Query'

    try:
      response = self._service.query(body={
          'query': datalog_query,
          'options': {
              'row_count_limit': max_rows
          }
      }).execute()
    except Exception as e:  # pylint: disable=broad-except
      raise RuntimeError('Failed to execute query: {}'.format(e))

    header = response.get('header', [])
    rows = response.get('rows', [])

    result_dict = collections.defaultdict(list)
    for row in rows:
      cells = row.get('cells', [])

      if len(cells) != len(header):
        raise RuntimeError('Response #cells mismatches #header: {}'
                           .format(response))

      for key, cell in zip(header, cells):
        if not cell:
          result_dict[key].append('')
        else:
          result_dict[key].append(cell['value'])

    return pd.DataFrame(result_dict)[header]

  def GetCities(self, state, max_rows=500):
    """Get a list of city dcids.

    Args:
      state: A string of state the cities contained in.
      max_rows: max number of returend results.

    Returns:
      A pandas.DataFrame with city dcids
    """
    assert self._inited, 'Initialization was unsuccessful, cannot execute Query'
    query = ('SELECT ?city,'
             'typeOf ?node City,'
             'dcid ?node ?city,'
             'containedInPlace ?node ?county,'
             'containedInPlace ?county ?state,'
             'name ?state "{}"').format(state)
    type_row = pd.DataFrame(data=[{'city': 'City'}])
    dcid_column = self.Query(query, max_rows)
    dcid_column['city'] = dcid_column['city'].apply(lambda x: x[0])
    return pd.concat([type_row, dcid_column], ignore_index=True)

  def GetStates(self, country, max_rows=500):
    """Get a list of state dcids.

    Args:
      country: A string of the country states contained in.
      max_rows: max number of returend results.

    Returns:
      A pandas.DataFrame with city dcids
    """
    assert self._inited, 'Initialization was unsuccessful, cannot execute Query'
    query = ('SELECT ?state,'
             'typeOf ?node State,'
             'dcid ?node ?state,'
             'containedInPlace ?node ?country,'
             'name ?country "{}"').format(country)
    type_row = pd.DataFrame(data=[{'state': 'State'}])
    dcid_column = self.Query(query, max_rows)
    dcid_column['state'] = dcid_column['state'].apply(lambda x: x[0])
    return pd.concat([type_row, dcid_column], ignore_index=True)

  def AddColumn(self,
                pd_table,
                col_name,
                parent_col_name,
                prop_name,
                population_type=None,
                start_time=None,
                end_time=None,
                **kwargs):
    """Add a new column to an existing pandas dataframe.

    The existing pandas dataframe should contain entity ids for certain types.
    Added data would be a property values of exsting entities.

    Args:
      pd_table: Pandas dataframe that contains entity information.
      col_name: New column name.
      parent_col_name: The column name that contains entity (ids) that the added
        properties belong to.
      prop_name: The property to add to the table..
      population_type: If set, the added column is about population statistics,
        and this is the population type like "Person".
      **kwargs: keyword properties to define the population.

    Returns:
      A pandas.DataFrame with an additional column added.
    """
    assert self._inited, 'Initialization was unsuccessful, cannot execute Query'
    parent_col = pd_table[parent_col_name]
    parent_type = parent_col[0]
    dcids = parent_col[1:]

    if population_type:
      node_type = 'Text'
      start_epoch = int((datetime.datetime.strptime(start_time, '%Y-%m-%d') -
                         EPOCH_TIME).total_seconds()) * 1000000
      end_epoch = int((datetime.datetime.strptime(end_time, '%Y-%m-%d') -
                       EPOCH_TIME).total_seconds()) * 1000000
      query = ('SELECT ?dcid ?{col_name},'
               'typeOf ?node {parent_type},'
               'typeOf ?pop Population,'
               'typeOf ?o Observation,'
               'dcid ?node {dcids},'
               'dcid ?node ?dcid,'
               'place ?pop ?node,'
               'populationType ?pop {population_type},'
               'observedNode ?o ?pop,'
               'observedNode ?o ?pop,'
               'startTime ?o {start_time},'
               'endTime ?o {end_time},'
               'measuredProperty ?o {prop_name},'
               '{prop_name}Value ?o ?{col_name},').format(
                   col_name=col_name,
                   parent_type=parent_type,
                   dcids=' '.join(dcids),
                   population_type=population_type,
                   prop_name=prop_name,
                   start_time=start_epoch,
                   end_time=end_epoch)
      pv = collections.OrderedDict(sorted(kwargs.items()))
      index = 0
      for p, v in pv.items():
        index += 1
        query += 'p{} ?pop {},'.format(index, p)
        query += 'v{} ?pop {},'.format(index, v)
      query += 'numConstraints ?pop {}'.format(index)

    else:
      node_type = self._prop_type[parent_type][prop_name]
      query = ('SELECT ?dcid ?{col_name},'
               'typeOf ?node {parent_type},'
               'dcid ?node {dcids},'
               'dcid ?node ?dcid,'
               '{prop_name} ?node ?{col_name}').format(
                   prop_name=prop_name,
                   parent_type=parent_type,
                   col_name=col_name,
                   dcids=' '.join(dcids))
    type_row = pd.DataFrame(data=[{col_name: node_type}])
    query_result = self.Query(query)
    # Only handle non-repeated fields
    query_result[col_name] = query_result[col_name].apply(
        lambda x: x[0] if x else '')
    prop_map = {
        row['dcid'][0]: row[col_name] for index, row in query_result.iterrows()
    }
    result_values = [prop_map.get(dcid, '') for dcid in dcids]
    result_column = pd.DataFrame(data={col_name: result_values})
    col_data = pd.concat([type_row, result_column], ignore_index=True)
    return pd.concat([pd_table, col_data], axis=1)
