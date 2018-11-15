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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import defaultdict
import datetime
import json
from itertools import product
from . import _auth
import pandas as pd

_PLACES = ('City', 'County', 'State', 'Country', 'Continent')

_CLIENT_ID = ('66054275879-a0nalqfe2p9shlv4jpra5jekfkfnr8ug.apps.googleusercontent.com')
_CLIENT_SECRET = 'fuJy7JtECndEXgtQA46hHqqa'
_API_ROOT = 'https://datcom-api.appspot.com'

_MICRO_SECONDS = 1000000
_EPOCH_START = datetime.datetime(year=1970, month=1, day=1)


def _year_epoch_micros(year):
  """Get the timestamp of the start of a year in micro seconds.

  Args:
    year: An integer number of the year.

  Returns:
    Timestamp of the start of a year in micro seconds.
  """
  now = datetime.datetime(year=year, month=1, day=1)

  return int((now - _EPOCH_START).total_seconds()) * _MICRO_SECONDS


def _date_epoch_micros(date_string):
  """Get the timestamp of the date string in micro seconds.

  Args:
    date_string: An string of date

  Returns:
    Timestamp of the start of a year in micro seconds.
  """
  now = datetime.datetime.strptime(date_string, '%Y-%m-%d')
  return int((now - _EPOCH_START).total_seconds()) * _MICRO_SECONDS


class Client(object):
  """Provides Data Commons API."""

  def __init__(self,
               client_id=_CLIENT_ID,
               client_secret=_CLIENT_SECRET,
               api_root=_API_ROOT):
    self._service = _auth.do_auth(client_id, client_secret, api_root)
    response = self._service.get_prop_type(body={}).execute()
    self._prop_type = defaultdict(dict)
    self._inv_prop_type = defaultdict(dict)
    for t in response.get('type_info', []):
      self._prop_type[t['node_type']][t['prop_name']] = t['prop_type']
      if t['prop_type'] != 'Text':
        self._inv_prop_type[t['prop_type']][t['prop_name']] = t['node_type']
    self._inited = True

  def query(self, datalog_query, max_rows=100):
    """Performs a query returns results as a table.

    Args:
      datalog_query: string representing datalog query in [TODO(shanth): link]
      max_rows: max number of returned rows.

    Returns:
      A pandas.DataFrame with the selected variables in the query as the
      the column names. If the query returns multiple values for a property then
      the result is flattened into multiple rows.

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
      raise RuntimeError('Failed to execute query: %s' % e)

    header = response.get('header', [])
    rows = response.get('rows', [])
    result_dict = {header: [] for header in header}
    for row in rows:
      cells = row.get('cells', [])
      if len(cells) != len(header):
        raise RuntimeError(
            'Response #cells mismatches #header: {}'.format(response))
      cell_values = []
      for key, cell in zip(header, cells):
        if not cell:
          cell_values.append([''])
        else:
          try:
            cell_values.append(cell['value'])
          except KeyError:
            raise RuntimeError('No value in cell: {}'.format(row))

      # Iterate through the cartesian product to flatten the query results.
      for values in product(*cell_values):
        for idx, key in enumerate(header):
          result_dict[key].append(values[idx])

    return pd.DataFrame(result_dict)[header]

  def expand(self,
             pd_table,
             arc_name,
             seed_col_name,
             new_col_name,
             outgoing=True,
             max_rows=100):
    """Create a new column with values for the given property.

    The existing pandas dataframe should include a column containing entity IDs
    for a certain schema.org type. This function populates a new column with
    property values for the entities and adds additional rows if a property has
    repeated values.

    Args:
      pd_table: Pandas dataframe that contains entity information.
      arc_name: The property to add to the table.
      seed_col_name: The column name that contains entity (ids) that the added
        properties belong to.
      new_col_name: New column name.
      outgoing: Set this flag if the property points away from the entities
        denoted by the seed column.
      max_rows: The maximum number of rows returned by the query results.

    Returns:
      A pandas.DataFrame with the additional column and rows added.

    Raises:
      ValueError: when input argument is not valid.
    """
    assert self._inited, 'Initialization was unsuccessful, cannot execute query'

    if seed_col_name not in pd_table:
      raise ValueError('%s is not a valid seed column name' % seed_col_name)
    if new_col_name in pd_table:
      raise ValueError(
          '%s is already a column name in the data frame' % new_col_name)

    seed_col = pd_table[seed_col_name]
    seed_col_type = seed_col[0]
    assert seed_col_type != 'Text', 'Parent entity should not be Text'

    # Determine the new column type
    if outgoing:
      if arc_name not in self._prop_type[seed_col_type]:
        raise ValueError(
            '%s does not have outgoing property %s' % (seed_col_type, arc_name))
      new_col_type = self._prop_type[seed_col_type][arc_name]
    else:
      if arc_name not in self._inv_prop_type[seed_col_type]:
        raise ValueError(
            '%s does not have incoming property %s' % (seed_col_type, arc_name))
      new_col_type = self._inv_prop_type[seed_col_type][arc_name]

    dcids = ' '.join(seed_col[1:]).strip()
    if not dcids:
      # All entries in the seed column are empty strings. The new column should
      # contain no entries.
      pd_table[new_col_name] = ""
      pd_table[new_col_name][0] = new_col_type
      return pd_table
    if outgoing:
      query = ('SELECT ?{seed_col_name} ?{new_col_name},'
               'typeOf ?node {seed_col_type},'
               'dcid ?node {dcids},'
               'dcid ?node ?{seed_col_name},'
               '{arc_name} ?node ?{new_col_name}').format(
                   arc_name=arc_name,
                   seed_col_name=seed_col_name,
                   seed_col_type=seed_col_type,
                   new_col_name=new_col_name,
                   dcids=dcids)
    else:
      query = ('SELECT ?{seed_col_name} ?{new_col_name},'
               'typeOf ?node {seed_col_type},'
               'dcid ?node {dcids},'
               'dcid ?node ?{seed_col_name},'
               '{arc_name} ?{new_col_name} ?node').format(
                   arc_name=arc_name,
                   seed_col_name=seed_col_name,
                   seed_col_type=seed_col_type,
                   new_col_name=new_col_name,
                   dcids=dcids)

    # Run the query and merge the results.
    return self._query_and_merge(
        pd_table,
        query,
        seed_col_name,
        new_col_name,
        new_col_type,
        max_rows=max_rows)

  # ----------------------- OBSERVATION QUERY FUNCTIONS -----------------------

  def get_instances(self, col_name, instance_type, max_rows=100):
    """Get a list of instance dcids for a given type.

    Args:
      col_name: Column name for the returned column.
      instance_type: String of the instance type.
      max_rows: Max number of returend rows.

    Returns:
      A pandas.DataFrame with instance dcids.
    """
    assert self._inited, 'Initialization was unsuccessful, cannot execute Query'
    query = ('SELECT ?{col_name},'
             'typeOf ?node {instance_type},'
             'dcid ?node ?{col_name}').format(
                 col_name=col_name, instance_type=instance_type)
    type_row = pd.DataFrame(data=[{col_name: instance_type}])

    try:
      dcid_column = self.query(query, max_rows)
    except RuntimeError as e:
      raise RuntimeError('Execute query\n%s\ngot an error:\n%s' % (query, e))

    return pd.concat([type_row, dcid_column], ignore_index=True)

  def get_populations(self,
                      pd_table,
                      seed_col_name,
                      new_col_name,
                      population_type,
                      max_rows=100,
                      **kwargs):
    """Create a new column with population dcid.

    The existing pandas dataframe should include a column containing entity IDs
    for geo entities. This function populates a new column with
    population dcid corresponding to the geo entity.

    Args:
      pd_table: Pandas dataframe that contains geo entity dcids.
      seed_col_name: The column name that contains entity (ids) that the added
        properties belong to.
      new_col_name: New column name.
      population_type: Population type like "Person".
      max_rows: The maximum number of rows returned by the query results.
      **kwargs: keyword properties to define the population.

    Returns:
      A pandas.DataFrame with an additional column added.

    Raises:
      ValueError: when input argument is not valid.
    """
    assert self._inited, 'Initialization was unsuccessful, cannot execute query'
    if seed_col_name not in pd_table:
      raise ValueError('%s is not a valid seed column name' % seed_col_name)
    if new_col_name in pd_table:
      raise ValueError(
          '%s is already a column name in the data frame' % new_col_name)

    seed_col = pd_table[seed_col_name]
    seed_col_type = seed_col[0]
    assert seed_col_type != 'Text', 'Parent entity should not be Text'

    # Create the datalog query for the requested observations
    dcids = ' '.join(seed_col[1:]).strip()
    if not dcids:
      pd_table[new_col_name] = ""
      pd_table[new_col_name][0] = 'Population'
      return pd_table
    query = ('SELECT ?{seed_col_name} ?{new_col_name},'
             'typeOf ?node {seed_col_type},'
             'typeOf ?pop Population,'
             'dcid ?node {dcids},'
             'dcid ?node ?{seed_col_name},'
             'location ?pop ?node,'
             'dcid ?pop ?{new_col_name},'
             'populationType ?pop {population_type},').format(
                 new_col_name=new_col_name,
                 seed_col_name=seed_col_name,
                 seed_col_type=seed_col_type,
                 dcids=dcids,
                 population_type=population_type)
    pv_pairs = sorted(kwargs.items())
    idx = 0
    for idx, pv in enumerate(pv_pairs, 1):
      query += 'p{} ?pop {},'.format(idx, pv[0])
      query += 'v{} ?pop {},'.format(idx, pv[1])
    query += 'numConstraints ?pop {}'.format(idx)

    # Run the query and merge the results.
    return self._query_and_merge(
        pd_table,
        query,
        seed_col_name,
        new_col_name,
        'Population',
        max_rows=max_rows)

  def get_observations(self,
                       pd_table,
                       seed_col_name,
                       new_col_name,
                       start_date,
                       end_date,
                       measured_property,
                       stats_type,
                       max_rows=100):
    """Create a new column with values for an observation of the given property.

    The existing pandas dataframe should include a column containing entity IDs
    for a certain schema.org type. This function populates a new column with
    property values for the entities.

    Args:
      pd_table: Pandas dataframe that contains entity information.
      seed_col_name: The column that contains the population dcid.
      new_col_name: New column name.
      start_date: The start date of the observation (in 'YYY-mm-dd' form).
      end_date: The end date of the observation (in 'YYY-mm-dd' form).
      measured_property: observation measured property.
      stats_type: Statistical type like "Median"
      max_rows: The maximum number of rows returned by the query results.

    Returns:
      A pandas.DataFrame with an additional column added.

    Raises:
      ValueError: when input argument is not valid.
    """
    assert self._inited, 'Initialization was unsuccessful, cannot execute query'
    if seed_col_name not in pd_table:
      raise ValueError('%s is not a valid seed column name' % seed_col_name)
    if new_col_name in pd_table:
      raise ValueError(
          '%s is already a column name in the data frame' % new_col_name)

    seed_col = pd_table[seed_col_name]
    seed_col_type = seed_col[0]
    assert seed_col_type == 'Population' or seed_col_type == 'City', (
        'Parent entity should be Population' or 'City')

    # Create the datalog query for the requested observations
    dcids = ' '.join(seed_col[1:]).strip()
    if not dcids:
      pd_table[new_col_name] = ""
      pd_table[new_col_name][0] = 'Observation'
      return pd_table
    query = ('SELECT ?{seed_col_name} ?{new_col_name},'
             'typeOf ?pop {seed_col_type},'
             'typeOf ?o Observation,'
             'dcid ?pop {dcids},'
             'dcid ?pop ?{seed_col_name},'
             'observedNode ?o ?pop,'
             'startTime ?o {start_time},'
             'endTime ?o {end_time},'
             'measuredProperty ?o {measured_property},'
             '{stats_type}Value ?o ?{new_col_name},').format(
                 seed_col_type=seed_col_type,
                 new_col_name=new_col_name,
                 seed_col_name=seed_col_name,
                 dcids=dcids,
                 measured_property=measured_property,
                 stats_type=stats_type,
                 start_time=_date_epoch_micros(start_date),
                 end_time=_date_epoch_micros(end_date))
    # Run the query and merge the results.
    return self._query_and_merge(
        pd_table,
        query,
        seed_col_name,
        new_col_name,
        'Observation',
        max_rows=max_rows)

  # -------------------------- CACHING FUNCTIONS --------------------------

  def read_dataframe(self, file_name):
    """Read a previously saved pandas dataframe.

      User can only read previously saved data file with the same authentication
      email.

    Args:
      file_name: The saved file name.

    Returns:
      A pandas dataframe.

    Raises:
      RuntimeError: when failed to read the dataframe.
    """
    assert self._inited, 'Initialization was unsuccessful, cannot execute Query'
    try:
      response = self._service.read_dataframe(file_name=file_name).execute()
    except Exception as e:  # pylint: disable=broad-except
      raise RuntimeError('Failed to read dataframe: {}'.format(e))
    return pd.read_json(json.loads(response['data']), dtype=False)

  def save_dataframe(self, pd_dataframe, file_name):
    """Saves pandas dataframe for later retrieving.

      Each aunthentication email has its own scope for saved dataframe. Write
      with same file_name overwrites previously saved dataframe.

    Args:
      pd_dataframe: A pandas.DataFrame.
      file_name: The saved file name.

    Raises:
      RuntimeError: when failed to save the dataframe.
    """
    assert self._inited, 'Initialization was unsuccessful, cannot execute Query'
    data = json.dumps(pd_dataframe.to_json())
    try:
      response = self._service.save_dataframe(body={
          'data': data,
          'file_name': file_name
      }).execute()
    except Exception as e:  # pylint: disable=broad-except
      raise RuntimeError('Failed to save dataframe: {}'.format(e))
    return response['file_name']

  # -------------------------- OTHER QUERY FUNCTIONS --------------------------

  def get_cities(self, state, new_col_name, max_rows=100):
    """Get a list of city dcids in a given state.

    Args:
      state: Name of the state name.
      new_col_name: Column name for the returned city column.
      max_rows: Max number of returend rows.

    Returns:
      A pandas.DataFrame with city dcids.
    """
    assert self._inited, 'Initialization was unsuccessful, cannot execute Query'
    query = ('SELECT ?{new_col_name},'
             'typeOf ?node City,'
             'dcid ?node ?{new_col_name},'
             'containedInPlace ?node ?county,'
             'containedInPlace ?county ?state,'
             'name ?state "{state}"').format(
                 new_col_name=new_col_name, state=state)
    type_row = pd.DataFrame(data=[{new_col_name: 'City'}])

    try:
      dcid_column = self.query(query, max_rows)
    except RuntimeError as e:
      raise RuntimeError('Execute query\n%s\ngot an error:\n%s' % (query, e))

    return pd.concat([type_row, dcid_column], ignore_index=True)

  def get_states(self, country, new_col_name, max_rows=100):
    """Get a list of state dcids.

    Args:
      country: A string of the country states contained in.
      new_col_name: Column name for the returned state column.
      max_rows: max number of returend results.

    Returns:
      A pandas.DataFrame with state dcids.
    """
    assert self._inited, 'Initialization was unsuccessful, cannot execute Query'
    query = ('SELECT ?{new_col_name},'
             'typeOf ?node State,'
             'dcid ?node ?{new_col_name},'
             'containedInPlace ?node ?country,'
             'name ?country "{country}"').format(
                 new_col_name=new_col_name, country=country)
    type_row = pd.DataFrame(data=[{new_col_name: 'State'}])

    try:
      dcid_column = self.query(query, max_rows)
    except RuntimeError as e:
      raise RuntimeError('Execute query %s got an error:\n%s' % (query, e))

    return pd.concat([type_row, dcid_column], ignore_index=True)


  def get_places_in(self, place_type, container_dcid, col_name, max_rows=100):
    """Get a list of places that are contained in a higher level geo places.

    Args:
      place_type: The place type, like "City".
      container_dcid: The dcid of the container place.
      col_name: Column name for the returned state column.
      max_rows: max number of returend results.

    Returns:
      A pandas.DataFrame with dcids of the contained place.
    """
    assert self._inited, 'Initialization was unsuccessful, cannot execute Query'
    assert place_type in _PLACES, 'Input place types are not supported'

    # Get the type of the container place.
    type_query = 'SELECT ?type, dcid ?node {dcid}, subType ?node ?type'.format(
        dcid=container_dcid)
    query_result = self.query(type_query)
    assert query_result['type'].count() == 1, (
        'Type of the container dcid not found')
    container_type = query_result['type'][0]

    # Sanity check the type information.
    place_type_ind = _PLACES.index(place_type)
    container_type_ind = _PLACES.index(container_type)
    assert container_type_ind > place_type_ind, (
        'Requested place type should be of a lower level than the container')

    # Do the actual query.
    query = ('SELECT ?{col_name},'
             'typeOf ?node_{place_type} {place_type},'
             'dcid ?node_{place_type} ?{col_name},').format(
                 col_name=col_name,
                 place_type=place_type)
    for i in range(place_type_ind, container_type_ind):
      query += 'containedInPlace ?node_{child} ?node_{parent},'.format(
          child=_PLACES[i], parent=_PLACES[i+1])
    query += 'dcid ?node_{container_type} "{container_dcid}"'.format(
        container_type=container_type, container_dcid=container_dcid)
    try:
      dcid_column = self.query(query, max_rows)
    except RuntimeError as e:
      raise RuntimeError('Execute query %s got an error:\n%s' % (query, e))

    type_row = pd.DataFrame(data=[{col_name: place_type}])
    return pd.concat([type_row, dcid_column], ignore_index=True)


  # ------------------------ INTERNAL HELPER FUNCTIONS ------------------------

  def _query_and_merge(self,
                       pd_table,
                       query,
                       seed_col_name,
                       new_col_name,
                       new_col_type,
                       max_rows=100):
    """A utility function that executes the given query and adds a new column.

    It sends an request to the API server to execute the given query and joins
    a new column with the result and type data along with the values in the seed
    column.

    Args:
      pd_table: A Pandas dataframe where the new data will be added.
      query: The query to be executed. This query must output a column with the
             same name as "seed_col_name"
      seed_col_name: The name of the seed column (i.e. the column to join the
                     new data against).
      new_col_name: The name of the new column.
      new_col_type: The type of the entities contained in the new column.
      max_rows: The maximum number of rows returned by the query results.

    Returns:
      A pandas.DataFrame with an additional column containing the result of the
      query joined with elements in the seed column.
    """
    try:
      query_result = self.query(query, max_rows=max_rows)
    except RuntimeError as e:
      raise RuntimeError('Execute query \n%s\ngot an error:\n%s' % (query, e))

    new_data = pd.merge(
        pd_table[1:], query_result, how='left', on=seed_col_name)
    new_data[new_col_name] = new_data[new_col_name].fillna('')
    new_type_row = pd_table.loc[0].to_frame().T
    new_type_row[new_col_name] = new_col_type

    return pd.concat([new_type_row, new_data], ignore_index=True)
