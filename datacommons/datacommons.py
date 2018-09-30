"""Data Commons Public API.
"""

import collections
import datetime
from itertools import product
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

    self._inv_prop_type = collections.defaultdict(dict)
    for out_type, prop_map in self._prop_type.items():
      for prop_name, in_type in prop_map.items():
        self._inv_prop_type[in_type][prop_name] = out_type

    self._inited = True

  def Query(self, datalog_query, max_rows=100):
    """Performs a query returns results as a table.

    Args:
      datalog_query: string representing datalog query in [TODO(shanth): link]
      max_rows: max number returned rows.

    Returns:
      A pandas.DataFrame with the selected variables in the query as the
      the column names. Entities with multiple values for the queried properties
      will create multiple rows in the output data frame.

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

      # Collect cell values into a single list
      cell_vals = []
      for key, cell in zip(header, cells):
        if not cell:
          cell_vals.append([''])
        else:
          cell_vals.append(cell['value'])

      # Iterate through the cartesian product adding rows
      for new_elems in product(*cell_vals):
        for idx, key in enumerate(header):
          result_dict[key].append(new_elems[idx])

    return pd.DataFrame(result_dict)[header]

  def AddProperty(self,
                  pd_table,
                  prop_name,
                  seed_col_name,
                  new_col_name,
                  incoming_prop=False,
                  outgoing_prop=False,
                  max_rows=100):
    """Populate a new column in the existing pandas dataframe with the values
    of the given property name.

    The existing pandas dataframe should contain entity ids for certain types.
    Added data would be the property values of entities specified as ids in
    "seed_col_name".

    Args:
      pd_table: Pandas dataframe that contains entity information.
      prop_name: The property to add to the table.
      seed_col_name: The column name containg entity (ids) that the property
        should associate to. The "seed_col_name" should always be a column name
        in the "pd_table"
      new_col_name: The column name for the new column added to "pd_table"
      incoming_prop: If set, denotes that "prop_name" is a property that points
        towards the entities described in the seed column. Either this or
        "outgoing_prop" must be set.
      outgoing_prop: If set, denotes that "prop_name" is a property that points
        away from the entities described in the seed column. Either this or
        "incoming_prop" must be set.
      max_rows: The maximum number of rows returned by querying observations.
        All rows that return no result will be filled with the empty string.

    Returns:
      A pandas.DataFrame with an additional column added.
    """
    assert self._inited, 'Initialization was unsuccessful, cannot execute Query'
    assert (incoming_prop or outgoing_prop), 'Must specify if property is incoming or outgoing'

    seed_col = pd_table[seed_col_name]
    seed_type = seed_col[0]
    dcids = seed_col[1:]

    # Generate the query based on incoming or outgoing property
    if incoming_prop:
      query = ('SELECT ?{seed_col_name} ?{new_col_name},'
               'typeOf ?node {seed_type},'
               'dcid ?node {dcids},'
               'dcid ?node ?{seed_col_name},'
               '{prop_name} ?{new_col_name} ?node ')
      new_col_type = self._inv_prop_type[seed_type][prop_name]
    else:
      query = ('SELECT ?{seed_col_name} ?{new_col_name},'
               'typeOf ?node {seed_type},'
               'dcid ?node {dcids},'
               'dcid ?node ?{seed_col_name},'
               '{prop_name} ?node ?{new_col_name}')
      new_col_type = self._prop_type[seed_type][prop_name]

    # Get the query and type of the new column then query and merge.
    query = query.format(prop_name=prop_name,
                         seed_type=seed_type,
                         seed_col_name=seed_col_name,
                         new_col_name=new_col_name,
                         dcids=' '.join(set(dcids)))

    return self._query_and_merge(pd_table, query, seed_col_name, new_col_name,
                                 new_col_type,
                                 max_rows=max_rows)

  # ---------------------------------------------------------------------------
  # CONVENIENCE FUNCTIONS
  # ---------------------------------------------------------------------------

  def GetObservations(self,
                      pd_table,
                      seed_col_name,
                      new_col_name,
                      prop_name,
                      population_type,
                      start_time,
                      end_time,
                      max_rows=100,
                      **kwargs):
    """Adds a column of observations for populations contained in entities
    denoted by the seed column.

    Args:
      pd_table: A Pandas dataframe where the observations will be added.
      seed_col_name: The name of the seed column contained in the Pandas
        dataframe.
      new_col_name: The name of the column to be added to the dataframe.
      prop_name: The observation's property name such as "count".
      population_type: The type described by the observation's population such
        as "Person".
      start_time: The start time for when the observation was taken.
      end_time: The end time for when the observation was taken.
      max_rows: The maximum number of rows returned by querying observations.
        All rows that return no result will be filled with the empty string.
      **kwargs: Any additional keyword properties defining the population such
        "income" or "education".

    Returns:
      A pandas.DataFrame with an additional column populated by observations
      on the population specified by "population_type", "start_time", "end_time"
      and any additional keyword arguments with populations contained entities
      denoted by the "seed_col_name".
    """
    assert self._inited, 'Initialization was unsuccessful, cannot execute Query'

    seed_col = pd_table[seed_col_name]
    seed_type = seed_col[0]
    dcids = seed_col[1:]

    # Create the query
    start_epoch = int((datetime.datetime.strptime(start_time, '%Y-%m-%d') -
                       EPOCH_TIME).total_seconds()) * 1000000
    end_epoch = int((datetime.datetime.strptime(end_time, '%Y-%m-%d') -
                     EPOCH_TIME).total_seconds()) * 1000000
    query = ('SELECT ?{seed_col_name} ?{new_col_name},'
             'typeOf ?node {seed_type},'
             'typeOf ?pop Population,'
             'typeOf ?o Observation,'
             'dcid ?node {dcids},'
             'dcid ?node ?{seed_col_name},'
             'place ?pop ?node,'
             'populationType ?pop {population_type},'
             'observedNode ?o ?pop,'
             'observedNode ?o ?pop,'
             'startTime ?o {start_time},'
             'endTime ?o {end_time},'
             'measuredProperty ?o {prop_name},'
             '{prop_name}Value ?o ?{new_col_name},').format(
                 seed_col_name=seed_col_name,
                 new_col_name=new_col_name,
                 seed_type=seed_type,
                 dcids=' '.join(set(dcids)),
                 population_type=population_type,
                 prop_name=prop_name,
                 start_time=start_epoch,
                 end_time=end_epoch)
    pv_pairs = sorted(kwargs.items())
    idx = 0
    for idx, pv in enumerate(pv_pairs, 1):
      query += 'p{} ?pop {},'.format(idx, pv[0])
      query += 'v{} ?pop {},'.format(idx, pv[1])
    query += 'numConstraints ?pop {}'.format(idx)

    # Query and merge
    return self._query_and_merge(pd_table, query, seed_col_name, new_col_name,
                                 'Text',
                                 max_rows=max_rows)

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
    return pd.concat([type_row, dcid_column], ignore_index=True)

  # ---------------------------------------------------------------------------
  # UTILITY FUNCTIONS
  # ---------------------------------------------------------------------------

  def _query_and_merge(self,
                       pd_table,
                       query,
                       seed_col_name,
                       new_col_name,
                       new_col_type,
                       max_rows=100):
    """Executes the given query and joins a new column with the result and type
    data along the values in the seed column.

    Args:
      pd_table: A Pandas dataframe where the new data will be added.
      query: The query to be executed. This query must output a column with the
        same name as "seed_col_name"
      seed_col_name: The name of the seed column (i.e. the column to join the
        new data against).
      new_col_name: The name of the new column.
      new_col_type: The type of the entities contained in the new column.
      max_rows: The maximum number of rows returned by querying observations.
        All rows that return no result will be filled with the empty string.

    Returns:
      A pandas.DataFrame with an additional column containing the result of the
      query joined with elements in the seed column.
    """
    assert self._inited, 'Initialization was unsuccessful, cannot execute Query'

    # Run the query and update the data. If the left-joins does not fill a value
    # for a row, then replace the NaN with an empty string.
    curr_data = pd_table[1:]
    query_data = self.Query(query, max_rows=max_rows)

    new_data = pd.merge(curr_data, query_data, how='left', on=seed_col_name)
    new_data[new_col_name] = new_data[new_col_name].fillna('')

    # Create the type row
    new_type_row = pd_table.loc[0].to_frame().T
    new_type_row[new_col_name] = new_col_type

    return pd.concat([new_type_row, new_data], ignore_index=True)
