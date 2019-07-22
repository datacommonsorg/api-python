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
""" DataCommons base public API.

Contains Client which connects to the DataCommons knowledge graph, DCNode which
wraps a node in the graph, and DCFrame which provides a tabular view of graph
data.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import defaultdict, OrderedDict
from . import _auth
from . import utils
import json
import pandas as pd

# Database paths
# TODO(antaresc): set default path to BQ path once query is stable.
_BIG_QUERY_PATH = 'google.com:datcom-store-dev.dc_v3_clustered'

# Standard API Server target
_CLIENT_ID = '66054275879-a0nalqfe2p9shlv4jpra5jekfkfnr8ug.apps.googleusercontent.com'
_CLIENT_SECRET = 'fuJy7JtECndEXgtQA46hHqqa'
_API_ROOT = 'https://datcom-api.appspot.com'

# Sandbox API Server target
_SANDBOX_CLIENT_ID = '381568890662-ff9evnle0lj0oqttr67p2h6882d9ensr.apps.googleusercontent.com'
_SANDBOX_CLIENT_SECRET = '77HJA4S5m48Z98UKkW_o-jAY'
_SANDBOX_API_ROOT = 'https://datcom-api-sandbox.appspot.com'

# Encode API Server target
_ENCODE_CLIENT_ID = '708273713739-42iak5pck92be9q6hrafmen12tf8eht5.apps.googleusercontent.com'
_ENCODE_CLIENT_SECRET = 'Rg5gFEE0nfodRToI1d4VwLoj'
_ENCODE_API_ROOT = 'https://datcom-api-encode.appspot.com'

_PARENT_TYPES = {
  'containedInPlace': 'Place'
}


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
    rows = response.get('rows', [])
    result_dict = OrderedDict([(h, []) for h in header])
    for row in rows:
      for col in row['cols']:
        result_dict[col['key']].append(col['value'])

    # Create the Pandas DataFrame
    return pd.DataFrame(result_dict).drop_duplicates()


class DCNode(object):
  """ Wraps a node found in the DataCommons knowledge graph. Supports the
  following functionalities.

  - Querying for properties that have this node as either a subject or object.
  - Querying for values in triples containing this node and a given property.
  - Querying for all triples containing this node.
  """
  def __init__(self,
               dcid,
               db_path=_BIG_QUERY_PATH,
               client_id=_SANDBOX_CLIENT_ID,
               client_secret=_SANDBOX_CLIENT_SECRET,
               api_root=_SANDBOX_API_ROOT):
    self._client = Client(db_path=db_path,
                          client_id=client_id,
                          client_secret=client_secret,
                          api_root=api_root)
    self._dcid = dcid

  def get_properties(self, outgoing=True):
    """ Returns a list of properties associated with this node.

    Args:
      outgoing: whether or not the node is a subject or object.
    """
    pass

  def get_property_values(self, property, outgoing=True):
    """ Returns a list of values mapped to this node with the given property.

    Args:
      outgoing: whether or not the node is a subject or object.
    """
    pass

  def get_triples(self):
    """ Returns a list of triples where this node is either a subject or object.

    Args:
      outgoing: whether or not the node is a subject or object.
    """
    pass


class DCFrame(object):
  """ Provides a tabular view of the DataCommons knowledge graph. """

  def __init__(self,
               file_name=None,
               datalog_query=None,
               labels=None,
               select=None,
               process=None,
               type_hint=None,
               rows=100,
               db_path=None,
               client_id=_SANDBOX_CLIENT_ID,
               client_secret=_SANDBOX_CLIENT_SECRET,
               api_root=_SANDBOX_API_ROOT):
    """ Initializes the DCFrame.

    A DCFrame can also be initialized by providing the file name of a cached
    frame or a datalog query. When a datalog query is provided, the results
    of the query are stored in the frame with selected variables set as the
    column names. Additional fields such as labels, select, process, etc. can
    be provided to manipuate the results of the datalog query before it is
    wrapped by the DCFrame.

    The DCFrame requires typing information for the columns that it maintains.
    If the frame is initialized from a query then either the query variable
    types must be inferrable from the query, or it must be provided in the type
    hint.

    Args:
      file_name: File name of a cached DCTable.
      datalog_query: Query object representing datalog query [TODO(shanth): link]
      labels: A map from the query variables to column names in the DCFrame.
      select: A function that takes in a row and returns true if the row in the
        result should be added to the final DCFrame. Functions should index into
        columns using column names prior to relabeling.
      process: A function that takes in a Pandas DataFrame. Can be used for
        post processing the results such as converting columns to certain types.
        Functions should index into columns using names prior to relabeling.
      type_hint: A map from column names to the type that the column contains.
      db_path: The path for the database to query.
      client_id: The API client id
      client_secret: The API client secret
      api_root: The API root url

    Raises:
      RuntimeError: some problem with executing query (hint in the string)
    """
    self._client = Client(db_path=db_path,
                          client_id=client_id,
                          client_secret=client_secret,
                          api_root=api_root)
    self._dataframe = pd.DataFrame()
    self._col_types = {}

    # Read the dataframe from cache if a file name is provided or initialize
    # from a datalog query if the query is provided
    if file_name:
      try:
        response = self._client._service.read_dataframe(
            file_name=file_name
        ).execute()
      except Exception as e:  # pylint: disable=broad-except
        raise RuntimeError('Failed to read "{}": {}'.format(file_name, e))

      # Inflate the json string.
      data = json.loads(response['data'])
      self._dataframe = pd.read_json(data['dataframe'])
      self._col_types = data['col_types']
    elif datalog_query:
      variables = datalog_query.variables()
      var_types = datalog_query.var_types()
      query_string = str(datalog_query)
      pd_frame = self._client.query(query_string, rows=rows)
      pd_frame = pd_frame.dropna()

      # If variable type is not provided in type_hint or from the query, infer
      # the type as text.
      for var in variables:
        if var not in var_types and (type_hint is None or var not in type_hint):
          var_types[var] = 'Text'

      # Processing is run the order of row filtering via select, table
      # manipulation via process, and column renaming via labels,
      if select:
        pd_frame = pd_frame[pd_frame.apply(select, axis=1)]
      if process:
        pd_frame = process(pd_frame)
      for col in pd_frame:
        # Set the column types and remap if the column labels are provided. Only
        # add types for columns that appear in the dataframe. This is critical
        # as "process" may delete columns from the query result.
        col_name = col
        if labels and col in labels:
          col_name = labels[col]
        if type_hint and col in type_hint:
          self._col_types[col_name] = type_hint[col]
        else:
          self._col_types[col_name] = var_types[col]
      if labels:
        pd_frame = pd_frame.rename(index=str, columns=labels)
      self._dataframe = pd_frame.reset_index(drop=True)

  def columns(self):
    """ Returns the set of column names for this frame.

    Returns:
      Set of column names for this frame.
    """
    return [col for col in self._dataframe]

  def types(self):
    """ Returns a map from column name to associated DataCommons type.

    Returns:
      Map from column name to column type.
    """
    return self._col_types

  def pandas(self, col_names=None, ignore_populations=False):
    """ Returns a copy of the data in this view as a Pandas DataFrame.

    Args:
      col_names: An optional list specifying which columns to extract.
      ignore_populations: Ignores all columns that have type
        StatisticalPopulation. col_names takes precedence over this argument

    Returns: A deep copy of the underlying Pandas DataFrame.
    """
    if not col_names:
      col_names = list(self._dataframe)
    if ignore_populations:
      col_names = list(filter(lambda name: self._col_types[name] != 'StatisticalPopulation', col_names))
    return self._dataframe[col_names].copy()

  def csv(self, col_names=None):
    """ Returns the data in this view as a CSV string.

    Args:
      col_names: An optional list specifying which columns to extract.

    Returns:
      The DataFrame exported as a CSV string.
    """
    if col_names:
      return self._dataframe[col_names].to_csv(index=False)
    return self._dataframe.to_csv(index=False)

  def tsv(self, col_names=None):
    """ Returns the data in this view as a TSV string.

    Args:
      col_names: An optional list specifying which columns to extract.

    Returns:
      The DataFrame exported as a TSV string.
    """
    if col_names:
      return self._dataframe[col_names].to_csv(index=False, sep='\t')
    return self._dataframe.to_csv(index=False, sep='\t')

  def rename(self, labels):
    """ Renames the columns of the DCFrame.

    Args:
      labels: A map from current to new column names.
    """
    col_types = {}
    for col in self._dataframe:
      col_name = col
      if col in labels:
        col_name = labels[col]
      col_types[col_name] = self._col_types[col]
    self._col_types = col_types
    self._dataframe = self._dataframe.rename(index=str, columns=labels)

  def add_column(self, col_name, col_type, col_vals):
    """ Adds a column containing the given values of the given type.

    Args:
      col_name: The name of the column
      col_type: The type of the column
      col_vals: The values in the given column
    """
    self._col_types[col_name] = col_type
    self._dataframe[col_name] = col_vals

  def expand(self, property, seed_col_name, new_col_name, new_col_type=None, outgoing=True, rows=100):
    """ Creates a new column containing values for the given property.

    For each entity in the given seed column, queries for entities related to
    the seed entity via the given property. Results are stored in a new column
    under the provided name. The seed column should contain only DCIDs.

    Args:
      property: The property to add to the table.
      seed_col_name: The column name that contains dcids that the added
        properties belong to.
      new_col_name: The new column name.
      new_col_type: The type contained by the new column. Provide this if the
        type is not immediately inferrable.
      outgoing: Set this flag if the seed property points away from the entities
        denoted by the seed column. That is the seed column serve as subjects
        in triples formed with the given property.
      rows: The maximum number of rows returned by the query results.

    Raises:
      ValueError: when input argument is not valid.
    """
    if seed_col_name not in self._dataframe:
      raise ValueError(
          'Expand error: {} is not a valid seed column.'.format(seed_col_name))
    if new_col_name in self._dataframe:
      raise ValueError(
          'Expand error: {} is already a column.'.format(new_col_name))

    # Get the seed column information
    seed_col = self._dataframe[seed_col_name]
    seed_col_type = self._col_types[seed_col_name]
    if seed_col_type == 'Text':
      raise ValueError(
          'Expand error: {} must contain DCIDs'.format(seed_col_name))

    # Determine the new column type
    if new_col_type is None:
      new_col_type = self._client.property_type(seed_col_type, property, outgoing=outgoing)
    if new_col_type is None and outgoing:
      new_col_type = 'Text'
    elif new_col_type is None:
      raise ValueError(
          'Expand error: {} does not have incoming property {}'.format(seed_col_type, property))

    # Get the list of DCIDs to query for
    dcids = list(seed_col)
    if not dcids:
      # All entries in the seed column are empty strings. The new column should
      # contain no entries.
      self._dataframe[new_col_name] = ''
      self._col_types[new_col_name] = new_col_type
      return

    # Construct the query
    seed_col_var = '?' + seed_col_name.replace(' ', '_')
    new_col_var = '?' + new_col_name.replace(' ', '_')
    labels = {seed_col_var: seed_col_name, new_col_var: new_col_name}
    type_hint = {seed_col_var: seed_col_type, new_col_var: new_col_type}

    query = utils.DatalogQuery()
    query.add_variable(seed_col_var, new_col_var)
    query.add_constraint('?node', 'typeOf', seed_col_type)
    query.add_constraint('?node', 'dcid', dcids)
    query.add_constraint('?node', 'dcid', seed_col_var)
    if outgoing:
      query.add_constraint('?node', property, new_col_var)
    else:
      query.add_constraint(new_col_var, property, '?node')

    # Create a new DCFrame and merge it in
    new_frame = DCFrame(datalog_query=query, rows=rows, labels=labels, type_hint=type_hint)
    self.merge(new_frame)

  def merge(self, frame, how='left', default=''):
    """ Joins the given frame into the current frame along shared column names.

    Args:
      frame: The DCFrame to merge in.
      how: Optional argument specifying the joins type to perform. Valid types
        include 'left', 'right', 'inner', and 'outer'
      default: The default place holder for an empty cell produced by the join.

    Raises:
      ValueError: if the given arguments are not valid. This may include either
        the given or current DCFrame does not contain the columns specified.
    """
    merge_on = set(self.columns()) & set(frame.columns())
    merge_on = list(merge_on)

    # If the current dataframe is empty, select the given dataframe. If the
    # tables have no columns in common, perform a cross join. Otherwise join on
    # common columns.
    if self._dataframe.empty:
      self._col_types = {}
      self._dataframe = frame._dataframe
    elif len(merge_on) == 0:
      # Construct a unique dummy column name
      cross_on = ''.join(self.columns() + frame.columns())

      # Perform the cross join
      curr_frame = self._dataframe.assign(**{cross_on: 1})
      new_frame = frame._dataframe.assign(**{cross_on: 1})
      merged = curr_frame.merge(new_frame)
      self._dataframe = merged.drop(cross_on, 1)
    else:
      # Verify that columns being merged have the same type
      for col in merge_on:
        if self._col_types[col] != frame._col_types[col]:
          raise ValueError(
              'Merge error: columns type mismatch for {}.\n  Current: {}\n  Given: {}'.format(col, self._col_types[col], frame._col_types[col]))

      # Merge dataframe, column types, and property maps
      self._dataframe = self._dataframe.merge(frame._dataframe, how=how, left_on=merge_on, right_on=merge_on)
      self._dataframe = self._dataframe.fillna(default)

    # Merge the types
    self._col_types.update(frame._col_types)

  def clear(self):
    """ Clears all the data stored in this extension. """
    self._col_types = {}
    self._dataframe = pd.DataFrame()

  def save(self, file_name):
    """ Saves the current DCFrame to the DataCommons cache with given file name.

    Args:
      file_name: The name used to store the current DCFrame.

    Returns:
      The file name that the

    Raises:
      RuntimeError: when failed to save the dataframe.
    """
    assert self._client._inited, 'Initialization was unsuccessful, cannot execute Query'

    # Saves the DCFrame to cache
    data = json.dumps({
      'dataframe': self._dataframe.to_json(),
      'col_types': self._col_types
    })
    try:
      response = self._client._service.save_dataframe(body={
          'data': data,
          'file_name': file_name
      }).execute()
    except Exception as e:  # pylint: disable=broad-except
      raise RuntimeError('Failed to save dataframe: {}'.format(e))
    return response['file_name']
