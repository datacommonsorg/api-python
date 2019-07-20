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

"""Data Commons Populations API Extension.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from types import MethodType
from .datacommons import DCFrame
from . import utils

def PopulationsExtension(frame):
  """ The DataCommons populations API extension.
  Allows users to do frame.function_defined_in_this_extension
  as if these extension functions were built in frame funcs.
  """
  frame.get_populations = MethodType(get_populations, frame)
  frame.get_observations = MethodType(get_observations, frame)
  return frame

def get_populations(self,
                    seed_col_name,
                    new_col_name,
                    population_type,
                    rows=100,
                    location_property='location',
                    **kwargs):
  """Create a new column with population dcid, in place.
  The current pandas dataframe should include a column containing entity IDs
  for geo entities. This function populates a new column with
  population dcids corresponding to the geo entities.

  Args:
    seed_col_name: The column name that contains entity (ids) that the added
      populations belong to.
    new_col_name: New column name.
    population_type: Population type like "Person".
    max_rows: The maximum number of rows returned by the query results.
    **kwargs: keyword properties to define the population.

  Raises:
    ValueError: when input argument is not valid.
  """
  if seed_col_name not in self._dataframe:
    raise ValueError('{} is not a valid seed column.'.format(seed_col_name))
  if new_col_name in self._dataframe:
    raise ValueError('{} is already a column name.'.format(new_col_name))

  # Get the variable names
  seed_col_var = '?' + seed_col_name.replace(' ', '_')
  new_col_var = '?' + new_col_name.replace(' ', '_')
  labels = {seed_col_var: seed_col_name, new_col_var: new_col_name}

  # Get the type of the container place
  seed_col_type = self._col_types[seed_col_name]
  new_col_type = 'StatisticalPopulation'
  type_hint = {seed_col_var: seed_col_type, new_col_var: new_col_type}

  # Get allowed DCIDs
  dcids = list(self._dataframe[seed_col_name])
  if not dcids:
    # All entries in the seed column are empty strings. The new column should
    # contain no entries.
    self._dataframe[new_col_name] = ''
    self._col_types[new_col_name] = new_col_type
    return

  # Construct the query
  query = utils.DatalogQuery()
  # Specify which variables to SELECT
  query.add_variable(seed_col_var, new_col_var)
  # Add constraints to the SELECT SQL query
  query.add_constraint('?node', 'typeOf', seed_col_type)
  query.add_constraint('?pop', 'typeOf', 'StatisticalPopulation')
  query.add_constraint('?node', 'dcid', dcids)
  query.add_constraint('?node', 'dcid', seed_col_var)
  query.add_constraint('?pop', location_property, '?node')
  query.add_constraint('?pop', 'dcid', new_col_var)
  query.add_constraint('?pop', 'populationType', population_type)

  pv_pairs = sorted(kwargs.items())
  idx = 0
  for idx, pv in enumerate(pv_pairs, 1):
    query.add_constraint('?pop', 'p{}'.format(idx), pv[0]) # ? need str(pv[0])
    query.add_constraint('?pop', 'v{}'.format(idx), pv[1]) # ditto
  query.add_constraint('?pop', 'numConstraints', idx)

  # Perform the query and merge the results
  new_frame = DCFrame(datalog_query=query, labels=labels, type_hint=type_hint, rows=rows)
  self.merge(new_frame)

def get_observations(self,
                     seed_col_name,
                     new_col_name,
                     observation_date,
                     measured_property,
                     stats_type=None,
                     clean_data=False,
                     rows=100):
  """Create a new column with values for an observation of the given property.
  The current pandas dataframe should include a column containing population
  dcids. This function populates a new column with observations of the
  populations' measured property. A column containing geo ids of type City
  can be used instead of population dcids.

  Args:
    seed_col_name: The column that contains the population dcid or city geo id.
    new_col_name: New column name.
    observations_date: The date of the observation (in 'YYY-mm-dd' form).
    measured_property: observation measured property.
    stats_type: Statistical type like "Median"
    clean_data: A flag to convert to numerical types and filter out any NaNs.
    rows: The maximum number of rows returned by the query results.

  Raises:
    ValueError: when input argument is not valid.
  """
  if seed_col_name not in self._dataframe:
    raise ValueError('{} is not a valid seed column.'.format(seed_col_name))
  if new_col_name in self._dataframe:
    raise ValueError('{} is already a column name.'.format(new_col_name))

  # Get the variable names
  seed_col_var = '?' + seed_col_name.replace(' ', '_')
  new_col_var = '?' + new_col_name.replace(' ', '_')
  labels = {seed_col_var: seed_col_name, new_col_var: new_col_name}

  # Get the type of the container place
  seed_col_type = self._col_types[seed_col_name]
  new_col_type = 'Observation'
  type_hint = {seed_col_var: seed_col_type, new_col_var: new_col_type}

  # Make sure the seed column can have observations
  assert seed_col_type == 'StatisticalPopulation' or seed_col_type == 'City', (
        'Parent entity should be StatisticalPopulation' or 'City')

  # Get allowed DCIDs
  dcids = list(self._dataframe[seed_col_name])
  if not dcids:
    self._dataframe[new_col_name] = ''
    self._col_types[new_col_name] = new_col_type
    return

  if stats_type is None:
      stats_type = 'measured'

  # Construct the query
  query = utils.DatalogQuery()
  # Specify which variables to SELECT
  query.add_variable(seed_col_var, new_col_var)
  # Add constraints to the SELECT SQL query
  query.add_constraint('?pop', 'typeOf', seed_col_type)
  query.add_constraint('?o', 'typeOf', 'Observation')
  query.add_constraint('?pop', 'dcid', dcids)
  query.add_constraint('?pop', 'dcid', seed_col_var)
  query.add_constraint('?o', 'observedNode', '?pop')
  query.add_constraint('?o', 'observationDate', '\"{}\"'.format(observation_date))
  query.add_constraint('?o', 'measuredProperty', measured_property)
  query.add_constraint('?o', '{}Value'.format(stats_type), new_col_var)
  measurement_method = None
  if measured_property == 'prevalence':
    measurement_method = 'CDC_CrudePrevalence'
  elif measured_property == 'unemploymentRate':
    measurement_method = 'BLSSeasonallyUnadjusted'
  if measurement_method:
    query.add_constraint('?o', 'measurementMethod', measurement_method)

  # Check if data should be cleaned
  clean_func = None
  if clean_data:
    type_func = utils.convert_type(new_col_var, 'float')
    nan_func = utils.drop_nan(new_col_var)
    clean_func = utils.compose_process(type_func, nan_func)

  # Perform the query and merge the results
  new_frame = DCFrame(datalog_query=query, labels=labels, type_hint=type_hint, rows=rows)
  self.merge(new_frame, process=clean_func)
