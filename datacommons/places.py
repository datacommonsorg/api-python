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

"""DataCommons Places data API Extension.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from types import MethodType
from .datacommons import DCFrame
from . import utils

_PLACES = {
  'City': 'County',
  'CensusTract': 'County',
  'County': 'State',
  'State': 'Country',
  'Country': 'Continent'
}

def PlacesExtension(frame):
  """ The DataCommons places API extension. """
  frame.get_places_in = MethodType(get_places_in, frame)
  return frame

def get_places_in(self, seed_col_name, new_col_name, new_col_type, rows=100):
  """ Adds a new column to the frame places contained in seed column entities.

  Args:
    seed_col_name: The column name containing DCIDs to get contained entities.
    new_col_name: The column name for where the results are stored.
    new_col_type: The type of place to query for.
    rows: max number of returend results.
  Returns:
    A pandas.DataFrame with dcids of the contained place.
  """
  if seed_col_name not in self._dataframe:
    raise ValueError('{} is not a valid seed column.'.format(seed_col_name))
  if new_col_name in self._dataframe:
    raise ValueError('{} is already a column name.'.format(new_col_name))
  if new_col_type not in _PLACES:
    raise ValueError('Place type {} is not supported.'.format(new_col_type))

  # Get the variable names
  seed_col_var = '?' + seed_col_name.replace(' ', '_')
  new_col_var = '?' + new_col_name.replace(' ', '_')
  labels = {seed_col_var: seed_col_name, new_col_var: new_col_name}

  # Get the type of the container place
  seed_col_type = self._col_types[seed_col_name]
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
  query.add_variable(seed_col_var, new_col_var)
  query.add_constraint('?node{}'.format(new_col_type), 'typeOf', new_col_type)
  query.add_constraint('?node{}'.format(new_col_type), 'dcid', new_col_var)

  # Construct chain of parent types
  curr_type = new_col_type
  parent_type = _PLACES[curr_type]
  while curr_type != seed_col_type:
    query.add_constraint('?node{}'.format(curr_type), 'containedInPlace', '?node{}'.format(parent_type))
    curr_type = parent_type
    if curr_type not in _PLACES:
      raise ValueError('{} is not contained in {}.'.format(new_col_type, seed_col_type))
    parent_type = _PLACES[curr_type]

  query.add_constraint('?node{}'.format(seed_col_type), 'dcid', dcids)
  query.add_constraint('?node{}'.format(seed_col_type), 'dcid', seed_col_var)

  # Perform the query and merge the results
  new_frame = DCFrame(datalog_query=query, labels=labels, type_hint=type_hint, rows=rows)
  self.merge(new_frame)
