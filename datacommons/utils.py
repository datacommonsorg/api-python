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
""" DataCommons utilities library

Contains various functions that can aid in the extension of the DataCommons API.
"""

from collections import OrderedDict

import pandas as pd


class MeasuredValue:
  """ An enumeration of valid measured values in the DataCommons graph.

  A measured value in a Statistical Observation is the type of statistic being
  measured for example: "mean", "min", "max", etc. The associated string field
  is the DataCommons property used to query for the measured value.
  """
  MIN = 'minValue'
  MAX = 'maxValue'
  MEAN = 'meanValue'


class DatalogQuery(object):
  """ A class wrapping a DataCommons datalog query string. """

  def __init__(self):
    self._variables = []
    self._constraints = OrderedDict()

  def __str__(self):
    """ Returns the query stored by this object. """
    query_body = ''
    for sub in self._constraints:
      for pred in self._constraints[sub]:
        for obj in self._constraints[sub][pred]:
          if isinstance(obj, list):
            obj_vals = ' '.join(obj).strip()
            query_body += '{} {} {},\n'.format(pred, sub, obj_vals)
          else:
            query_body += '{} {} {},\n'.format(pred, sub, obj)
    query = 'SELECT {},\n{}'.format(' '.join(self._variables), query_body)
    return query

  def variables(self):
    """ Returns the set of variables. """
    return self._variables

  def var_types(self):
    """ Returns a map from query variable to types specified in the query. """
    # Get initial var types
    var_types = {}
    for sub in self._constraints:
      for pred in self._constraints[sub]:
        if pred == 'typeOf':
          var_types[sub] = self._constraints[sub][pred][0]

    # If the property is dcid, then the type can be carried over
    for sub in self._constraints:
      for pred in self._constraints[sub]:
        if pred == 'dcid':
          for obj in self._constraints[sub][pred]:
            if isinstance(obj, str) and sub in var_types:
              var_types[obj] = var_types[sub]
    return var_types

  def add_variable(self, *variables):
    """ Add variables to the query. """
    for var in variables:
      if var not in self._variables:
        # Maintaining order of the variables is important
        self._variables.append(var)

  def add_constraint(self, sub, pred, obj):
    """ Add constraints to the query. """
    # Entries in the set correspond to separate lines
    if sub not in self._constraints:
      self._constraints[sub] = OrderedDict()
    if pred not in self._constraints[sub]:
      self._constraints[sub][pred] = []
    self._constraints[sub][pred].append(obj)


# ------------------------ SELECT AND PROCESS HELPERS -------------------------


def convert_type(col_names, dtype):
  """ Converts values in a given column to the given type.

  Args:
    col_names: The column or columns to convert
    dtype: Data type or a dictionary from column name to data type.

  Returns: A process function that converts the column to a given type.
  """
  if isinstance(col_names, str):
    col_names = [col_names]
  def process(pd_frame):
    for name in col_names:
      pd_frame[name] = pd.to_numeric(pd_frame[name])
    return pd_frame
  return process

def drop_nan(col_names):
  """ Drops rows containing NAN as a value in columns in col_names.

  Args:
    col_names: single column name or a list of column names.
  """
  if isinstance(col_names, str):
    col_names = [col_names]
  def process(pd_frame):
    return pd_frame.dropna(subset=col_names)
  return process

def delete_column(*cols):
  """ Returns a function that deletes the given column from a frame.

  Args:
    cols: Columns to delete from the data frame.

  Returns:
    A function that deletes columns in the given Pandas DataFrame.
  """
  def process(pd_frame):
    for col in cols:
      if col in pd_frame:
        pd_frame = pd_frame.drop(col, axis=1)
    return pd_frame
  return process

def compose_select(*select_funcs):
  """ Returns a filter function composed of the given selectors.

  Args:
    select_funcs: Functions to compose.

  Returns:
    A filter function which returns True iff all select_funcs return True.
  """
  def select(row):
    return all(select_func(row) for select_func in select_funcs)
  return select

def compose_process(*process_funcs):
  """ Returns a process function composed of the given functions.

  Args:
    process_funcs: Functions to compose.

  Returns:
    A process function which performs each function in the order given.
  """
  def process(pd_frame):
    for process_func in process_funcs:
      pd_frame = process_func(pd_frame)
    return pd_frame
  return process
