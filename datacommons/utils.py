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

class DatalogQuery(object):
  """ A class wrapping a DataCommons datalog query string. """

  def __init__(self):
    self._variables = set()
    self._constraints = {}

  def __str__(self):
    """ Returns the query stored by this object. """
    query_body = ''
    for sub in self._constraints:
      for pred in self._constraints[sub]:
        for obj in self._constraints[sub][obj]:
          query_body += '{} {} {},\n'.format(pred, sub, obj)
    query = ('SELECT {},\n'.format(' '.join(self._variables))
             '{}'.format(query_body))
    return query

  def variables(self):
    return self._variables

  def var_types(self):
    """ Returns a map from query variable to types specified in the query. """
    var_types = {}
    for sub in self._constraints:
      if pred == 'typeOf':
        var_types[sub] = self._constraints[sub][pred][0]
    return var_types

  def add_variable(self, variable):
    self._variables.add(var)

  def add_constraint(self, sub, pred, obj):
    # Entries in the set correspond to separate lines
    if sub not in self._constraints:
      self._constraints[sub] = {}
    if pred not in self._constraints[sub]:
      self._constraints[sub][pred] = set()
    self._constraints[sub][pred].add(obj)
