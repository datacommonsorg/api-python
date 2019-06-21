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
"""Basic demo for the DCFrame.

This demo showcases basic features of the DCFrame class.
- Initializing a frame from query
- Relabeling the columns
- Getting column names and types
- Getting a Pandas DataFrame view
- Getting csv and tsv strings of the frame
"""

from datacommons.utils import DatalogQuery

import datacommons


def main():
  # Create the query
  query = DatalogQuery()
  query.add_variable('?id', '?lat', '?lon')
  query.add_constraint('?node', 'typeOf', 'City')
  query.add_constraint('?node', 'name', '"San Luis Obispo"')
  query.add_constraint('?node', 'dcid', '?id')
  query.add_constraint('?node', 'latitude', '?lat')
  query.add_constraint('?node', 'longitude', '?lon')

  # Create the frame
  labels = {'?id': 'city', '?lat': 'latitude', '?lon': 'longitude'}
  frame = datacommons.DCFrame(datalog_query=query, labels=labels)

  print('> Columns\t{}'.format(frame.columns()))
  print('> Col types\t{}'.format(frame.types()))
  print('> Pandas frame\n')
  print(frame.pandas())
  print('\n> CSV string\n')
  print(frame.csv())
  print('\n> TSV string\n')
  print(frame.tsv())

if __name__ == '__main__':
  main()
