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
"""

import datacommons

import pandas as pd

pd.options.display.max_rows = 10

def print_header(label):
  print('\n' + '-'*80)
  print(label)
  print('-'*80 + '\n')

def main():
  # Initialize the frame
  frame = datacommons.DCFrame()

  # Start by initializing a column of three US states: California, Kentucky, and
  # Maryland.
  frame.add_column('state_dcid', 'State', ['geoId/06', 'geoId/21', 'geoId/24'])

  # Name is an outgoing property of the State. We can call expand to populate a
  # column 'state_name' with names of states corresponding to dcids in the
  # 'state_dcid' column.
  print_header('GET STATE NAMES')
  frame.expand('name', 'state_dcid', 'state_name', reload=True)
  print(frame.pandas())

  # We can also use expand to traverse incoming properties. To get all Counties
  # contained in States, we construct a column of county dcids using the
  # containedInPlace property pointing into State. This requires a type hint for
  # as multiple types can be containedInPlace of a State.
  print_header('GET CONTAINING COUNTIES')
  frame.expand('containedInPlace', 'state_dcid', 'county_dcid', new_col_type='County', outgoing=False, reload=True)
  print(frame.pandas())

  # Finally, we populate a column of County names.
  print_header('GET COUNTY NAMES')
  frame.expand('name', 'county_dcid', 'county_name', reload=True)
  print(frame.pandas())

  # Print out basic information about the table.
  print_header('TABLE INFORMATION')
  print('> Columns: {}'.format(frame.columns()))
  print('> Column type for "county_dcid": {}'.format(frame.types('county_dcid')))
  print('> CSV string\n')
  print(frame.csv()[:500] + '...')
  print('\n> TSV string\n')
  print(frame.tsv()[:500] + '...')

if __name__ == '__main__':
  main()
