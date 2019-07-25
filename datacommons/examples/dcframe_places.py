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

  # Start by initializing a column of with Santa Clara County.
  frame.add_column('county_dcid', 'County', ['geoId/06085'])

  # Get the names for counties in the frame.
  print_header('GET COUNTY NAMES')
  frame.expand('name', 'county_dcid', 'county_name', reload=True)
  print(frame.pandas())

  # We can call "get_places_in" to get places contained in nodes that represent
  # geographical locations. Get all cities in Santa Clara County
  print_header('GET CONTAINING CITIES')
  frame.get_places_in('county_dcid', 'city_dcid', 'City', reload=True)
  print(frame.pandas())

  # Finally, we populate a column of City names.
  print_header('GET CITY NAMES')
  frame.expand('name', 'city_dcid', 'city_name', reload=True)
  print(frame.pandas())

if __name__ == '__main__':
  main()
