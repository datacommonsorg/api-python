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
"""Example query demonstrating expand API.

Adds properties in the incoming and outgoing direction by building a table of
all counties contained in the United States.
"""

import datacommons
import pandas as pd


def main():
  dc = datacommons.Client()

  # Start with all states in the United States and add the state names. This
  # is an outgoing property of State.
  pd_state = dc.get_contained_places(
      place_type='Country',
      place_name='United States',
      contained_place_type='State',
      col_name='state')
  pd_state = dc.expand(pd_state, 'name', 'state', 'state_name', outgoing=True)

  # Add information for counties contained in states in the 'state' column.
  # Getting the county is an incoming property of State. Note that there are
  # roughly 3100 counties in the United States
  pd_state = dc.expand(
      pd_state,
      'containedInPlace',
      'state',
      'county',
      outgoing=False,
      max_rows=50)
  pd_state = dc.expand(
      pd_state, 'name', 'county', 'county_name', outgoing=True, max_rows=50)

  # Print out the final data frame
  with pd.option_context('display.width', 400, 'display.max_rows', 100):
    print pd_state


  pd_city = dc.get_contained_places(
      place_type='State',
      place_name='California',
      contained_place_type='City',
      col_name='city')
  pd_city = dc.expand(pd_city, 'name', 'city', 'city_name', outgoing=True)
  with pd.option_context('display.width', 400, 'display.max_rows', 100):
    print pd_city

if __name__ == '__main__':
  main()
