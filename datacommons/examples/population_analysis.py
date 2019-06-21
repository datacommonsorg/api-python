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
"""Example analysis with DataCommons Python API.

"""

import datacommons
import pandas as pd


def main():
  dc = datacommons.Client()

  # Build a table with a single US state
  state_table = dc.get_states('United States', 'state', max_rows=1)

  # Add the state name and the 5 counties contained in that state
  state_table = dc.expand(
      state_table, 'name', 'state', 'state name', outgoing=True)
  state_table = dc.expand(
      state_table,
      'containedInPlace',
      'state',
      'county',
      outgoing=False,
      max_rows=3)
  state_table = dc.expand(
      state_table, 'name', 'county', 'county name', outgoing=True)

  state_table = dc.get_populations(
      state_table,
      seed_col_name='county',
      new_col_name='county population',
      population_type='Person',
      max_rows=100)
  with pd.option_context('display.width', 400, 'display.max_rows', 100):
    print state_table

  state_table = dc.get_populations(
      state_table,
      seed_col_name='county',
      new_col_name='county_18_24_years_population',
      population_type='Person',
      max_rows=100,
      age='USC/18To24Years')
  with pd.option_context('display.width', 400, 'display.max_rows', 100):
    print state_table

  state_table = dc.get_populations(
      state_table,
      seed_col_name='county',
      new_col_name='county male population',
      population_type='Person',
      max_rows=100,
      gender='Male')
  with pd.option_context('display.width', 400, 'display.max_rows', 100):
    print state_table

  state_table = dc.get_observations(
      state_table,
      seed_col_name='county population',
      new_col_name='county person count',
      observation_date='2016',
      measured_property='count')

  with pd.option_context('display.width', 400, 'display.max_rows', 100):
    print state_table


if __name__ == '__main__':
  main()
