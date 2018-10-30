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
"""Querying for multiple populations in one call to get_populations

"""

import datacommons
import pandas as pd


def main():
  dc = datacommons.Client()

  # Build a table with a single US state
  state_table = dc.get_states('United States', 'state', max_rows=10)

  # Add the state name and the 5 counties contained in that state
  state_table = dc.expand(
      state_table, 'name', 'state', 'state_name', outgoing=True)

  col_names = ['Person/18To24Years', 'Person/25To34Years', 'Person/35To44Years']
  age_vals = ['USC_18To24Years', 'USC_25To34Years', 'USC_35To44Years']
  state_table = dc.get_populations(
      state_table,
      seed_col_name='state',
      new_col_name=col_names,
      population_type='Person',
      max_rows=100,
      age=age_vals)
  with pd.option_context('display.width', 400, 'display.max_rows', 100):
    print state_table


if __name__ == '__main__':
  main()
