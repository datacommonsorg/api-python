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

  city_table = dc.GetCities('California', max_rows=10)
  city_table = dc.AddColumn(city_table, 'city_name', 'city', 'name')
  city_table = dc.AddColumn(city_table, 'county', 'city', 'containedInPlace')
  city_table = dc.AddColumn(city_table, 'county_name', 'county', 'name')
  with pd.option_context('display.width', 400, 'display.max_rows', 100):
    print city_table

  state_table = dc.GetStates('United States', max_rows=100)
  state_table = dc.AddColumn(state_table, 'state_name', 'state', 'name')
  state_table = dc.AddColumn(state_table, 'total_population', 'state', 'count',
                             'Person', '2012-01-01', '2016-01-01')
  state_table = dc.AddColumn(
      state_table,
      'female_population',
      'state',
      'count',
      'Person',
      '2012-01-01',
      '2016-01-01',
      gender='Female')
  with pd.option_context('display.width', 400, 'display.max_rows', 100):
    print state_table


if __name__ == '__main__':
  main()
