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

import pandas as pd
import datacommons


def main():
  dc = datacommons.Client()

  # Bootstrap with IDs of a few US cities.
  pd_table = pd.DataFrame({'city' : ['City', 'dc/ve1tlm',
                                     'dc/0vypck3', 'dc/prehdd2']})

  # Add names of those cities.
  weather_table = dc.expand(pd_table, 'name', 'city', 'city_name')
  with pd.option_context('display.width', 400, 'display.max_rows', 100):
    print(weather_table)

  # Add monthly mean temperature for those cities for all 2017 months.
  for d in range(1, 13):
    weather_table = dc.get_observations(weather_table,
                                        seed_col_name='city',
                                        new_col_name=('temp_2017%.2d' % d),
                                        start_date=('2017-%.2d-01' % d),
                                        end_date=('2017-%.2d-01' % d),
                                        measured_property='temperature',
                                        stats_type='mean')

  with pd.option_context('display.width', 400, 'display.max_rows', 100):
    print(weather_table)


if __name__ == '__main__':
  main()
