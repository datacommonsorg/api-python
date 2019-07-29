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
"""How to use get_populations and get_observations with a Pandas DataFrame.

"""

import datacommons as dc
import pandas as pd

pd.options.display.max_rows = 10
pd.options.display.max_colwidth = 30

def print_header(label):
  print('\n' + '-'*80)
  print(label)
  print('-'*80 + '\n')

def main():
  # Initialize a DataFrame with Santa Clara and Montgomery County.
  print_header('Initialize the DataFrame')
  pd_frame = pd.DataFrame({'state': ['geoId/06', 'geoId/21', 'geoId/24']})
  pd_frame['state_name'] = dc.get_property_values(pd_frame['state'], 'name')
  pd_frame = dc.flatten_frame(pd_frame)
  print(pd_frame)

  # Get populations for male persons
  print_header('Get Male Persons Populations')
  pd_frame['male_pops'] = dc.get_populations(pd_frame['state'], 'Person', {'gender': 'Male'})
  pd_frame = dc.flatten_frame(pd_frame)
  print(pd_frame)

  # Get total number of males for each state in 2016
  print_header('Get Male Persons Observations')
  pd_frame['male_count'] = dc.get_observations(pd_frame['male_pops'], 'count', '2016', 'measured_value', measurement_method='CenusACS5yrSurvey')
  print(pd_frame)

  # Final dataframe. Use the convenience function "clean_frame" to convert
  # columns to numerical types.
  print_header('Final Data Frame')
  pd_frame = dc.flatten_frame(pd_frame)
  pd_frame = dc.clean_frame(pd_frame)
  print(pd_frame)

if __name__ == '__main__':
  main()
