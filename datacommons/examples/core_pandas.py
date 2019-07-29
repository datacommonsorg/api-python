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
"""Demo showing how to use get_property_value with a Pandas DataFrame

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
  # get_property_values can be easily used to populate Pandas DataFrames. First
  # create a DataFrame with some data.
  print_header('Initialize the DataFrame')
  pd_frame = pd.DataFrame({'county': ['geoId/06085', 'geoId/24031']})
  print(pd_frame)

  # Get the names for the given counties.
  print_header('Get County Names')
  pd_frame['county_name'] = dc.get_property_values(pd_frame['county'], 'name')
  print(pd_frame)

  # Get the cities contained in these counties.
  print_header('Get Contained Cities')
  pd_frame['city'] = dc.get_property_values(pd_frame['county'], 'containedInPlace', outgoing=False, value_type='City', reload=True)
  print(pd_frame)

  # To expand on a column with get_property_values, the data frame has to be
  # flattened first. Clients can use flatten_frame to do this.
  print_header('Flatten the Frame')
  pd_frame = dc.flatten_frame(pd_frame)
  print(pd_frame)

  # Get the names for each city.
  print_header('Get City Names')
  pd_frame['city_name'] = dc.get_property_values(pd_frame['city'], 'name')
  print(pd_frame)

  # Format the final frame.
  print_header('The Final Frame')
  pd_frame = dc.flatten_frame(pd_frame)
  print(pd_frame)

if __name__ == '__main__':
  main()
