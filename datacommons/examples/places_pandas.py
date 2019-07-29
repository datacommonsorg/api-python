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
"""Basic demo for get_places_in

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
  pd_frame = pd.DataFrame({'county': ['geoId/06085', 'geoId/24031']})
  print(pd_frame)

  # Get all CensusTracts in these two counties.
  print_header('Get Census Tracts')
  pd_frame['tracts'] = dc.get_places_in(pd_frame['county'], 'CensusTract')
  pd_frame = dc.flatten_frame(pd_frame)
  print(pd_frame)

if __name__ == '__main__':
  main()
