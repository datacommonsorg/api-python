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

Note to use this API code in an Colab or another iPython notebook environment
add the following code:
pip install --upgrade numpy
pip install --upgrade pandas
pip install --upgrade git+https://github.com/ACscooter/datacommons.git@feature/api-version-2
"""

import datacommons
from datacommons.utils import DatalogQuery
from datacommons.populations import PopulationsExtension

import pandas as pd

# Print options
pd.set_option('display.max_colwidth', -1)
pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', 50)
pd.set_option('display.max_rows', 20)

# Helper function for formatting table printing
def print_pandas(example_num, df):
    print('-'*80)
    print('EXAMPLE {}'.format(example_num))
    print('-'*80 + '\n')
    print(df)
    print('\n')

def main():

  frame_1 = datacommons.DCFrame() # establish generic df
  frame_1 = PopulationsExtension(frame_1) # add population features to df

  # Start by initializing a column of three US states: California, Kentucky, and
  # Maryland.
  frame_1.add_column('state_dcid', 'State', ['geoId/06', 'geoId/21', 'geoId/24'])
  print(frame_1.pandas())

  # Name is an outgoing property of the State. We can call expand to populate a
  # column 'state_name' with names of states corresponding to dcids in the
  # 'state_dcid' column.
  frame_1.expand('name', 'state_dcid', 'state_name')
  print(frame_1.pandas())

  # Get populations for state
  frame_1.get_populations(
      seed_col_name='state_dcid',
      new_col_name='state_population',
      population_type='Person',
      rows=100)
  with pd.option_context('display.width', 400, 'display.max_rows', 100):
    print frame_1 

  frame_1.get_populations(
      seed_col_name='state_dcid',
      new_col_name='state_18_24_years_population',
      population_type='Person',
      rows=100,
      age='USC/18To24Years')
  with pd.option_context('display.width', 400, 'display.max_rows', 100):
    print frame_1

  frame_1.get_populations(
      seed_col_name='state_dcid',
      new_col_name='state_male_population',
      population_type='Person',
      rows=100,
      gender='Male')
  with pd.option_context('display.width', 400, 'display.max_rows', 100):
    print frame_1

  # Get observations on state populations
  frame_1.get_observations(
      seed_col_name='state_population',
      new_col_name='state_person_2016_count',
      observation_date='2016',
      measured_property='count')

  # Add 3 counties contained in each state
  frame_1.expand(
      'containedInPlace',
      'state_dcid',
      'county_dcid',
      new_col_type='County',
      outgoing=False,
      rows=3)
  print(frame_1.pandas())

  # Get populations for counties
  frame_1.get_populations(
      seed_col_name='county_dcid',
      new_col_name='county_population',
      population_type='Person',
      rows=100)
  with pd.option_context('display.width', 400, 'display.max_rows', 100):
    print frame_1 

  frame_1.get_populations(
      seed_col_name='county_dcid',
      new_col_name='county_18_24_years_population',
      population_type='Person',
      rows=100,
      age='USC/18To24Years')
  with pd.option_context('display.width', 400, 'display.max_rows', 100):
    print frame_1

  frame_1.get_populations(
      seed_col_name='county_dcid',
      new_col_name='county_male_population',
      population_type='Person',
      rows=100,
      gender='Male')
  with pd.option_context('display.width', 400, 'display.max_rows', 100):
    print frame_1

  # Get observations on county populations
  frame_1.get_observations(
      seed_col_name='county_population',
      new_col_name='county_person_2016_count',
      observation_date='2016',
      measured_property='count')
if __name__ == '__main__':
  main()
