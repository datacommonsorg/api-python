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
"""Example analysis with DataCommons Plotting helper API.

Plots a scatter plot using FBI crime data at the State level.
"""

import datacommons
import datacommons.plot_utils as dc_plt
import matplotlib.pyplot as plt
import pandas as pd


def main():
  dc = datacommons.Client()

  # Start with a few states in the United States
  pd_states = dc.get_states('United States', 'states')
  pd_states = dc.expand(pd_states, 'name', 'states', 'name')

  # Query for categorical data
  pd_states = dc_plt.get_categorical_data(
      dc_client=dc,
      pd_table=pd_states,
      seed_col_name='states',
      population_type='CriminalActivities',
      start_date='2017-01-01',
      end_date='2017-01-01',
      measured_property='count',
      stats_type='count',
      free_prop_name='crimeType',
      free_enum_type='FBI_CrimeTypeEnum')
  pd_states = dc_plt.get_categorical_data(
      dc_client=dc,
      pd_table=pd_states,
      seed_col_name='states',
      population_type='Person',
      start_date='2012-01-01',
      end_date='2016-01-01',
      measured_property='age',
      stats_type='median')

  columns = [
      'count_FBI_Property_2017-01-01_2017-01-01',
      'count_FBI_PropertyArson_2017-01-01_2017-01-01',
      'count_FBI_PropertyBurglary_2017-01-01_2017-01-01',
      'count_FBI_PropertyLarcenyTheft_2017-01-01_2017-01-01',
      'count_FBI_PropertyMotorVehicleTheft_2017-01-01_2017-01-01'
  ]

  # Generate the scatter plot.
  scatter_plt = dc_plt.scatter(
      pd_table=pd_states,
      x_col='age_Person_2012-01-01_2016-01-01',
      y_cols=columns,
      figsize=(12, 10),
      title='Crime',
      xlabel='Median Age',
      ylabel='Crime Occurences',
      xscale='log')

  with pd.option_context('display.width', 400, 'display.max_rows', 100):
    print pd_states.head(10)
    scatter_plt.show()

if __name__ == '__main__':
  main()
