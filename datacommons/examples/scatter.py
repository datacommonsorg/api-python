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
  pd_states = dc.get_states('United States', 'State')
  pd_states = dc.expand(pd_states, 'name', 'State', 'StateName')

  # Crime enumeration instances we want to query for
  crime_vals = [
      "FBI_PropertyArson",
      "FBI_PropertyBurglary",
      "FBI_PropertyLarcenyTheft",
      "FBI_PropertyMotorVehicleTheft"
  ]
  crime_cols = ['Person/' + val for val in crime_vals]

  # Get populations for all people then different crimes
  pd_states = dc.get_populations(pd_states,
                                 seed_col_name='State',
                                 new_col_name='Person/Total',
                                 population_type='Person')
  pd_states = dc.get_populations(pd_states,
                                 seed_col_name='State',
                                 new_col_name=crime_cols,
                                 population_type='CriminalActivities',
                                 crimeType=crime_vals)

  # Get the median age and crime population observations
  pd_states = dc.get_observations(pd_states,
                                  seed_col_name='Person/Total',
                                  new_col_name='Person/Total/MedianAge',
                                  start_date='2012-01-01',
                                  end_date='2016-01-01',
                                  measured_property='age',
                                  stats_type='median')
  pd_states = dc.get_observations(pd_states,
                                  seed_col_name=crime_cols,
                                  new_col_name=crime_vals,
                                  start_date='2016-01-01',
                                  end_date='2016-01-01',
                                  measured_property='count',
                                  stats_type='count')

  # Generate the scatter plot.
  pd_data, scatter = scatter_plt = dc_plt.scatter(
      pd_table=pd_states,
      pd_xcol='Person/Total/MedianAge',
      pd_ycols=crime_vals,
      pd_labels=['Arson', 'Burglary', 'Larceny Theft', 'Motor Vehicle Theft'],
      pd_reserve_cols=['StateName'],
      title='Crime',
      xlabel='Median Age',
      ylabel='Crime Occurences',
      s=5,
      alpha=0.85)

  with pd.option_context('display.width', 400, 'display.max_rows', 100):
    print "Raw data"
    print pd_states.head(5)
    print "\nFormatted data"
    print pd_data.head(5)
    plt.show()

if __name__ == '__main__':
  main()
