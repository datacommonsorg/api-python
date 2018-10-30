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
  pd_states = dc.get_states('United States', 'State', max_rows=3)
  pd_states = dc.expand(pd_states, 'name', 'State', 'Name')

  # Query for the populations
  age_vals = ['USC_18To24Years', 'USC_25To34Years']
  age_cols = ['18To24Years', '25To34Years']
  pd_states = dc.get_populations(pd_states,
                                 seed_col_name='State',
                                 new_col_name=age_cols,
                                 population_type='Person',
                                 age=age_vals)
  with pd.option_context('display.width', 400, 'display.max_rows', 100):
    print pd_states

  # Query for the time series observations
  time_pd = dc.get_date_ranged_observations(pd_states,
      seed_col_name=age_cols,
      label_col_name='Name',
      start_range=('2006-01-01', '2010-01-01'),
      end_range=('2012-01-01', '2016-01-01'),
      range_freq='1YS',
      measured_property='count',
      stats_type='count')
  with pd.option_context('display.width', 400, 'display.max_rows', 100):
    print ""
    print time_pd

  # Plot the time-series
  data_cols = [
      "18To24Years/count/Missouri",
      "18To24Years/count/Arkansas",
      "18To24Years/count/Arizona",
      "25To34Years/count/Missouri",
      "25To34Years/count/Arkansas",
      "25To34Years/count/Arizona"
  ]
  plot = dc_plt.plot(pd_table=time_pd,
                     pd_time_col='endTime',
                     pd_data_cols=data_cols)
  plt.show()

if __name__ == '__main__':
  main()
