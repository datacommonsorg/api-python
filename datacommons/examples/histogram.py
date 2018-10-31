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

Plots a histogram comparing median income split by gender.
"""

import datacommons
import datacommons.plot_utils as dc_plt
import matplotlib.pyplot as plt
import pandas as pd


def main():
  dc = datacommons.Client()

  # Start with all cities in the United States
  max_rows = 4000
  us_dcid = 'dc/2sffw13'        # DCID for the United States
  cities = dc.get_places_in('City', us_dcid, 'City', max_rows=max_rows)
  cities = dc.expand(cities, 'name', 'City', 'CityName', max_rows=max_rows)

  # Query for populations by gender
  gender_vals = ['Female', 'Male']
  gender_cols = ['Person/' + val for val in gender_vals]
  cities = dc.get_populations(cities,
                              seed_col_name='City',
                              new_col_name=gender_cols,
                              population_type='Person',
                              max_rows=max_rows,
                              gender=gender_vals)

  # Query for the observations
  gender_obs = [val + '/Income' for val in gender_vals]
  cities = dc.get_observations(cities,
                               seed_col_name=gender_cols,
                               new_col_name=gender_obs,
                               start_date='2012-01-01',
                               end_date='2016-01-01',
                               measured_property='income',
                               stats_type='median',
                               max_rows=max_rows)

  # Plot the histogram
  pd_data, hist = dc_plt.histogram(pd_table=cities,
                                   pd_cols=gender_obs,
                                   pd_labels=['Female Income', 'Male Income'],
                                   pd_reserve_cols=['CityName'],
                                   title='Median Income in US Cities by Gender',
                                   xlabel='Median Income',
                                   ylabel='Number of US Cities',
                                   bins=50,
                                   alpha=0.85)

  with pd.option_context('display.width', 400, 'display.max_rows', 100):
    print "Raw data"
    print cities.head(5)
    print "\nFormatted data"
    print pd_data.head(5)
    plt.show()

if __name__ == '__main__':
  main()
