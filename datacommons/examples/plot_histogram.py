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
  cities = dc.get_places_in('City', us_dcid, 'city', max_rows=max_rows)
  cities = dc.expand(cities, 'name', 'city', 'city_name', max_rows=max_rows)


  # Query for categorical data
  cities = dc_plt.get_categorical_data(
      dc_client=dc,
      pd_table=cities,
      seed_col_name='city',
      population_type='Person',
      start_date='2012-01-01',
      end_date='2016-01-01',
      measured_property='income',
      stats_type='median',
      max_rows=max_rows,
      gender='Male')
  cities = dc_plt.get_categorical_data(
      dc_client=dc,
      pd_table=cities,
      seed_col_name='city',
      population_type='Person',
      start_date='2012-01-01',
      end_date='2016-01-01',
      measured_property='income',
      stats_type='median',
      max_rows=max_rows,
      gender='Female')

  # Plot the histogram
  dc_plt.histogram(
      pd_table=cities,
      pd_cols=['Person/income/2016-01-01/Male', 'Person/income/2016-01-01/Female'],
      pd_labels=['Male Income', 'Female Income'],
      title='Median Income in US Cities by Gender',
      xlabel='Median Income',
      ylabel='Number of US Cities',
      bins=50,
      alpha=0.85)

  with pd.option_context('display.width', 400, 'display.max_rows', 100):
    print cities.head(10)
    plt.show()

  return cities

if __name__ == '__main__':
  cities = main()
