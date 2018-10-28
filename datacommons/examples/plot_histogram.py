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

Plots a histogram using income data at the State level.
"""

import datacommons
import datacommons.plot_utils as dc_plt
import matplotlib.pyplot as plt
import pandas as pd


def main():
  dc = datacommons.Client()

  # Start with a few states in the United States
  pd_states = dc.get_states('United States', 'states', max_rows=5)
  pd_states = dc.expand(pd_states, 'name', 'states', 'name')

  # Query for categorical data
  pd_states = dc_plt.get_categorical_data(
      dc_client=dc,
      pd_table=pd_states,
      seed_col_name='states',
      population_type='Person',
      start_date='2012-01-01',
      end_date='2016-01-01',
      measured_property='count',
      stats_type='count',
      free_prop_name='income',
      free_enum_type='USC_IncomeEnum')

  # Truncate the data
  columns = [
      'USC_LessThan10000_2016-01-01',
      'USC_10000To14999_2016-01-01',
      'USC_15000To19999_2016-01-01',
      'USC_20000To24999_2016-01-01',
      'USC_25000To29999_2016-01-01',
      'USC_30000To34999_2016-01-01',
      'USC_35000To39999_2016-01-01',
      'USC_40000To44999_2016-01-01',
      'USC_45000To49999_2016-01-01',
      'USC_50000To59999_2016-01-01',
      'USC_60000To74999_2016-01-01',
      'USC_75000To99999_2016-01-01',
      'USC_100000To124999_2016-01-01',
      'USC_125000To149999_2016-01-01',
      'USC_150000To199999_2016-01-01',
      'USC_200000OrMore_2016-01-01'
  ]

  # Generate the histogram
  hist = dc_plt.histogram(
      pd_table=pd_states,
      series_col='name',
      data_cols=columns,
      figsize=(12, 10),
      title='Income Bracket by State',
      ylabel='Population Size')

  with pd.option_context('display.width', 400, 'display.max_rows', 100):
    print pd_states.head(10)
    hist.show()

if __name__ == '__main__':
  main()
