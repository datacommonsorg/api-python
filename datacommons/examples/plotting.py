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

"""

import datacommons
import datacommons.plot_utils as dc_plt
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
      population_type='CriminalActivities',
      start_date='2017-01-01',
      end_date='2017-01-01',
      measured_property='count',
      stats_type='count',
      free_prop_name='crimeType',
      free_enum_type='FBI_CrimeTypeEnum')

  with pd.option_context('display.width', 400, 'display.max_rows', 100):
    print pd_states


if __name__ == '__main__':
  main()
