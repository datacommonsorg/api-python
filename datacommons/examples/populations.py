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
""" Data Commons Python Client API examples.

Basic demo for get_populations and get_observations.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datacommons as dc
import pprint
import json


def main():
  # Create a list of dcids for California, Kentucky, and Maryland
  ca, ky, md = 'geoId/06', 'geoId/21', 'geoId/24'
  dcids = [ca, ky, md]

  # Get the population of all employed individuals in the above states.
  print('Get Populations for All Employed Individuals')
  employed = dc.get_populations(dcids, 'Person', constraining_properties={
                  'employment': 'BLS_Employed'})
  print(json.dumps(employed, indent=2))

  # Get the count for all male / females for the above states in 2016
  print('Get Population Counts for Employed Individuals in Maryland')
  pop_dcids = [employed[md]]
  obs = dc.get_observations(pop_dcids,
                            'count',
                            'measuredValue',
                            '2018-12',
                            observation_period='P1M',
                            measurement_method='BLSSeasonallyAdjusted')
  print(json.dumps(obs, indent=2))

  # Get all population and observation data of Mountain View.
  print('Get Mountain View population and observation')
  popobs = dc.get_pop_obs("geoId/0649670")
  pprint.pprint(popobs)

if __name__ == '__main__':
  main()
