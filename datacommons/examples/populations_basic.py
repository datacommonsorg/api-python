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

import json

def print_header(label):
  print('\n' + '-'*80)
  print(label)
  print('-'*80 + '\n')

def main():
  # Create a list of dcids for California, Kentucky, and Maryland
  ca, ky, md = 'geoId/06', 'geoId/21', 'geoId/24'
  dcids = [ca, ky, md]

  # Get populations for all males and females for the above states
  print_header('Get Populations for All Males and Females')
  males = dc.get_populations(dcids, 'Person', {'gender': 'Male'})
  females = dc.get_populations(dcids, 'Person', {'gender': 'Female'})
  print('> Printing all population dcids for Male Persons\n')
  print(json.dumps(males, indent=2))
  print('\n> Printing all population dcids for Female Persons\n')
  print(json.dumps(females, indent=2))

  # Get the count for all male / females for the above states in 2016
  print_header('Get Population Counts for All Males / Females in Maryland in 2016')
  pop_dcids = males[md] + females[md]
  print('> Requesting observations for {}\n'.format(pop_dcids))
  obs = dc.get_observations(pop_dcids, 'count', '2016', 'measured_value', measurement_method='CenusACS5yrSurvey')
  print(json.dumps(obs, indent=2))

if __name__ == '__main__':
  main()
