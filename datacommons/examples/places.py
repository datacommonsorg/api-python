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

Basic demo for get_places_in
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datacommons as dc
import pandas as pd
import pprint

import datacommons.utils as utils


def main():
  # Create a list of dcids for Santa Clara and Montgomery County.
  sc, mc = 'geoId/06085', 'geoId/24031'
  dcids = [sc, mc]

  # Get all CensusTracts in these two counties.
  utils._print_header('Get Census Tracts')
  tracts = dc.get_places_in(dcids, 'CensusTract')
  if sc in tracts:
    print('> 10 CensusTracts in Santa Clara County')
    for dcid in tracts[sc][:10]:
      print('  - {}'.format(dcid))
  print()
  if mc in tracts:
    print('> 10 CensusTracts in Montgomery County')
    for dcid in tracts[mc][:10]:
      print('  - {}'.format(dcid))

  # We perform the same task using a Pandas DataFrame. First, initialize a
  # DataFrame with Santa Clara and Montgomery County.
  utils._print_header('Initialize the DataFrame')
  pd_frame = pd.DataFrame({'county': ['geoId/06085', 'geoId/24031']})
  print(pd_frame)

  # Get all CensusTracts in these two counties.
  utils._print_header('Get Census Tracts')
  pd_frame['tracts'] = dc.get_places_in(pd_frame['county'], 'CensusTract')
  pd_frame = dc.flatten_frame(pd_frame)
  print(pd_frame)

  # Get all population and observation data of Mountain View.
  utils._print_header('Get Mountain View population and observation')
  popobs = dc.get_pop_obs("geoId/0649670")
  pprint.pprint(popobs)


if __name__ == '__main__':
  main()
