# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
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


def main():
  # Create a list of dcids for Santa Clara and Montgomery County.
  sc, mc = 'geoId/06085', 'geoId/24031'
  dcids = [sc, mc]

  # Get all CensusTracts in these two counties.
  print('Get Census Tracts')
  tracts = dc.get_places_in(dcids, 'CensusTract')
  if sc in tracts:
    print('> 10 CensusTracts in Santa Clara County')
    for dcid in tracts[sc][:10]:
      print('  - {}'.format(dcid))
  if mc in tracts:
    print('> 10 CensusTracts in Montgomery County')
    for dcid in tracts[mc][:10]:
      print('  - {}'.format(dcid))

  # Get related places.
  print('Get related places')
  related_places = dc.get_related_places(['geoId/06085'], 'Person',
      {'age': "Years21To64", "gender": "Female"}, 'count', '')
  print(related_places)


if __name__ == '__main__':
  main()
