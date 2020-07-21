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
""" Data Commons Python API examples.

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

  # Get place stats.
  print('Get place stats -- all')
  stats = dc.get_stats(['geoId/05', 'geoId/06', 'dc/madDcid'], 'dc/0hyp6tkn18vcb', obs_dates='all')
  print(stats)

  print('Get place stats -- latest')
  stats = dc.get_stats(['geoId/05', 'geoId/06', 'dc/madDcid'], 'dc/0hyp6tkn18vcb')
  print(stats)

  print('Get place stats -- 2014')
  stats = dc.get_stats(['geoId/05', 'geoId/06', 'dc/madDcid'], 'dc/0hyp6tkn18vcb', obs_dates=['2014'])
  print(stats)

  print('Get place stats -- 2014 badly formatted')
  stats = dc.get_stats(['geoId/05', 'geoId/06', 'dc/madDcid'], 'dc/0hyp6tkn18vcb', obs_dates='2014')
  print(stats)

  print('Get place stats -- 2015-2016')
  stats = dc.get_stats(['geoId/05', 'geoId/06', 'dc/madDcid'], 'dc/0hyp6tkn18vcb', obs_dates=['2015', '2016'])
  print(stats)

  # Get related places.
# TODO(*): Fix the related places example.
#  print('Get related places')
#   related_places = dc.get_related_places(['geoId/06085'], 'Person', 'count',
#       'CensusACS5yrSurvey', "measuredValue", {"gender": "Female"})
#   print(related_places)


if __name__ == '__main__':
  main()
