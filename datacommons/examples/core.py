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

Basic demo for get_property_labels, get_property_values, and get_triples.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datacommons as dc
import pandas as pd

import datacommons.utils as utils


def main():
  # Set the dcid to be that of Santa Clara County.
  dcids = ['geoId/06085']

  # Print all incoming and outgoing properties from Santa Clara County.
  utils._print_header('Property Labels for Santa Clara County')
  in_labels = dc.get_property_labels(dcids)
  out_labels = dc.get_property_labels(dcids, out=False)
  print('> Printing properties for {}'.format(dcids))
  print('> Incoming properties: {}'.format(in_labels))
  print('> Outgoing properties: {}'.format(out_labels))

  # Print all property values for "containedInPlace" for Santa Clara County.
  utils._print_header(
    'Property Values for "containedInPlace" of Santa Clara County')
  prop_vals = dc.get_property_values(
    dcids, 'containedInPlace', out=False, value_type='City')
  print('> Cities contained in {}'.format(dcids))
  for dcid in dcids:
    for city_dcid in prop_vals[dcid]:
      print('  - {}'.format(city_dcid))

  # Print the first 10 triples associated with Santa Clara County
  utils._print_header('Triples for Santa Clara County')
  triples = dc.get_triples(dcids)
  for dcid in dcids:
    print('> Triples for {}'.format(dcid))
    for s, p, o in triples[dcid][:5]:
      print('  - ("{}", {}, "{}")'.format(s, p, o))

  # get_property_values can be easily used to populate Pandas DataFrames. First
  # create a DataFrame with some data.
  utils._print_header('Initialize the DataFrame')
  pd_frame = pd.DataFrame({'county': ['geoId/06085', 'geoId/24031']})
  print(pd_frame)

  # Get the names for the given counties.
  utils._print_header('Get County Names')
  pd_frame['county_name'] = dc.get_property_values(
    pd_frame['county'], 'name')
  print(pd_frame)

  # Get the cities contained in these counties.
  utils._print_header('Get Contained Cities')
  pd_frame['city'] = dc.get_property_values(
    pd_frame['county'], 'containedInPlace', out=False, value_type='City')
  print(pd_frame)

  # To expand on a column with get_property_values, the data frame has to be
  # flattened first. Clients can use flatten_frame to do this.
  utils._print_header('Flatten the Frame')
  pd_frame = dc.flatten_frame(pd_frame)
  print(pd_frame)

  # Get the names for each city.
  utils._print_header('Get City Names')
  pd_frame['city_name'] = dc.get_property_values(pd_frame['city'], 'name')
  print(pd_frame)

  # Format the final frame.
  utils._print_header('The Final Frame')
  pd_frame = dc.flatten_frame(pd_frame)
  print(pd_frame)


if __name__ == '__main__':
  main()
