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
"""Basic demo for get_property_labels, get_property_values, and get_triples.

"""

import datacommons as dc

def print_header(label):
  print('\n' + '-'*80)
  print(label)
  print('-'*80 + '\n')

def main():
  # Set the dcid to be that of Santa Clara County.
  dcid = 'geoId/06085'

  # Print all incoming and outgoing properties from Santa Clara County.
  print_header('Property Labels for Santa Clara County')
  in_labels = dc.get_property_labels(dcid)
  out_labels = dc.get_property_labels(dcid, outgoing=False)
  print('> Printing properties for {}'.format(dcid))
  print('> Incoming properties: {}'.format(in_labels))
  print('> Outgoing properties: {}'.format(out_labels))

  # Print all property values for "containedInPlace" for Santa Clara County.
  print_header('Property Values for "containedInPlace" of Santa Clara County')
  prop_vals = dc.get_property_values(dcid, 'containedInPlace', outgoing=False, value_type='City', reload=True)
  print('> Cities contained in {}'.format(dcid))
  if dcid in prop_vals:
    for city_dcid in prop_vals[dcid]:
      print('  - {}'.format(city_dcid))

  # Print the first 10 triples associated with Santa Clara County
  print_header('Triples for "containedInPlace" of Santa Clara County')
  triples = dc.get_triples(dcid, reload=True)
  print('> Triples for {}'.format(dcid))
  for s, p, o in triples[:10]:
    print('  - ("{}", {}, "{}")'.format(s, p, o))

if __name__ == '__main__':
  main()
