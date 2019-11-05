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

Example on how to use the Client API SPARQL query wrapper.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datacommons as dc


def main():
  # Create a SPARQL query querying for the name of some states
  query = ('''
SELECT  ?name ?dcid
WHERE {
  ?a typeOf Place .
  ?a name ?name .
  ?a dcid ("geoId/06" "geoId/21" "geoId/24") .
  ?a dcid ?dcid
}
''')
  print('> Issuing query.\n{}'.format(query))

  # Iterate through all the rows in the results.
  print('> Printing results.\n')
  for row in dc.query(query_string=query):
    print('  {}'.format(row))


if __name__ == '__main__':
  main()
