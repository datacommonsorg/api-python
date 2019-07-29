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
"""Basic demo for the Node.

This demo showcases basic features of the Node class.
- Initialize a Node
- Get property values for a Node.
"""

import datacommons

def main():
  # Create a node for Santa Clara county
  node = datacommons.Node(dcid="geoId/06085")

  # Notice that its name and types are immediately populated
  print("> Node: {}".format(node))

  # Print the in and out properties defined for this node.
  out_props = node.get_properties()
  in_props = node.get_properties(outgoing=False)
  print("> Incoming properties: {}".format(in_props))
  print("> Outgoing properties: {}".format(out_props))

  # Get all cities contained in Santa Clara county
  cities = node.get_property_values("containedInPlace", outgoing=False, value_type="City", reload=True)
  print("\n> Cities contained in Santa Clara County.")
  for city in cities:
    print("  {}".format(city))

  # Print all triples associated with Santa Clara county
  triples = node.get_triples()
  print("\n> Printing 5 triples for Santa Clara County.")
  for s, p, o in triples[:5]:
    print("  ({}, {}, {})".format(s, p, o))

  # Print all triples associated with Santa Clara county converting subject
  # and object to Nodes.
  triples = node.get_triples(as_node=True)
  print("\n> Printing 5 triples for Santa Clara County with as_node = True.")
  for s, p, o in triples[:5]:
    print("  ({}, {}, {})".format(s, p, o))

if __name__ == '__main__':
  main()