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
"""Basic demo for the DCNode.

This demo showcases basic features of the DCNode class.
- Initialize a DCNode
- Get property values for a DCNode.
"""

import datacommons

def main():
  # Create a node for Santa Clara county
  node = datacommons.DCNode(dcid="geoId/06085")

  # Notice that its name and types are immediately populated
  print("> Node: {}".format(node))

  # Get all cities contained in Santa Clara county
  cities = node.get_property_values("containedInPlace", outgoing=False, value_type="City")
  print("> Cities contained in Santa Clara County")
  for city in cities:
    print("  {}".format(city))

  print(node._in_props)

if __name__ == '__main__':
  main()
