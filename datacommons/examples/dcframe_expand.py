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
"""Demo showcasing DataCommons caching features.

This demo shows how to use the DataCommons expand feature to incrementally
build you DCFrame. The expand feature is very useful for adding data from
other properties in the DataCommons graph.
"""

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
"""Example query demonstrating expand API.

Adds properties in the incoming and outgoing direction by building a table of
counties contained in the United States.
"""


from datacommons.utils import DatalogQuery

import datacommons


def main():
  frame = datacommons.DCFrame()

  # Start by initializing a column of three US states: California, Kentucky, and
  # Maryland.
  frame.add_column('state_dcid', 'State', ['geoId/06', 'geoId/21', 'geoId/24'])

  # Name is an outgoing property of the State. We can call expand to populate a
  # column 'state_name' with names of states corresponding to dcids in the
  # 'state_dcid' column.
  frame.expand('name', 'state_dcid', 'state_name')
  print(frame.pandas())

  # We can also use expand to traverse incoming properties. To get all Counties
  # contained in States, we construct a column of county dcids using the
  # containedInPlace property pointing into State. This requires a type hint for
  # as multiple types can be containedInPlace of a State.
  frame.expand('containedInPlace', 'state_dcid', 'county_dcid', new_col_type='County', outgoing=False)
  print(frame.pandas())

  # Finally, we populate a column of County names.
  frame.expand('name', 'county_dcid', 'county_name')
  print(frame.pandas())


if __name__ == '__main__':
  main()
