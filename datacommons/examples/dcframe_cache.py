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

This demo shows how to use the DataCommons caching service to store and load
DCFrames that have been created. The are associated with your user
authenticated email.
"""

from datacommons.utils import DatalogQuery

import datacommons


def main():
  # Create the query
  query = DatalogQuery()
  query.add_variable('?id', '?lat', '?lon')
  query.add_constraint('?node', 'typeOf', 'City')
  query.add_constraint('?node', 'name', '"San Luis Obispo"')
  query.add_constraint('?node', 'dcid', '?id')
  query.add_constraint('?node', 'latitude', '?lat')
  query.add_constraint('?node', 'longitude', '?lon')

  # Create the frame
  labels = {'?id': 'city', '?lat': 'latitude', '?lon': 'longitude'}
  frame = datacommons.DCFrame(datalog_query=query, labels=labels)

  # Save the dataframe
  saved_name = frame.save('test_df')
  print('> Saving dataframe to {}'.format(saved_name))
  print(frame.pandas())

  # Read a new frame from the saved version
  saved_frame = datacommons.DCFrame(file_name=saved_name)
  print('\n> Reading from {}'.format(saved_name))
  print(frame.pandas())


if __name__ == '__main__':
  main()
