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
"""Demo showcasing the DCFrame manipulation workflow

When constructing a DCFrame from a query, the frame can be manipulated using
the 'select' and 'process' argument.
"""

from datacommons.utils import DatalogQuery

import datacommons

def main():
  # We begin with a query that gets States in the datacommons graph.
  query = DatalogQuery()
  query.add_variable('?stateName')
  query.add_constraint('?state', 'typeOf', 'State')
  query.add_constraint('?state', 'name', '?stateName')
  frame = datacommons.DCFrame(datalog_query=query)
  print('> Querying for states')
  print(frame.pandas())

  # If we want to perform the same query but select states that start with the
  # letter 'A', then we can provide a selector function to the frame constructor
  # that returns True iff the row is to be selected.
  select = lambda row: row['stateName'].startswith('A')
  frame_select = datacommons.DCFrame(datalog_query=query, select=select)
  print('\n> States that begin with "A"')
  print(frame_select.pandas())

  # Perhaps we also want to capitalize all State names returned in the query.
  # This can be done by specifying a post processing function that †akes in the
  # resulting table as an argument.
  def process(frame):
    frame.columns = map(lambda row: str(row).upper(), frame.columns)
    return frame
  frame_process = datacommons.DCFrame(datalog_query=query, select=select, process=process)
  print('\n> States that begin with "A" capitalized')
  print(frame_process.pandas())


if __name__ == '__main__':
  main()
