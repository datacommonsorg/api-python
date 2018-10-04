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
"""Examples to use get_intance() API."""

_MAX_ROW = 20


import pandas as pd
import datacommons

def main():
  dc = datacommons.Client()

  # Get a list of "Class" type instance.
  pd_class = dc.get_instances('class', 'Class', max_rows=_MAX_ROW)
  with pd.option_context('display.width', 400, 'display.max_rows', 20):
    print pd_class

  # Get a list of states with their names
  pd_state = dc.get_instances('state', 'State', max_rows=_MAX_ROW)
  pd_state = dc.expand(pd_state, 'name', 'state', 'state_name', outgoing=True)
  with pd.option_context('display.width', 400, 'display.max_rows', 20):
    print pd_state


  # Get a list of cities with their names and timezone.
  pd_city = dc.get_instances('city', 'City', max_rows=_MAX_ROW)
  pd_city = dc.expand(pd_city, 'name', 'city', 'name', outgoing=True)
  pd_city = dc.expand(pd_city, 'timezone', 'city', 'timezone', outgoing=True)
  with pd.option_context('display.width', 400, 'display.max_rows', 20):
    print pd_city

if __name__ == '__main__':
  main()
