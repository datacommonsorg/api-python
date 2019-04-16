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
"""Example client for DataCommons Python API.

"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datacommons
import pandas as pd


def main():
  dc = datacommons.Client()

  # Get lat/long of a city.
  query = ('''
           SELECT ?id ?lat ?long,
             typeOf ?o City,
             name ?o "San Luis Obispo",
             dcid ?o ?id,
             latitude ?o ?lat,
             longitude ?o ?long
           ''')
  print('Issuing query "{}"'.format(query))
  try:
    df = dc.query(query)
  except RuntimeError as e:
    print(e)
    return

  with pd.option_context('display.width', 400, 'display.max_rows', 100):
    print(df)

  saved_file_name = dc.save_dataframe(df, 'test_df')
  print(saved_file_name)
  saved_df = dc.read_dataframe(saved_file_name)
  assert df.equals(saved_df)


if __name__ == '__main__':
  main()
