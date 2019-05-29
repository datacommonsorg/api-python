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
"""Basic demo showing how to use the weather extension
"""

from datacommons.utils import DatalogQuery
from datacommons.weather import WeatherExtension
from datacommons.utils import MeasuredValue

import pandas as pd

import datacommons

# Print options
pd.set_option('display.max_colwidth', -1)
pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', 50)
pd.set_option('display.max_rows', 20)

def display_table(num, label, df):
  print('-'*80)
  print('EXAMPLE {}: {}'.format(num, label))
  print('-'*80 + '\n')
  print(df)
  print('\n')

def main():
  # Create a list of places to get weather.
  places = [
      'geoId/4261000', # Pittsburgh, PA
      # 'geoId/0649670', # Mountain View, CA
      # 'geoId/4805000', # Austin, TX
      # 'geoId/0606000', # Berkeley, CA
  ]

  # Example 1: Getting the temperature
  frame_1 = datacommons.DCFrame()
  frame_1 = WeatherExtension(frame_1)
  frame_1.add_column('CityID', 'City', places)
  frame_1.expand('name', 'CityID', 'CityName', new_col_type='Text')
  frame_1.get_temperature('CityID', 'MeanTemp', MeasuredValue.MEAN, date='2019-05-09')
  frame_1.get_temperature('CityID', 'MinTemp', MeasuredValue.MIN, date='2019-05-09')
  display_table(1, 'Temperature', frame_1.pandas())

  # Example 2: Getting the rainfall
  frame_2 = datacommons.DCFrame()
  frame_2 = WeatherExtension(frame_2)
  frame_2.add_column('CityID', 'City', places)
  frame_2.expand('name', 'CityID', 'CityName', new_col_type='Text')
  frame_2.get_rainfall('CityID', 'MeanRain', MeasuredValue.MEAN, date='2019-05-09')
  frame_2.get_rainfall('CityID', 'MinRain', MeasuredValue.MIN, date='2019-05-09')
  display_table(2, 'Rainfall', frame_2.pandas())

  # Example 3: Getting the visibility
  frame_3 = datacommons.DCFrame()
  frame_3 = WeatherExtension(frame_3)
  frame_3.add_column('CityID', 'City', places)
  frame_3.expand('name', 'CityID', 'CityName', new_col_type='Text')
  frame_3.get_visibility('CityID', 'MeanVisibility', MeasuredValue.MEAN, date='2019-05-09')
  frame_3.get_visibility('CityID', 'MaxVisibility', MeasuredValue.MAX, date='2019-05-09')
  display_table(3, 'Humidity', frame_3.pandas())


if __name__ == '__main__':
  main()
