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

"""DataCommons Weather API Extension.

Potential improvements:
- Include option to also query for the unit measured.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from types import MethodType
from .datacommons import DCFrame
from . import utils

_PLACES_WITH_WEATHER = ['City']

def WeatherExtension(frame):
  """ The DataCommons weather API extension. """
  frame.get_temperature = MethodType(get_temperature, frame)
  frame.get_visibility = MethodType(get_visibility, frame)
  frame.get_rainfall = MethodType(get_rainfall, frame)
  frame.get_snowfall = MethodType(get_snowfall, frame)
  frame.get_barometric_pressure = MethodType(get_barometric_pressure, frame)
  frame.get_humidity = MethodType(get_humidity, frame)
  return frame

def get_temperature(self, seed_col_name, new_col_name, measured_value, rows=100, **kwargs):
  """ Returns column(s) containing temperature data in celsius.

  Args:
    seed_col_name: The name of the seed column. The seed column can either
      contain date or location dcids.
    new_col_name: The new column's name.
    measured_value: The statistic type measured i.e. min / max / mean temp.
    rows: The maximum number of rows to return.
    kwargs: Additional keyword arguments include "date" specified as
      "YYYY-MM-DD".
  """
  _get_weather(self,
               seed_col_name,
               new_col_name,
               measured_value,
               'temperature',
               rows=rows,
               **kwargs)


def get_visibility(self, seed_col_name, new_col_name, measured_value, rows=100, **kwargs):
  """ Returns column(s) containing visibility data in kilometer.

  Args:
    seed_col_name: The name of the seed column. The seed column can either
      contain date or location dcids.
    new_col_name: The new column's name.
    measured_value: The statistic type measured i.e. min / max / mean temp.
    rows: The maximum number of rows to return.
    kwargs: Additional keyword arguments include "date" specified as
      "YYYY-MM-DD".
  """
  _get_weather(self,
               seed_col_name,
               new_col_name,
               measured_value,
               'visibility',
               rows=rows,
               **kwargs)

def get_rainfall(self, seed_col_name, new_col_name, measured_value, rows=100, **kwargs):
  """ Returns column(s) containing rainfall data in millimeter.

  Args:
    seed_col_name: The name of the seed column. The seed column can either
      contain date or location dcids.
    new_col_name: The new column's name.
    measured_value: The statistic type measured i.e. min / max / mean temp.
    rows: The maximum number of rows to return.
    kwargs: Additional keyword arguments include "date" specified as
      "YYYY-MM-DD".
  """
  _get_weather(self,
               seed_col_name,
               new_col_name,
               measured_value,
               'rainfall',
               rows=rows,
               **kwargs)

def get_snowfall(self, seed_col_name, new_col_name, measured_value, rows=100, **kwargs):
  """ Returns column(s) containing snowfall data in millimeter.

  Args:
    seed_col_name: The name of the seed column. The seed column can either
      contain date or location dcids.
    new_col_name: The new column's name.
    measured_value: The statistic type measured i.e. min / max / mean temp.
    rows: The maximum number of rows to return.
    kwargs: Additional keyword arguments include "date" specified as
      "YYYY-MM-DD".
  """
  _get_weather(self,
               seed_col_name,
               new_col_name,
               measured_value,
               'snowfall',
               rows=rows,
               **kwargs)

def get_barometric_pressure(self, seed_col_name, new_col_name, measured_value, rows=100, **kwargs):
  """ Returns column(s) containing barometric pressure data in millibar.

  Args:
    seed_col_name: The name of the seed column. The seed column can either
      contain date or location dcids.
    new_col_name: The new column's name.
    measured_value: The statistic type measured i.e. min / max / mean temp.
    rows: The maximum number of rows to return.
    kwargs: Additional keyword arguments include "date" specified as
      "YYYY-MM-DD".
  """
  _get_weather(self,
               seed_col_name,
               new_col_name,
               measured_value,
               'barometricPressure',
               rows=rows,
               **kwargs)

def get_humidity(self, seed_col_name, new_col_name, measured_value, rows=100, **kwargs):
  """ Returns column(s) containing barometric pressure data in percent.

  Args:
    seed_col_name: The name of the seed column. The seed column can either
      contain date or location dcids.
    new_col_name: The new column's name.
    measured_value: The statistic type measured i.e. min / max / mean temp.
    rows: The maximum number of rows to return.
    kwargs: Additional keyword arguments include "date" specified as
      "YYYY-MM-DD".
  """
  _get_weather(self,
               seed_col_name,
               new_col_name,
               measured_value,
               'humidity',
               rows=rows,
               **kwargs)

def _get_weather(self,
                 seed_col_name,
                 new_col_name,
                 measured_value,
                 weather_type,
                 rows=100,
                 **kwargs):
    """ Returns a column containing statistics measuring the given weather type.

    Adds a new column to the dataframe containing weather statistics for the
    given weather type for the "date" specified in kwargs.

    POTENTIAL USAGE:
    # A list of places and date must be provided as parameters with one as the
    # seed column and the other in the keyword arguments.
    # - If places are provided in the seed column, then "date" must be specified
    #   in kwargs.
    # - If date are provided in the seed column, then "places" must be specified
    #   in kwargs.
    #
    # A new column is created for each parameter provided in kwargs. If places
    # are provided as the seed column, and date in the kwargs, then a new column
    # is created for each date where each row contains the temperature for the
    # given date and the row's place.

    Args:
      seed_col_name: The name of the seed column. The seed column can either
        contain date or location dcids.
      new_col_name: The new column's name.
      measured_value: The statistic type measured i.e. min / max / mean temp.
      weather_type: Can be one of the following:
        - "temperature"
        - "visibility"
        - "rainfall"
        - "snowfall"
        - "barometricPressure"
        - "humidity"
      rows: The maximum number of rows to return.
      kwargs: Additional keyword arguments include "date".
        - The date must be specified as "YYYY-MM-DD"
    """
    if seed_col_name not in self._dataframe:
      raise ValueError('{} is not a column in the frame.'.format(seed_col_name))
    if new_col_name in self._dataframe:
      raise ValueError('{} is already a column.'.format(new_col_name))
    if self._col_types[seed_col_name] not in _PLACES_WITH_WEATHER:
      valid_places = ', '.join([place for place in _PLACES_WITH_WEATHER])
      raise ValueError('{} needs to be type of Place e.g. one of {}'.format(seed_col_name, valid_places))
    if 'date' not in kwargs:
      raise ValueError('"date" must be specified as a keyword argument.')

    # Get the query variable label map
    seed_col_var = '?' + seed_col_name.replace(' ', '_')
    new_col_var = '?' + new_col_name.replace(' ', '_')
    labels = {seed_col_var: seed_col_name, new_col_var: new_col_name}

    # Get the query variable types
    seed_col_type = self._col_types[seed_col_name]
    new_col_type = 'Text'
    type_hint = {seed_col_var: seed_col_type, new_col_var: new_col_type}

    # Get the query parameters.
    place_dcids, date_strings = None, None
    if 'date' in kwargs:
      place_dcids = list(self._dataframe[seed_col_name])
      date_strings = ['"{}"'.format(date) for date in [kwargs['date']]]

    # NOTE: If we ever want to allow the seed column to contain dates, uncomment
    #       this block of code. Additionally, remove the list call surrounding
    #       kwargs['date'] above.
    # elif 'places' in kwargs:
    #   place_dcids = kwargs['places']
    #   date_strings = list(self._dataframe[seed_col_name])

    # Construct the query
    query = utils.DatalogQuery()
    query.add_variable(seed_col_var, new_col_var)

    # Add the constraints
    query.add_constraint('?o', 'typeOf', 'WeatherObservation')
    query.add_constraint('?o', 'measuredProperty', '{}'.format(weather_type))
    query.add_constraint('?o', measured_value, new_col_var)
    query.add_constraint(seed_col_var, 'dcid', place_dcids)
    query.add_constraint('?o', 'observationDate', ' '.join(date_strings))
    if 'date' in kwargs:
      query.add_constraint('?o', 'observedNode', seed_col_var)

    # NOTE: If we ever want to allow the seed column to contain dates, uncomment
    #       this block of code.
    # elif 'places' in kwargs:
    #   query.add_constraint('?o', 'observationDate', seed_col_var)

    new_frame = DCFrame(datalog_query=query, labels=labels, type_hint=type_hint, rows=rows)
    self.merge(new_frame)
