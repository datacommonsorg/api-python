# Copyright 2020 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Data Commons Pandas API DataFrame Builder Module.

Provides functions for building pandas DataFrames using the Data Commons Graph.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import pandas as pd
import six

import datacommons.stat_vars as dc


def build_time_series(place, stat_var):
    """Constructs a pandas Series with `dates` as the index and corresponding `stat_var` statistics as values.
    
    Args:
      place (`str`): The dcid of Place to query for.
      stat_var (`str`): The dcid of the StatisticalVariable.
    Returns:
      A pandas Series with Place IDs as the index, and Observed statistics as values.
    """
    return pd.Series(dc.get_stat_series(place, stat_var))


def _group_stat_all_by_obs_options(places, stat_vars, mode):
    """Groups the result of `get_stat_all` by Observation options for time series.
    
    Args:
      places (`str` or `iterable` of `str`): The dcids of Places to query for.
      stat_vars (`Iterable` of `str`): The dcids of the StatisticalVariables.
      mode (`str`): "series" to output time series grouped by Observation options, or
        "covariates" to output latest Observations.
    Returns:
      A pandas Series with Place IDs as the index, and Observed statistics as values.

    Raises:
      ValueError: If the payload returned by the Data Commons REST API is
        malformed.
    """
    kseries = "series"
    kcov = "covariates"

    if mode == kseries:
        if len(stat_vars) != 1:
            raise ValueError(
                'When `mode=series`, only one StatisticalVariable for `stat_vars` is allowed.'
            )
        res = collections.defaultdict(list)
    elif mode == kcov:
        res = collections.defaultdict(lambda: collections.defaultdict(list))
    else:
        raise ValueError(
            'Value of `mode` must be one of ("series", "covariates")')

    stat_all = dc.get_stat_all(places, stat_vars)
    for place, place_data in stat_all.items():
        if not place_data:
            continue
        for stat_var, stat_var_data in place_data.items():
            if not stat_var_data:
                continue
            for source_series in stat_var_data['sourceSeries']:
                time_series = source_series['val']
                # Create a hashable for Observation options.
                obs_options = (('measurementMethod',
                                source_series.get('measurementMethod')),
                               ('observationPeriod',
                                source_series.get('observationPeriod')),
                               ('unit', source_series.get('unit')),
                               ('scalingFactor',
                                source_series.get('scalingFactor')))
                if mode == kseries:
                    res[obs_options].append(
                        dict({'place': place}, **time_series))
                elif mode == kcov:
                    date = max(time_series)
                    res[stat_var][obs_options].append({
                        'place': place,
                        'date': date,
                        'val': time_series[date]
                    })
    if mode == kseries:
        return dict(res)
    elif mode == kcov:
        return {k: dict(v) for k, v in res.items()}


def _time_series_pd_input(places, stat_var):
    """Returns a `list` of `dict` per element of `places` based on the `stat_var`.

    Data Commons will pick a set of Observation options that covers the
    maximum number of queried places. Among ties, Data Commons selects an option
    set with the latest Observation.

    Args:
      places (`str` or `iterable` of `str`): The dcids of Places to query for.
      stat_var (`str`): The dcid of the StatisticalVariable.
    Returns:
      A `list` of `dict`, one per element of `places`. Each `dict` consists of
      the time series and place identifier.

    Examples:
      >>> _time_series_pd_input(["geoId/29", "geoId/33"], "Count_Person")
          [
            {'2020-03-07': 20, '2020-03-08': 40, 'place': 'geoId/29'},
            {'2020-08-21': 428, '2020-08-22': 429, 'place': 'geoId/33'}
          ]
    """

    rows_dict = _group_stat_all_by_obs_options(places, [stat_var], 'series')
    most_geos = []
    max_geos_so_far = 0
    latest_date = []
    latest_date_so_far = ''
    for options, rows in rows_dict.items():
        current_geos = len(rows)
        if current_geos > max_geos_so_far:
            max_geos_so_far = current_geos
            most_geos = [options]
            # Reset tiebreaker stats. Recompute after this if-else block.
            latest_date = []
            latest_date_so_far = ''
        elif current_geos == max_geos_so_far:
            most_geos.append(options)
        else:
            # Do not compute tiebreaker stats if not in most_geos.
            continue
        for row in rows:
            dates = set(row.keys())
            dates.remove('place')
            row_max_date = max(dates)
            if row_max_date > latest_date_so_far:
                latest_date_so_far = row_max_date
                latest_date = [options]
            elif row_max_date == latest_date_so_far:
                latest_date.append(options)
    for options in most_geos:
        if options in latest_date:
            return rows_dict[options]


def build_time_series_dataframe(places, stat_var, desc_col=False):
    """Constructs a pandas DataFrame with `places` as the index and dates of the time series as the columns.

    To ensure statistics are comparable across all Places, when multiple
    StatVarObservations are available for Place and StatVar combos, Data
    Commons selects the Observation options that covers the most Places, and breaks
    ties using the Observation options that yield the latest Observation for any
    Place.
    
    Args:
      places (`str` or `iterable` of `str`): The dcids of Places to query for.
      stat_var (`str`): The dcid of the StatisticalVariable.
      desc_col: Whether to order columns in descending order.
    Returns:
      A pandas DataFrame with Place IDs as the index, and sorted dates as columns.
    """
    try:
        if isinstance(places, six.string_types):
            places = [places]
        else:
            places = list(places)
            assert all(isinstance(place, six.string_types) for place in places)
    except:
        raise ValueError(
            'Parameter `places` must be a string object or list-like object of string.'
        )
    if not isinstance(stat_var, six.string_types):
        raise ValueError('Parameter `stat_var` must be a string.')

    df = pd.DataFrame.from_records(_time_series_pd_input(places, stat_var))
    df.set_index('place', inplace=True)
    df.sort_index(inplace=True)
    return df[sorted(df.columns, reverse=desc_col)]


def _covariate_pd_input(places, stat_vars):
    """Returns a `list` of `dict` per element of `places` based on the `stat_var`.

    Data Commons will pick a set of StatVarObservation options that covers the
    maximum number of queried places. Among ties, Data Commons selects an option
    set with the latest Observation.

    Args:
      places (`str` or `iterable` of `str`): The dcids of Places to query for.
      stat_vars (`Iterable` of `str`): The dcids of the StatisticalVariables.
    Returns:
      A `list` of `dict`, one per element of `places`. Each `dict` consists of
      the time series and place identifier.

    Examples:
      >>> _covariate_pd_input(["geoId/29", "geoId/33"], ["Count_Person", "Median_Income_Person"])
          [
            {'Count_Person': 20, 'Median_Income_Person': 40, 'place': 'geoId/29'},
            {'Count_Person': 428, 'Median_Income_Person': 429, 'place': 'geoId/33'}
          ]
    """

    rows_dict = _group_stat_all_by_obs_options(places, stat_vars, 'covariates')
    place2cov = collections.defaultdict(dict)  # {geo: {var1: 3, var2: 33}}

    for stat_var, candidates_dict in rows_dict.items():
        selected_rows = None
        most_geos = []
        max_geos_so_far = 0
        latest_date = []
        latest_date_so_far = ''
        for options, rows in candidates_dict.items():
            current_geos = len(rows)
            if current_geos > max_geos_so_far:
                max_geos_so_far = current_geos
                most_geos = [options]
                # Reset tiebreaker stats. Recompute after this if-else block.
                latest_date = []
                latest_date_so_far = ''
            elif current_geos == max_geos_so_far:
                most_geos.append(options)
            else:
                # Do not compute tiebreaker stats if not in most_geos.
                continue
            for row in rows:
                row_date = row['date']
                if row_date > latest_date_so_far:
                    latest_date_so_far = row_date
                    latest_date = [options]
                elif row_date == latest_date_so_far:
                    latest_date.append(options)
        for options in most_geos:
            if options in latest_date:
                selected_rows = candidates_dict[options]

        for row in selected_rows:
            place2cov[row['place']][stat_var] = row['val']
    return [
        dict({'place': place}, **covariates)
        for place, covariates in place2cov.items()
    ]


def build_covariate_dataframe(places, stat_vars):
    """Constructs a pandas DataFrame with `places` as the index and `stat_vars` as the columns.

    To ensure statistics are comparable across all Places, when multiple
    StatVarObservations are available for Place and StatVar combos, Data
    Commons selects the Observation options that covers the most Places, and breaks
    ties using the Observation options that yield the latest Observation for any
    Place.
    
    Args:
      places (`str` or `iterable` of `str`): The dcids of Places to query for.
      stat_vars (`Iterable` of `str`): The dcids of the StatisticalVariables.
    Returns:
      A pandas DataFrame with Place IDs as the index and `stat_vars` as columns.
    """
    try:
        if isinstance(places, six.string_types):
            places = [places]
        else:
            places = list(places)
            assert all(isinstance(place, six.string_types) for place in places)
        if isinstance(stat_vars, six.string_types):
            stat_vars = [stat_vars]
        else:
            stat_vars = list(stat_vars)
            assert all(
                isinstance(stat_var, six.string_types)
                for stat_var in stat_vars)
    except:
        raise ValueError(
            'Parameter `places` and `stat_vars` must be string object or list-like object.'
        )
    df = pd.DataFrame.from_records(_covariate_pd_input(places, stat_vars))
    df.set_index('place', inplace=True)
    df.sort_index(inplace=True)
    return df
