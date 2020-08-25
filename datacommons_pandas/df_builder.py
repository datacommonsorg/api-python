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

import datacommons_pandas.stat_vars as dc


def build_time_series(place,
                      stat_var,
                      measurement_method=None,
                      observation_period=None,
                      unit=None,
                      scaling_factor=None):
    """Constructs a pandas Series with `dates` as the index and corresponding `stat_var` statistics as values.
    
    Args:
      place (`str`): The dcid of Place to query for.
      stat_var (`str`): The dcid of the StatisticalVariable.
      measurement_method (`str`): Optional, the dcid of the preferred
        `measurementMethod` value.
      observation_period (`str`): Optional, the preferred
        `observationPeriod` value.
      unit (`str`): Optional, the dcid of the preferred `unit` value.
      scaling_factor (`int`): Optional, the preferred `scalingFactor` value.
    Returns:
      A pandas Series with Place IDs as the index and observed statistics as
      values, representing a time series satisfying all optional args.
    """
    return pd.Series(
        dc.get_stat_series(place, stat_var, measurement_method,
                           observation_period, unit, scaling_factor))


def _group_stat_all_by_obs_options(places, stat_vars, keep_series=True):
    """Groups the result of `get_stat_all` by StatVarObservation options for time series or multivariates.

    Note that this function does not preserve `(place, stat_var)` pairs that
    yield no data `from get_stat_all`. In the extreme case that there is no
    data for any pairs, raise a ValueError instead of returning an empty dict.
    
    Args:
      places (`str` or `iterable` of `str`): The dcids of Places to query for.
      stat_vars (`Iterable` of `str`): The dcids of the StatisticalVariables.
      keep_series (`boolean`): if True, output time series grouped by
        StatVarObservation options; if False, output latest statistics grouped
        by StatVarObservation options.
    Returns:
      A nested dict mapping each StatisticalVariable in `stat_vars` to its
      StatVarObservation options. In turn, each StatVarObservation option
      maps to a list of rows, one per place, with the place id and stat data.

    Raises:
      ValueError: If the payload returned by the Data Commons REST API is
        malformed, or if there is no data for any (Place, StatisticalVariables)
        pair.
    """
    if keep_series:
        if len(stat_vars) != 1:
            raise ValueError(
                'When `keep_series` is set, only one StatisticalVariable for `stat_vars` is allowed.'
            )
        res = collections.defaultdict(list)
    else:
        res = collections.defaultdict(lambda: collections.defaultdict(list))

    stat_all = dc.get_stat_all(places, stat_vars)
    for place, place_data in stat_all.items():
        if not place_data:
            continue
        for stat_var, stat_var_data in place_data.items():
            if not stat_var_data:
                continue
            for source_series in stat_var_data['sourceSeries']:
                series = source_series['val']
                # Convert dict of SVO options into nested tuple (hashable key).
                obs_options = (('measurementMethod',
                                source_series.get('measurementMethod')),
                               ('observationPeriod',
                                source_series.get('observationPeriod')),
                               ('unit', source_series.get('unit')),
                               ('scalingFactor',
                                source_series.get('scalingFactor')))
                if keep_series:
                    res[obs_options].append(dict({'place': place}, **series))
                else:
                    date = max(series)
                    res[stat_var][obs_options].append({
                        'place': place,
                        'date': date,
                        'val': series[date]
                    })
    if not res:
        raise ValueError(
            'No data for any of specified Places and StatisticalVariables.')
    if keep_series:
        return dict(res)
    else:
        return {k: dict(v) for k, v in res.items()}


def _time_series_pd_input(places, stat_var):
    """Returns a `list` of `dict` per element of `places` based on the `stat_var`.

    Data Commons will pick a set of StatVarObservation options that covers the
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

    rows_dict = _group_stat_all_by_obs_options(places, [stat_var],
                                               keep_series=True)
    most_geos = []
    max_geo_count_so_far = 0
    latest_date = []
    latest_date_so_far = ''
    for options, rows in rows_dict.items():
        current_geos = len(rows)
        if current_geos > max_geo_count_so_far:
            max_geo_count_so_far = current_geos
            most_geos = [options]
            # Reset tiebreaker stats. Recompute after this if-else block.
            latest_date = []
            latest_date_so_far = ''
        elif current_geos == max_geo_count_so_far:
            most_geos.append(options)
        else:
            # Do not compute tiebreaker stats if no change to most_geos.
            # Skip to top of the for loop.
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
    StatVarObservations options are available for Place and StatVar combos,
    Data Commons selects the StatVarObservation options that covers the most
    Places, and breaks ties using the StatVarObservation options that yield
    the latest Observation for any Place.
    
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


def _multivariate_pd_input(places, stat_vars):
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
      >>> _multivariate_pd_input(["geoId/29", "geoId/33"],
                              ["Count_Person", "Median_Income_Person"])
          [
            {'Count_Person': 20, 'Median_Income_Person': 40, 'place': 'geoId/29'},
            {'Count_Person': 428, 'Median_Income_Person': 429, 'place': 'geoId/33'}
          ]
    """

    rows_dict = _group_stat_all_by_obs_options(places,
                                               stat_vars,
                                               keep_series=False)
    place2cov = collections.defaultdict(dict)  # {geo: {var1: 3, var2: 33}}

    for stat_var, candidates_dict in rows_dict.items():
        selected_rows = None
        most_geos = []
        max_geo_count_so_far = 0
        latest_date = []
        latest_date_so_far = ''
        for options, rows in candidates_dict.items():
            current_geos = len(rows)
            if current_geos > max_geo_count_so_far:
                max_geo_count_so_far = current_geos
                most_geos = [options]
                # Reset tiebreaker stats. Recompute after this if-else block.
                latest_date = []
                latest_date_so_far = ''
            elif current_geos == max_geo_count_so_far:
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
        dict({'place': place}, **multivariates)
        for place, multivariates in place2cov.items()
    ]


def build_multivariate_dataframe(places, stat_vars):
    """Constructs a pandas DataFrame with `places` as the index and `stat_vars` as the columns.

    To ensure statistics are comparable across all Places, when multiple
    StatVarObservations options are available for Place and StatVar combos,
    Data Commons selects the StatVarObservation options that covers the most
    Places, and breaks ties using the StatVarObservation options that yield
    the latest Observation for any Place.
    
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
    df = pd.DataFrame.from_records(_multivariate_pd_input(places, stat_vars))
    df.set_index('place', inplace=True)
    df.sort_index(inplace=True)
    return df
