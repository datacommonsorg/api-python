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
"""Basic examples for building pandas objects using the Data Commons Pandas API."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datacommons_pandas as dcpd


def build_time_series_example():

    print("""
# Build a pd.Series of time series for one variable and one place.
$ dcpd.build_time_series('country/CAN', 'Count_WildlandFireEvent')
{}""".format(dcpd.build_time_series('country/CAN', 'Count_WildlandFireEvent')))

    print("""
# Build a pd.Series of time series for one variable and one place and optional args.
$ dcpd.build_time_series('country/USA', 'Count_Person', 'CensusPEPSurvey')
{}""".format(
        dcpd.build_time_series('country/USA', 'Count_Person',
                               'CensusPEPSurvey')))


def build_time_series_dataframe_example():

    def demonstrate_build_time_series_dataframe(intro_str,
                                                places,
                                                stat_var,
                                                desc_col=False):
        arg_str = "{}, '{}'".format(places, stat_var)
        if desc_col:
            arg_str += ", desc_col=True"
        print("""
    # {}
    $ dcpd.build_time_series_dataframe({})
    {}""".format(intro_str, arg_str,
                 dcpd.build_time_series_dataframe(places, stat_var, desc_col)))

    build_time_series_dataframe_params = [{
        'intro_str':
            'Build a DataFrame of time series for one variable in multiple places.',
        'places': ['geoId/33', 'geoId/29', 'country/USA'],
        'stat_var':
            'Median_Income_Person'
    }, {
        'intro_str':
            'Build a DataFrame of time series with columns sorted in descending order.',
        'places': ['country/USA'],
        'stat_var':
            'Median_Income_Person',
        'desc_col':
            True
    }]

    for param_set in build_time_series_dataframe_params:
        demonstrate_build_time_series_dataframe(**param_set)


def build_multivariate_dataframe_example():

    def demonstrate_build_multivariate_dataframe(intro_str, places, stat_vars):
        print("""
    # {}
    $ dcpd.build_multivariate_dataframe({}, {})
    {}""".format(intro_str, places, stat_vars,
                 dcpd.build_multivariate_dataframe(places, stat_vars)))

    build_multivariate_dataframe_params = [{
        'intro_str':
            'Build a DataFrame of latest observations for multiple variables in multiple places.',
        'places': ['geoId/06', 'country/FRA'],
        'stat_vars': ['Median_Age_Person', 'Count_Person', 'Count_Household']
    }]

    for param_set in build_multivariate_dataframe_params:
        demonstrate_build_multivariate_dataframe(**param_set)


def expect_err_examples():

    print("\n\nExpect 6 errors, starting HERE:")
    try:
        dcpd.build_time_series_dataframe(
            ['geoId/33'], ['Median_Income_Person', 'Count_Person'])
    except ValueError as e:
        print("Successfully errored on: ", e)
    try:
        dcpd.build_time_series_dataframe(24, ['Median_Income_Person'])
    except ValueError as e:
        print("Successfully errored on: ", e)
    try:
        dcpd.build_multivariate_dataframe(
            [3], ['Median_Income_Person', 'Count_Person'])
    except ValueError as e:
        print("Successfully errored on: ", e)
    try:
        dcpd.build_multivariate_dataframe('country/USA', True)
    except ValueError as e:
        print("Successfully errored on: ", e)
    # If the following two do not error due to the addition of
    # Median_Income_Person statistics for NUTS geos, then please
    # replace either the places or the StatVar.
    try:
        dcpd.build_time_series_dataframe(['nuts/HU2', 'nuts/HU22'],
                                         'Median_Income_Person')
    except ValueError as e:
        print("Successfully errored on: ", e)
    try:
        dcpd.build_multivariate_dataframe(['nuts/HU2', 'nuts/HU22'],
                                          ['Median_Income_Person'])
    except ValueError as e:
        print("Successfully errored on: ", e)
    print("until HERE.")


def main():
    build_time_series_example()
    build_time_series_dataframe_example()
    build_multivariate_dataframe_example()
    expect_err_examples()


if __name__ == '__main__':
    main()
