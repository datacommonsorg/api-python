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


def main():

    print("""
# Build a pd.Series of time series for one variable and one place.
$ dcpd.build_time_series("country/CAN", "Count_WildlandFireEvent")
{}""".format(dcpd.build_time_series("country/CAN", "Count_WildlandFireEvent")))

    print("""
# Build a pd.Series of time series for one variable and one place and optional args.
$ dcpd.build_time_series("country/USA", "Count_Person", "CensusPEPSurvey")
{}""".format(dcpd.build_time_series("country/CAN", "Count_WildlandFireEvent")))

    print("""
# Build a DataFrame of time series for one variable in multiple places.
$ dcpd.build_time_series_dataframe(["geoId/29", "geoId/33"], "Median_Income_Person")
{}""".format(
        dcpd.build_time_series_dataframe(
            ["geoId/33", "geoId/29", "country/USA"], "Median_Income_Person")))

    print("""
# Build a DataFrame of time series with columns sorted in descending order.
$ dcpd.build_time_series_dataframe(["country/USA"], "Median_Income_Person", desc_col=True)
{}""".format(
        dcpd.build_time_series_dataframe(["country/USA"],
                                         "Median_Income_Person",
                                         desc_col=True)))

    print("""
# Build a DataFrame of latest observations for multiple variables in multiple places.
$ dcpd.build_multivariate_dataframe(["geoId/06", "country/FRA"], ["Median_Age_Person", "Count_Person", "Count_Household"])
{}""".format(
        dcpd.build_multivariate_dataframe(
            ["geoId/06", "country/FRA"],
            ["Median_Age_Person", "Count_Person", "Count_Household"])))

    print('\n\nExpect 6 errors, starting HERE:')
    try:
        dcpd.build_time_series_dataframe(
            ["geoId/33"], ["Median_Income_Person", "Count_Person"])
    except ValueError as e:
        print("Successfully errored on: ", e)
    try:
        dcpd.build_time_series_dataframe(24, ["Median_Income_Person"])
    except ValueError as e:
        print("Successfully errored on: ", e)
    try:
        dcpd.build_multivariate_dataframe(
            [3], ["Median_Income_Person", "Count_Person"])
    except ValueError as e:
        print("Successfully errored on: ", e)
    try:
        dcpd.build_multivariate_dataframe("country/USA", True)
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
    print('until HERE.')


if __name__ == '__main__':
    main()
