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
"""Basic examples for StatisticalVariable-based Data Commons API functions."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datacommons as dc


def main():
    data = [
        {
            'place': 'geoId/06085',
            'stat_var': 'Count_Person',
        },
        {
            'place': 'geoId/06085',
            'stat_var': 'Count_Person',
            'date': '2018',
        },
        {
            'place': 'geoId/06085',
            'stat_var': 'Count_Person',
            'date': '2018',
            'measurement_method': 'CensusACS5yrSurvey',
        },
        {
            'place': 'geoId/06085',
            'stat_var': 'UnemploymentRate_Person',
        },
        {
            'place': 'geoId/06085',
            'stat_var': 'UnemploymentRate_Person',
            'observation_period': "P1Y",
        },
        {
            'place': 'geoId/06085',
            'stat_var': 'UnemploymentRate_Person',
            'observation_period': "P1Y",
            'measurement_method': "BLSSeasonallyUnadjusted",
        },
        {
            'place':
                'nuts/HU22',
            'stat_var':
                'Amount_EconomicActivity_GrossDomesticProduction_Nominal',
        },
        {
            'place':
                'nuts/HU22',
            'stat_var':
                'Amount_EconomicActivity_GrossDomesticProduction_Nominal',
            'observation_period':
                "P1Y",
            'unit':
                "PurchasingPowerStandard"
        },
    ]

    for d in data:
        print('\n>>> get_stat_value: ',
              [param for param in d.values() if param])
        print(
            '<<< ',
            dc.get_stat_value(d.get('place'),
                              d.get('stat_var'),
                              date=d.get('date'),
                              measurement_method=d.get('measurement_method'),
                              observation_period=d.get('observation_period'),
                              unit=d.get('unit'),
                              scaling_factor=d.get('scaling_factor')))
    for d in data:
        print('\n>>> get_stat_series: ',
              [d[k] for k in d.keys() if k != 'date' and d[k]])
        print(
            '<<< ',
            dc.get_stat_series(d.get('place'),
                               d.get('stat_var'),
                               measurement_method=d.get('measurement_method'),
                               observation_period=d.get('observation_period'),
                               unit=d.get('unit'),
                               scaling_factor=d.get('scaling_factor')))


if __name__ == '__main__':
    main()
