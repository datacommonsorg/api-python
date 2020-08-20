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

import sys
sys.path.append('../')
sys.path.append('../../')
import datacommons as dc


def main():
    # Dcid for Santa Clara County.
    sc = 'geoId/06085'

    # Get stat value.
    print('get_stat_value Count_Person')
    print(dc.get_stat_value(sc, 'Count_Person'))

    print('get_stat_value Count_Person 2018')
    print(dc.get_stat_value(sc, 'Count_Person', '2018'))
    print('get_stat_value Count_Person 2018 from ACS 5 yr')
    print(
        dc.get_stat_value(sc,
                          'Count_Person',
                          '2018',
                          measurement_method='CensusACS5yrSurvey'))

    # Get stat series.
    print('get_stat_series Count_Person')
    print(dc.get_stat_series(sc, 'Count_Person'))

    print('get_stat_series UnemploymentRate_Person')
    print(dc.get_stat_series(sc, 'UnemploymentRate_Person'))

    print('get_stat_series UnemploymentRate_Person for observationPeriod P1Y')
    print(
        dc.get_stat_series(sc,
                           'UnemploymentRate_Person',
                           observation_period="P1Y"))

    print(
        'get_stat_series UnemploymentRate_Person for observationPeriod P1Y and mmethod Unadjusted'
    )
    print(
        dc.get_stat_series(sc,
                           'UnemploymentRate_Person',
                           measurement_method="BLSSeasonallyUnadjusted",
                           observation_period="P1Y"))

    print('get_stat_series Nominal GDP')
    print(
        dc.get_stat_series(
            'nuts/HU22',
            'Amount_EconomicActivity_GrossDomesticProduction_Nominal'))

    print('get_stat_series Nominal GDP with unit PurchasingPowerStandard')
    print(
        dc.get_stat_series(
            'nuts/HU22',
            'Amount_EconomicActivity_GrossDomesticProduction_Nominal',
            observation_period="P1Y",
            unit="PurchasingPowerStandard"))


if __name__ == '__main__':
    main()
