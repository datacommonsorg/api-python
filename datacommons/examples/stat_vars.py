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
import pprint


def main():
    param_sets = [
        {
            'place': 'geoId/06085',
            'stat_var': "Count_Person",
        },
        {
            'place': 'geoId/06085',
            'stat_var': "Count_Person",
            'date': '2018',
        },
        {
            'place': 'geoId/06085',
            'stat_var': "Count_Person",
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
            'observation_period': 'P1Y',
        },
        {
            'place': 'geoId/06085',
            'stat_var': 'UnemploymentRate_Person',
            'observation_period': 'P1Y',
            'measurement_method': 'BLSSeasonallyUnadjusted',
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
                'P1Y',
            'unit':
                'PurchasingPowerStandard'
        },
    ]

    def call_str(pvs):
        """Helper function to print the minimal call string."""
        s = "'{}', '{}'".format(pvs.get('place'), pvs.get('stat_var'))
        if pvs.get('measurement_method'):
            s += ", measurement_method='{}'".format(
                pvs.get('measurement_method'))
        if pvs.get('observation_period'):
            s += ", observation_period='{}'".format(
                pvs.get('observation_period'))
        if pvs.get('unit'):
            s += ", unit='{}'".format(pvs.get('unit'))
        if pvs.get('scaling_factor'):
            s += ", scaling_factor={}".format(pvs.get('scaling_factor'))
        return s

    for pvs in param_sets:
        print('\nget_stat_value({})'.format(call_str(pvs)))
        print(
            '>>> ',
            dc.get_stat_value(pvs.get('place'),
                              pvs.get('stat_var'),
                              date=pvs.get('date'),
                              measurement_method=pvs.get('measurement_method'),
                              observation_period=pvs.get('observation_period'),
                              unit=pvs.get('unit'),
                              scaling_factor=pvs.get('scaling_factor')))
    for pvs in param_sets:
        pvs.pop('date', None)
        print('\nget_stat_series({})'.format(call_str(pvs)))
        print(
            '>>> ',
            dc.get_stat_series(pvs.get('place'),
                               pvs.get('stat_var'),
                               measurement_method=pvs.get('measurement_method'),
                               observation_period=pvs.get('observation_period'),
                               unit=pvs.get('unit'),
                               scaling_factor=pvs.get('scaling_factor')))

    pp = pprint.PrettyPrinter(indent=4)
    print(
        '\nget_stat_all(["geoId/06085", "country/FRA"], ["Median_Age_Person", "Count_Person"])'
    )
    print('>>> ')
    pp.pprint(
        dc.get_stat_all(["geoId/06085", "country/FRA"],
                        ["Median_Age_Person", "Count_Person"]))

    print(
        '\nget_stat_all(["badPlaceId", "country/FRA"], ["Median_Age_Person", "Count_Person"])'
    )
    print('>>> ')
    pp.pprint(
        dc.get_stat_all(["badPlaceId", "country/FRA"],
                        ["Median_Age_Person", "Count_Person"]))

    print('\nSTRESS TEST FOR GET_STAT_ALL')
    pp.pprint(
        dc.get_stat_all(
            dc.get_places_in(['country/USA'], 'County')['country/USA'], [
                'Count_Person_Unemployed', 'Median_Age_Person', 'Count_Death',
                'Count_Death_CertainInfectiousParasiticDiseases',
                'Count_UnemploymentInsuranceClaim_StateUnemploymentInsurance',
                'Count_Worker_NAICSAccommodationFoodServices',
                'Count_Household_With0AvailableVehicles',
                'Count_Person_WhiteAlone', 'Count_Person_WorkedFullTime',
                'Count_Person_Employed', 'Count_Person_EnrolledInSchool',
                'Count_Person_EnrolledInGrade9ToGrade12',
                'Count_Person_ResidesInGroupQuarters',
                'Count_Person_NowMarried',
                'RetailDrugDistribution_DrugDistribution_DMethamphetamine',
                'Count_Household_With2Person',
                'Count_Person_EducationalAttainment2NdGrade',
                'Count_Person_EducationalAttainmentGedOrAlternativeCredential',
                'Count_Person_EducationalAttainmentKindergarten',
                'Count_Person_EducationalAttainmentRegularHighSchoolDiploma',
                'Count_Death_DiseasesOfBloodAndBloodFormingOrgansAndImmuneDisorders',
                'Count_Household_NoComputer',
                'Median_Income_Household_HouseholderRaceHispanicOrLatino,',
                'Count_HousingUnit_RenterOccupied',
                'Count_Worker_NAICSInformation'
            ]))


if __name__ == '__main__':
    main()
