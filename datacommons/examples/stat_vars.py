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


    print('\nWhen no data for get_stat_value')
    pp.pprint(dc.get_stat_value('foooo', 'barrrr'))

    print('\nWhen no data for get_stat_series')
    pp.pprint(dc.get_stat_series('foobarbar', 'barfoo'))

    print('\nSTRESS TEST FOR GET_STAT_ALL')
    try:
        dc.get_stat_all(
            dc.get_places_in(['country/USA'], 'County')['country/USA'], [
                'Count_Person', 'LandAreaSqMeter',
                'PopulationDensityPerSqMeter',
                'Count_Person_BlackOrAfricanAmericanAlone',
                'PercentBlackOrAfricanAmericanAlone', 'Count_Person_Female',
                'Count_Person_Male',
                'Count_Person_AmericanIndianAndAlaskaNativeAlone',
                'Count_Person_AmericanIndianAndAlaskaNativeAloneOrInCombinationWithOneOrMoreOtherRaces',
                'Count_Person_AmericanIndianOrAlaskaNativeAlone',
                'Count_Person_AsianAlone',
                'Count_Person_AsianAloneOrInCombinationWithOneOrMoreOtherRaces',
                'Count_Person_BlackOrAfricanAmericanAloneOrInCombinationWithOneOrMoreOtherRaces',
                'Count_Person_HispanicOrLatino',
                'Count_Person_NativeHawaiianAndOtherPacificIslanderAlone',
                'Count_Person_NativeHawaiianAndOtherPacificIslanderAloneOrInCombinationWithOneOrMoreOtherRaces',
                'Count_Person_NativeHawaiianOrOtherPacificIslanderAlone',
                'Count_Person_SomeOtherRaceAlone',
                'Count_Person_SomeOtherRaceAloneOrInCombinationWithOneOrMoreOtherRaces',
                'Count_Person_TwoOrMoreRaces', 'Count_Person_WhiteAlone',
                'Count_Person_WhiteAloneNotHispanicOrLatino',
                'Count_Person_WhiteAloneOrInCombinationWithOneOrMoreOtherRaces',
                'Count_Person_Upto5Years', 'Count_Person_Upto18Years',
                'Count_Person_65OrMoreYears', 'Count_Person_75OrMoreYears',
                'Count_Person_ForeignBorn',
                'Count_Person_USCitizenByNaturalization',
                'Count_Person_NotAUSCitizen', 'Count_Person_Nonveteran',
                'Count_Person_Veteran', 'Count_Person_NotWorkedFullTime',
                'Count_Person_WorkedFullTime', 'Count_Person_Employed',
                'Count_Person_Unemployed', 'Count_Person_InLaborForce',
                'Count_Person_IncomeOf10000To14999USDollar',
                'Count_Person_IncomeOf15000To24999USDollar',
                'Count_Person_IncomeOf25000To34999USDollar',
                'Count_Person_IncomeOf35000To49999USDollar',
                'Count_Person_IncomeOf50000To64999USDollar',
                'Count_Person_IncomeOf65000To74999USDollar',
                'Count_Person_IncomeOf75000OrMoreUSDollar',
                'Count_Person_IncomeOfUpto9999USDollar',
                'Count_Person_EnrolledInSchool',
                'Count_Person_NotEnrolledInSchool',
                'Count_Person_EnrolledInCollegeUndergraduateYears',
                'Count_Person_EnrolledInGrade1ToGrade4',
                'Count_Person_EnrolledInGrade5ToGrade8',
                'Count_Person_EnrolledInGrade9ToGrade12',
                'Count_Person_EnrolledInKindergarten',
                'Count_Person_EnrolledInNurserySchoolPreschool',
                'Count_Person_GraduateOrProfessionalSchool',
                'Count_Person_EducationalAttainment10ThGrade',
                'Count_Person_EducationalAttainment11ThGrade',
                'Count_Person_EducationalAttainment12ThGradeNoDiploma',
                'Count_Person_EducationalAttainment1StGrade',
                'Count_Person_EducationalAttainment2NdGrade',
                'Count_Person_EducationalAttainment3RdGrade',
                'Count_Person_EducationalAttainment4ThGrade',
                'Count_Person_EducationalAttainment5ThGrade',
                'Count_Person_EducationalAttainment6ThGrade',
                'Count_Person_EducationalAttainment7ThGrade',
                'Count_Person_EducationalAttainment8ThGrade',
                'Count_Person_EducationalAttainment9ThGrade',
                'Count_Person_EducationalAttainmentAssociatesDegree',
                'Count_Person_EducationalAttainmentBachelorsDegree',
                'Count_Person_EducationalAttainmentBachelorsDegreeOrHigher',
                'Count_Person_EducationalAttainmentDoctorateDegree',
                'Count_Person_EducationalAttainmentGedOrAlternativeCredential',
                'Count_Person_EducationalAttainmentKindergarten',
                'Count_Person_EducationalAttainmentMastersDegree',
                'Count_Person_EducationalAttainmentNoSchoolingCompleted',
                'Count_Person_EducationalAttainmentNurserySchool',
                'Count_Person_EducationalAttainmentPrimarySchool',
                'Count_Person_EducationalAttainmentProfessionalSchoolDegree',
                'Count_Person_EducationalAttainmentRegularHighSchoolDiploma',
                'Count_Person_EducationalAttainmentSomeCollege1OrMoreYearsNoDegree',
                'Count_Person_EducationalAttainmentSomeCollegeLessThan1Year',
                'Count_Person_Divorced', 'Count_Person_MarriedAndNotSeparated',
                'Count_Person_NeverMarried', 'Count_Person_Separated',
                'Count_Person_Widowed', 'Count_Person_NowMarried',
                'Count_Person_AbovePovertyLevelInThePast12Months',
                'Count_Person_BelowPovertyLevelInThePast12Months',
                'Percent_Person_20OrMoreYears_WithDiabetes',
                'Percent_Person_20OrMoreYears_Obesity',
                'Percent_Person_20OrMoreYears_PhysicalInactivity',
                'Percent_Person_Upto64Years_NoHealthInsurance',
                'Median_Age_Person', 'Median_Income_Person', 'Count_Death',
                'Count_Death_CertainInfectiousParasiticDiseases',
                'Count_Death_DiseasesOfBloodAndBloodFormingOrgansAndImmuneDisorders',
                'Count_Death_DiseasesOfTheRespiratorySystem'
            ])
    except ValueError:
        print('Stress test for get_stat_all FAILED!')
    else:
        print('Stress test for get_stat_all succeeded.')


if __name__ == '__main__':
    main()
