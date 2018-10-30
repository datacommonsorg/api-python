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
"""Temporary constants used in the DataCommons API.

This should be removed once it's possible to query for enumeration instances.
"""

ENUM_TYPES = {
    "USC_AgeEnum" : [
        "USC_18To24Years",
        "USC_25To34Years",
        "USC_35To44Years",
        "USC_45To54Years",
        "USC_55To59Years",
        "USC_5To17Years",
        "USC_60And61Years",
        "USC_62To64Years",
        "USC_65To74Years",
        "USC_75YearsAndOver",
        "USC_Under5Years",
    ],
    "USC_EducationEnum" : [
        "USC_10ThGrade",
        "USC_11ThGrade",
        "USC_12ThGradeNoDiploma",
        "USC_1StGrade",
        "USC_2NdGrade",
        "USC_3RdGrade",
        "USC_4ThGrade",
        "USC_5ThGrade",
        "USC_6ThGrade",
        "USC_7ThGrade",
        "USC_8ThGrade",
        "USC_9ThGrade",
        "USC_AssociateDegree",
        "USC_BachelorDegree",
        "USC_DoctorateDegree",
        "USC_GedOrAlternativeCredential",
        "USC_Kindergarten",
        "USC_MasterDegree",
        "USC_NoSchoolingCompleted",
        "USC_NurserySchool",
        "USC_ProfessionalSchoolDegree",
        "USC_RegularHighSchoolDiploma",
        "USC_SomeCollege1OrMoreYearsNoDegree",
        "USC_SomeCollegeLessThan1Year",
    ],
    "USC_IncomeEnum": [
        "USC_LessThan10000",
        "USC_10000To14999",
        "USC_15000To19999",
        "USC_20000To24999",
        "USC_25000To29999",
        "USC_30000To34999",
        "USC_35000To39999",
        "USC_40000To44999",
        "USC_45000To49999",
        "USC_50000To59999",
        "USC_60000To74999",
        "USC_75000To99999",
        "USC_100000To124999",
        "USC_125000To149999",
        "USC_150000To199999",
        "USC_200000OrMore",
    ],
    "USC_OccupationEnum" : [
        "USC_ManagementBusinessScienceAndArtsOccupations",
        "USC_ManagementBusinessScienceAndArtsOccupations_ComputerEngineeringAndScienceOccupations",
        "USC_ManagementBusinessScienceAndArtsOccupations_EducationLegalCommunityServiceArtsAndMediaOccupations",
        "USC_ManagementBusinessScienceAndArtsOccupations_HealthcarePractitionersAndTechnicalOccupations",
        "USC_ManagementBusinessScienceAndArtsOccupations_ManagementBusinessAndFinancialOccupations",
        "USC_NaturalResourcesConstructionAndMaintenanceOccupations",
        "USC_NaturalResourcesConstructionAndMaintenanceOccupations_ConstructionAndExtractionOccupations",
        "USC_NaturalResourcesConstructionAndMaintenanceOccupations_FarmingFishingAndForestryOccupations",
        "USC_NaturalResourcesConstructionAndMaintenanceOccupations_InstallationMaintenanceAndRepairOccupations",
        "USC_ProductionTransportationAndMaterialMovingOccupations",
        "USC_ProductionTransportationAndMaterialMovingOccupations_MaterialMovingOccupations",
        "USC_ProductionTransportationAndMaterialMovingOccupations_ProductionOccupations",
        "USC_ProductionTransportationAndMaterialMovingOccupations_TransportationOccupations",
        "USC_SalesAndOfficeOccupations",
        "USC_SalesAndOfficeOccupations_OfficeAndAdministrativeSupportOccupations",
        "USC_SalesAndOfficeOccupations_SalesAndRelatedOccupations",
        "USC_ServiceOccupations",
        "USC_ServiceOccupations_BuildingAndGroundsCleaningAndMaintenanceOccupations",
        "USC_ServiceOccupations_FoodPreparationAndServingRelatedOccupations",
        "USC_ServiceOccupations_HealthcareSupportOccupations",
        "USC_ServiceOccupations_PersonalCareAndServiceOccupations",
        "USC_ServiceOccupations_ProtectiveServiceOccupations",
    ],
    "FBI_CrimeTypeEnum" : [
        "FBI_Property",
        "FBI_PropertyArson",
        "FBI_PropertyBurglary",
        "FBI_PropertyLarcenyTheft",
        "FBI_PropertyMotorVehicleTheft",
        "FBI_ViolentAggravatedAssault",
        "FBI_ViolentMurderAndNonNegligentManslaughter",
        "FBI_ViolentRape",
        "FBI_ViolentRobbery",
    ],
}
