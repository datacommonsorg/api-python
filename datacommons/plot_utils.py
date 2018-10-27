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
"""dataCommons plotting utilities.

"""
import matplotlib.pyplot as plt

import pandas as pd

# A temporary constant dictionary of all enumeration types.
# TODO(antaresc): Remove this when support for querying enum types is added
_ENUM_TYPES = {
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
        "USC_10ThGrade"
        "USC_11ThGrade"
        "USC_12ThGradeNoDiploma"
        "USC_1StGrade"
        "USC_2NdGrade"
        "USC_3RdGrade"
        "USC_4ThGrade"
        "USC_5ThGrade"
        "USC_6ThGrade"
        "USC_7ThGrade"
        "USC_8ThGrade"
        "USC_9ThGrade"
        "USC_AssociateDegree"
        "USC_BachelorDegree"
        "USC_DoctorateDegree"
        "USC_GedOrAlternativeCredential"
        "USC_Kindergarten"
        "USC_MasterDegree"
        "USC_NoSchoolingCompleted"
        "USC_NurserySchool"
        "USC_ProfessionalSchoolDegree"
        "USC_RegularHighSchoolDiploma"
        "USC_SomeCollege1OrMoreYearsNoDegree"
        "USC_SomeCollegeLessThan1Year"
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

def get_categorical_data(dc_client,
                         pd_table,
                         seed_col_name,
                         population_type,
                         start_date,
                         end_date,
                         measured_property,
                         stats_type,
                         max_rows=100,
                         free_prop_name=None,
                         free_enum_type=None,
                         **kwargs):
  """ Returns statistics that can be immediately plotted.

  The Pandas DataFrame contains only the original columns of the given
  "pd_table" and observations taken from the specified populations.

  Args:
    dc_client: The dataCommons client.
    pd_table: Pandas dataframe that contains geo entity dcids
    seed_col_name: The name of the column specifying geo locations containing
      populations of interest.
    population_type: Population type like "Person".
    start_date: The start date of the observation (in 'YYYY-mm-dd' form).
    end_date: The end date of the observation (in 'YYYY-mm-dd' form).
    measured_property: observation measured property.
    stats_type: Statistical type like "Median"
    max_rows: The maximum number of rows returned by the query results.
    free_prop_name: A property in the population of interest to treat as a
      free variable. For example, specifying "age" in a population for
      "Person" will query for all person populations with property "age" and
      values specified by the "free_enum_type"
    free_enum_type: The enumeration type that specifies values for the
      "free_prop_name".
    **kwargsL Keyword properties that further define the queried populations

  Returns:
    A Pandas DataFrame containing only the original columns of "pd_table" and
    observations taken from the specified populations. The new column names
    are given by the instances of the specified "free_enum_type" and
    observations are converted to numerical types. Any row containing NaNs are
    filtered out.
  """
  assert bool(free_prop_name) == bool(free_enum_type), 'free_prop_name and free_enum_type must either both be set or unset.'
  init_cols = list(pd_table)

  # Query for all populations
  pop_cols = []
  if free_enum_type:
    if free_enum_type not in _ENUM_TYPES:
      raise ValueError('Invalid enumeration type: {}'.format(free_enum_type))
    for enum_val in _ENUM_TYPES[free_enum_type]:
      if enum_val in pd_table:
        raise RuntimeError(
            'Column {} already exists in pd_table'.format(enum_val))

      # Query for the population
      pop_cols.append(enum_val)
      pop_kwargs = dict(kwargs)
      pop_kwargs[free_prop_name] = enum_val
      pd_table = dc_client.get_populations(
          pd_table=pd_table,
          seed_col_name=seed_col_name,
          new_col_name=enum_val,
          population_type=population_type,
          max_rows=max_rows,
          **pop_kwargs)
  else:
    pop_cols.append(population_type)
    pd_table = dc_client.get_populations(
        pd_table=pd_table,
        seed_col_name=seed_col_name,
        new_col_name=population_type,
        population_type=population_type,
        max_rows=max_rows,
        **kwargs)

  # Query for observations
  obs_cols = []
  for pop_col in pop_cols:
    obs_col_name = "{}_{}_{}_{}".format(
        measured_property, pop_col, start_date, end_date)
    obs_cols.append(obs_col_name)
    pd_table = dc_client.get_observations(
        pd_table=pd_table,
        seed_col_name=pop_col,
        new_col_name=obs_col_name,
        start_date=start_date,
        end_date=end_date,
        measured_property=measured_property,
        stats_type=stats_type,
        max_rows=max_rows)

  # Perform data cleanup
  pd_table[obs_cols] = pd_table[obs_cols].apply(pd.to_numeric, errors='coerce')
  pd_table = pd_table.dropna()
  return pd_table[init_cols + obs_cols]

def get_timeseries_data():
  pass

def plot():
  pass

def scatter():
  pass

def histogram():
  pass
