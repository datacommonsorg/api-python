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
import matplotlib.patches as mpatches
import matplotlib as mpl

import pandas as pd

# --------------------------------- CONSTANTS ---------------------------------

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

# Matplotlib plotting constants
_DEFAULT_SCALE = "linear"

# ----------------------- PLOTTING DATA QUERY FUNCTIONS -----------------------

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
    obs_col_name = "{}/{}/{}".format(pop_col, measured_property, end_date)
    if len(kwargs) > 0:
      obs_col_name += "/" + "/".join(kwargs.values())
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
  pd_head = pd_table.loc[0, init_cols + obs_cols].to_frame().T
  pd_data = pd_table.loc[1:, init_cols + obs_cols]
  pd_data[obs_cols] = pd_data[obs_cols].apply(pd.to_numeric, errors='coerce')
  pd_data = pd_data.dropna()
  return pd_head.append(pd_data, ignore_index=True)

def get_timeseries_data():
  pass

# ---------------------------- PLOTTING FUNCTIONS -----------------------------

def plot(pd_table,
         pd_cols,
         pd_labels=None,
         ax=None,
         title=None,
         xlabel=None,
         ylabel=None,
         xscale=_DEFAULT_SCALE,
         yscale=_DEFAULT_SCALE,
         **kwargs):
  """ Plots a time-series with values in pd_cols along a single axis.

  Args:
    pd_table: A Pandas dataframe with numerical values along "cols". The table
      is indexed by date time.
    pd_labels: An arraylike of string labels for each series plotted by columns
      specified in "pd_cols"
    ax: The pyplot axis to plot the histogram on. If ax is None then the default
      axis is used.
    title: The title of the plot
    xlabel: The x-axis label of the plot
    ylabel: The y-axis label of the plot
    xscale: The scale of the x-axis. This takes values specified by plt.xscale
      in the Matplotlib library.
    yscale: The scale of the y-axis. This takes values specified by plt.yscale
      in the Matplotlib library.
    **kwargs: Any keyword arguments passed into matplotlib.pyplot.plot

  Returns:
    A list of Line2D objects returned by calling matplotlib.pyplot.plot
  """
  if any(c not in pd_table for c in pd_cols):
    raise ValueError("Table does not contain all columns in {}".format(pd_cols))
  pd_table = pd_table.loc[1:].copy()
  pd_table[pd_cols] = pd_table[pd_cols].apply(pd.to_numeric, errors='coerce')

  # Plot the data
  ax = _init_axis(ax, title, xlabel, ylabel, xscale, yscale)
  plots = [ax.plot(pd_table.index, pd_table[c], **kwargs) for c in pd_cols]
  if pd_labels:
    ax.legend(pd_labels)
  else:
    ax.legend(pd_cols)
  return plots

def scatter(pd_table,
            pd_xcol,
            pd_ycols,
            pd_labels=None,
            ax=None,
            title=None,
            xlabel=None,
            ylabel=None,
            xscale=_DEFAULT_SCALE,
            yscale=_DEFAULT_SCALE,
            **kwargs):
  """ Plots a scatterplot with the specified arguments.

  The scatterplot plots data fixing x to be values specified in the "pd_xcol"
  column and varying y to be values specified by columns in "pd_ycols"

  Args:
    pd_table: A Pandas dataframe with numerical values along "cols". The table
      is indexed by date time.
    pd_xcol: The column name to sample x-values from
    pd_ycols: A list of column names to sample y-values from. Each y column is
      plotted with the x column as a different color.
    pd_labels: An arraylike of string labels for each series plotted by columns
      specified in "pd_ycols"
    ax: The pyplot axis to plot the histogram on. If ax is None then the default
      axis is used.
    title: The title of the plot
    xlabel: The x-axis label of the plot
    ylabel: The y-axis label of the plot
    xscale: The scale of the x-axis. This takes values specified by plt.xscale
      in the Matplotlib library.
    yscale: The scale of the y-axis. This takes values specified by plt.yscale
      in the Matplotlib library.
    **kwargs: Any keyword arguments passed into matplotlib.pyplot.scatter

  Returns:
    A list of PathCollections returned by calling matplotlib.pyplot.scatter
  """
  if pd_xcol not in pd_table or any(c not in pd_table for c in pd_ycols):
    raise ValueError(
        "Table does not contain all columns in {}, {}".format(pd_xcol, pd_ycols))
  pd_table = pd_table.loc[1:].copy()
  pd_table[pd_cols] = pd_table[pd_cols].apply(pd.to_numeric, errors='coerce')

  # Plot the data
  ax = _init_axis(ax, title, xlabel, ylabel, xscale, yscale)
  sc = [ax.scatter(pd_table[pd_xcol], pd_table[y], **kwargs) for y in pd_ycols]
  if pd_labels:
    ax.legend(pd_labels)
  else:
    ax.legend(pd_ycols)
  return sc

def histogram(pd_table,
              pd_cols,
              pd_labels=None,
              ax=None,
              title=None,
              xlabel=None,
              ylabel=None,
              xscale=_DEFAULT_SCALE,
              yscale=_DEFAULT_SCALE,
              **kwargs):
  """ Plots a histogram using values in each column specified in "pd_cols"

  Each column in "pd_cols" is plotted as a separate curve in the histogram.

  Args:
    pd_table: A Pandas dataframe with numerical values along "pd_cols"
    pd_cols: Column names specifying each histogram curve
    pd_labels: An arraylike of string labels for each series plotted by columns
      specified in "pd_cols"
    ax: The pyplot axis to plot the histogram on. If ax is None then the default
      axis is used.
    title: The title of the plot
    xlabel: The x-axis label of the plot
    ylabel: The y-axis label of the plot
    xscale: The scale of the x-axis. This takes values specified by plt.xscale
      in the Matplotlib library.
    yscale: The scale of the y-axis. This takes values specified by plt.yscale
      in the Matplotlib library.
    **kwargs: Any keyword arguments passed into matplotlib.pyplot.hist

  Returns:
    A list of tuples returned by calling matplotlib.pyplot.hist
  """
  if any(c not in pd_table for c in pd_cols):
    raise ValueError("Table does not contain all columns in {}".format(pd_cols))
  pd_table = pd_table.loc[1:].copy()
  pd_table[pd_cols] = pd_table[pd_cols].apply(pd.to_numeric, errors='coerce')

  # Plot the data
  ax = _init_axis(ax, title, xlabel, ylabel, xscale, yscale)
  hist = []
  for c in pd_cols:
    n, bins, patches = ax.hist(pd_table[c], **kwargs)
    hist.append((n, bins, patches))
    if 'bins' in kwargs:
      kwargs['bins'] = bins
  if pd_labels:
    ax.legend(pd_labels)
  else:
    ax.legend(pd_cols)
  return hist

# ------------------------- INTERNAL HELPER FUNCTIONS -------------------------

def _init_axis(ax, title, xlabel, ylabel, xscale, yscale):
  """ Initializes a matplotlib plot object with the set parameters.

  Args:
    ax: The axis of a matplotlib pyplot object. If ax is None, then the current
      axis is used.
    title: The title of the plot
    xlabel: The x-axis label of the plot
    ylabel: The y-axis label of the plot
    xscale: The scale of the x-axis. This takes values specified by plt.xscale
      in the Matplotlib library.
    yscale: The scale of the y-axis. This takes values specified by plt.yscale
      in the Matplotlib library.

  Returns:
    A matplotlib instance with the specified parameters set.
  """
  if ax is None:
    ax = plt.gca()
  ax.set_title(title)
  ax.set_xlabel(xlabel)
  ax.set_ylabel(ylabel)
  ax.set_xscale(xscale)
  ax.set_yscale(yscale)
  return ax
