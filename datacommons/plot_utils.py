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
_DEFAULT_CMAP = "tab20"
_DEFAULT_ALPHA = 0.75
_DEFAULT_ROTATION = 90

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
    obs_col_name = "{}_{}".format(pop_col, end_date)
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
         cols,
         figsize=(6, 4),
         title="",
         xlabel="",
         ylabel="",
         xscale=_DEFAULT_SCALE,
         yscale=_DEFAULT_SCALE,
         alpha=_DEFAULT_ALPHA,
         cmap=None,
         legend=True):
  """ Plots a time-series with values in cols along a single axis.

  Args:
    pd_table: A Pandas dataframe with numerical values along "cols". The table
      is indexed by date time.
    cols: A list of column names to plot on the time-series. Each entry must be
      a name of a column in "pd_table".
    figsize: The size of the figure as a tuple (width, height)
    title: The title of the plot
    xlabel: The x-axis label of the plot
    ylabel: The y-axis label of the plot
    xscale: The scale of the x-axis. This takes values specified by plt.xscale
      in the Matplotlib library.
    yscale: The scale of the y-axis. This takes values specified by plt.yscale
      in the Matplotlib library.
    alpha: The alpha value for lines along the plot
    cmap: The colormap object for plotting the table's data. By default this is
      set to "tab20b".
    legend: Set to show the plot's legend.

  Returns:
    A matplotlib pyplot object containing the plotted data.
  """
  if any(c not in pd_table for c in cols):
    raise ValueError("Table does not contain all columns in {}".format(cols))
  cols = set(cols)
  pd_table = pd_table.loc[1:]

  # Get the colormap
  colors = mpl.cm.get_cmap(_DEFAULT_CMAP)
  if isinstance(cmap, str):
    colors = mpl.cm.get_cmap(cmap)
  elif cmap:
    colors = cmap

  # Plot the data
  plt.figure(figsize=figsize)
  main_axis = _init_axis(title, xlabel, ylabel, xscale, yscale)
  axes, norm = [], mpl.colors.Normalize(vmin=0, vmax=len(cols))
  for idx, col_name in enumerate(cols):
    color_idx = norm(idx)
    new_ax = pd_table[col_name].plot(ax=main_axis,
                                     color=colors(color_idx),
                                     alpha=alpha,
                                     label=col_name)
    axes.append(new_ax)

  # Set the legend if specified
  if legend:
    handles = sum(ax.get_legend_handle_labels()[0] for a in axes)
    labels = sum(ax.get_legend_handle_labels()[1] for a in axes)
    plt.legend(handles, legends)
  return plt

def scatter(pd_table,
            x_col,
            y_cols,
            figsize=(6, 4),
            title="",
            xlabel="",
            ylabel="",
            xscale=_DEFAULT_SCALE,
            yscale=_DEFAULT_SCALE,
            alpha=_DEFAULT_ALPHA,
            cmap=None,
            legend=True):
  """ Plots a scatterplot with the specified arguments.

  The scatterplot plots data fixing x to be values specified in the "x_col"
  column and varying y to be values specified by columns in "y_col"

  Args:
    pd_table: A Pandas dataframe with numerical values along "cols". The table
      is indexed by date time.
    x_col: The column name to sample x-values from
    y_cols: A list of column names to sample y-values from. Each y column is
      plotted with the x column as a different color.
    figsize: The size of the figure as a tuple (width, height)
    title: The title of the plot
    xlabel: The x-axis label of the plot
    ylabel: The y-axis label of the plot
    xscale: The scale of the x-axis. This takes values specified by plt.xscale
      in the Matplotlib library.
    yscale: The scale of the y-axis. This takes values specified by plt.yscale
      in the Matplotlib library.
    alpha: The alpha value for lines along the plot
    cmap: The colormap object for plotting the table's data. By default this is
      set to "tab20b".
    legend: Set to show the plot's legend.

  Returns:
    A matplotlib pyplot object containing the plotted data.
  """
  if x_col not in pd_table or any(c not in pd_table for c in y_cols):
    raise ValueError(
        "Table does not contain all columns in {}, {}".format(x_col, y_cols))
  y_cols = set(y_cols)
  pd_table = pd_table.loc[1:]

  # Get the colormap
  colors = mpl.cm.get_cmap(_DEFAULT_CMAP)
  if isinstance(cmap, str):
    colors = mpl.cm.get_cmap(cmap)
  elif cmap:
    colors = cmap

  # Plot the data
  plt.figure(figsize=figsize)
  main_axis = _init_axis(title, xlabel, ylabel, xscale, yscale)
  norm = mpl.colors.Normalize(vmin=0, vmax=len(y_cols))
  for idx, y_col_name in enumerate(y_cols):
    color_idx = norm(idx)
    main_axis.scatter(pd_table[x_col],
                      pd_table[y_col_name],
                      color=colors(color_idx),
                      alpha=alpha)

  # Set the legend if specified
  if legend:
    handles = []
    for idx, y_col_name in enumerate(y_cols):
      color_idx = norm(idx)
      handles.append(mpatches.Patch(color=colors(color_idx), label=y_col_name))
    plt.legend(handles=handles)
  return plt

def histogram(pd_table,
              series_col,
              data_cols,
              figsize=(6, 4),
              title="",
              ylabel="",
              yscale=_DEFAULT_SCALE,
              alpha=_DEFAULT_ALPHA,
              cmap=None,
              legend=True):
  """ Plots a histogram with the specified arguments.

  For each row in "series_col", the histogram will plot a set of bars specified
  by the "data_cols".

  Args:
    pd_table: A Pandas dataframe with numerical values along "cols". The table
      is indexed by date time.
    series_col: The column name specifying each data series
    data_cols: A list of column names to sample each histogram bin from.
    figsize: The size of the figure as a tuple (width, height)
    title: The title of the plot
    ylabel: The y-axis label of the plot
    yscale: The scale of the y-axis. This takes values specified by plt.yscale
      in the Matplotlib library.
    alpha: The alpha value for lines along the plot
    cmap: The colormap object for plotting the table's data. By default this is
      set to "tab20b".
    legend: Set to show the plot's legend.

  Returns:
    A matplotlib pyplot object containing the plotted data.
  """
  if series_col not in pd_table or any(c not in pd_table for c in data_cols):
    raise ValueError("Table does not contain all columns in {}, {}".format(
        series_col, data_cols))
  data_cols = set(data_cols)
  pd_table = pd_table.loc[1:]

  # Get the colormap
  colors = mpl.cm.get_cmap(_DEFAULT_CMAP)
  if isinstance(cmap, str):
    colors = mpl.cm.get_cmap(cmap)
  elif cmap:
    colors = cmap

  # Create the data and position maps
  data_vals, data_pos, data_widths = {}, {}, {}
  width = 2                     # The width of each set of bars + spacing
  set_width = 1.5              # The width of each set of bars
  bar_width = set_width / len(pd_table[series_col]) # The width of a single bar
  for series_idx, name in enumerate(pd_table[series_col]):
    pd_row = pd_table.loc[pd_table[series_col] == name].squeeze()
    data_vals[name] = pd_row[data_cols]
    data_pos[name] = [data_idx * width + series_idx * bar_width for data_idx in range(len(data_cols))]
    data_widths[name] = [bar_width for data_idx in range(len(data_cols))]

  # Plot the data
  plt.figure(figsize=figsize)
  ax = _init_axis(title, "", ylabel, _DEFAULT_SCALE, yscale)
  norm = mpl.colors.Normalize(vmin=0, vmax=len(pd_table[series_col]))
  for idx, name in enumerate(pd_table[series_col]):
    color_idx = norm(idx)
    ax.bar(data_pos[name],
           data_vals[name].values,
           data_widths[name],
           color=colors(color_idx),
           alpha=alpha)

  # Set the legend
  tick_pos = [data_idx * width + (set_width / 2) for data_idx in range(len(data_cols))]
  plt.xticks(tick_pos, data_cols, rotation=_DEFAULT_ROTATION)
  if legend:
    handles = []
    for idx, name in enumerate(pd_table[series_col]):
      color_idx = norm(idx)
      handles.append(mpatches.Patch(color=colors(color_idx), label=name))
    plt.legend(handles=handles)
  return plt


# ------------------------- INTERNAL HELPER FUNCTIONS -------------------------

def _init_axis(title, xlabel, ylabel, xscale, yscale):
  """ Initializes a matplotlib plot object with the set parameters.

  Args:
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
  ax = plt.gca()
  ax.set_title(title)
  ax.set_xlabel(xlabel)
  ax.set_ylabel(ylabel)
  ax.set_xscale(xscale)
  ax.set_yscale(yscale)
  return ax
