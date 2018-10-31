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
import numpy as np

_DEFAULT_SCALE = "linear"           # Default matplotlib axis scale for plotting

# ---------------------------- PLOTTING FUNCTIONS -----------------------------

def plot(pd_table,
         pd_time_col,
         pd_data_cols,
         pd_labels=None,
         pd_reserve_cols=[],
         ax=None,
         title="",
         xlabel="",
         ylabel="",
         xscale=_DEFAULT_SCALE,
         yscale=_DEFAULT_SCALE,
         **kwargs):
  """ Plots a time-series with values in pd_data_cols along a single axis.

  Args:
    pd_table: A Pandas dataframe with numerical values along "cols". The table
      is indexed by date time.
    pd_time_col: The column containing the time axis.
    pd_labels: An arraylike of string labels for each series plotted by columns
      specified in "pd_data_cols".
    pd_reserve_cols: A list of columns that are guaranteed to be in the returned
      dataframe.
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
    The dataframe used to generate the time-series plot and a list of Line2D
    objects returned by calling matplotlib.pyplot.plot
  """
  if pd_time_col not in pd_table:
    raise ValueError(
        "Table does not contain time column {}".format(pd_time_col))
  if any(c not in pd_table for c in pd_data_cols):
    raise ValueError(
        "Table does not contain all columns in {}".format(pd_data_cols))

  # Format the data and set the pd_time_col as the table's index
  columns = [pd_time_col] + pd_reserve_cols + pd_data_cols
  pd_table = pd_table.loc[1:, columns].copy()
  pd_table[pd_data_cols] = pd_table[pd_data_cols].apply(pd.to_numeric,
                                                        errors='coerce')
  pd_table[pd_time_col] = pd.to_datetime(pd_table[pd_time_col])
  pd_table = pd_table.set_index(pd_time_col)
  pd_table = pd_table.dropna()

  # Plot the data
  ax = _init_axis(ax, title, xlabel, ylabel, xscale, yscale)
  plots = [ax.plot(pd_table.index, pd_table[c], **kwargs) for c in pd_data_cols]
  if pd_labels:
    ax.legend(pd_labels)
  else:
    ax.legend(pd_data_cols)
  return pd_table, plots

def scatter(pd_table,
            pd_xcol,
            pd_ycols,
            pd_labels=None,
            pd_reserve_cols=[],
            ax=None,
            title="",
            xlabel="",
            ylabel="",
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
    pd_reserve_cols: A list of columns that are guaranteed to be in the returned
      dataframe.
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
    The dataframe used to generate the scatter plot and a list of
    PathCollections returned by calling matplotlib.pyplot.scatter
  """
  if pd_xcol not in pd_table or any(c not in pd_table for c in pd_ycols):
    raise ValueError(
        "Table does not contain all columns in {}, {}".format(pd_xcol, pd_ycols))
  columns = pd_reserve_cols + [pd_xcol] + pd_ycols
  pd_table = pd_table.loc[1:, columns].copy()
  pd_table[pd_xcol] = pd_table[pd_xcol].apply(pd.to_numeric, errors='coerce')
  pd_table[pd_ycols] = pd_table[pd_ycols].apply(pd.to_numeric, errors='coerce')
  pd_table = pd_table.dropna()
  pd_table = pd_table.reset_index()

  # Plot the data
  ax = _init_axis(ax, title, xlabel, ylabel, xscale, yscale)
  sc = [ax.scatter(pd_table[pd_xcol], pd_table[y], **kwargs) for y in pd_ycols]
  if pd_labels:
    ax.legend(pd_labels)
  else:
    ax.legend(pd_ycols)
  return pd_table, sc

def histogram(pd_table,
              pd_cols,
              pd_labels=None,
              pd_reserve_cols=[],
              ax=None,
              title="",
              xlabel="",
              ylabel="",
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
    pd_reserve_cols: A list of columns that are guaranteed to be in the returned
      dataframe.
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
    The dataframe used to generate the histogram and a list of tuples returned
    by calling matplotlib.pyplot.hist
  """
  if any(c not in pd_table for c in pd_cols):
    raise ValueError("Table does not contain all columns in {}".format(pd_cols))
  pd_table = pd_table.loc[1:, pd_reserve_cols + pd_cols].copy()
  pd_table[pd_cols] = pd_table[pd_cols].apply(pd.to_numeric, errors='coerce')

  # Fix the bins to account for all histogram series
  if 'bins' in kwargs:
    data_tuple = tuple(pd_table[c].dropna().values for c in pd_cols)
    kwargs['bins'] = np.histogram(np.hstack(data_tuple), bins=kwargs['bins'])[1]

  # Plot the data
  ax = _init_axis(ax, title, xlabel, ylabel, xscale, yscale)
  hist = [ax.hist(pd_table[c].dropna(), **kwargs) for c in pd_cols]
  if pd_labels:
    ax.legend(pd_labels)
  else:
    ax.legend(pd_cols)
  return pd_table, hist

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
