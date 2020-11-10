# Changelog

## 0.0.3

**Date** - 11/10/2020

**Release Tag** - [pd.0.0.3](https://github.com/datacommonsorg/api-python/releases/tag/pd0.0.3)

**Release Status** - Current head of branch [`master`](https://github.com/datacommonsorg/api-python/tree/master)

Update to use datacommons Python API 1.4.3, which returns empty data structures instead of erroring when no data is available.

## 0.0.2

**Date** - 09/16/2020

**Release Tag** - [pd.0.0.2](https://github.com/datacommonsorg/api-python/releases/tag/pd0.0.2)

**Release Status** - Current head of branch [`master`](https://github.com/datacommonsorg/api-python/tree/master)

Update to use datacommons Python API 1.4.2, which adds batching to the get_stat_all function used by build_time_series_dataframe and build_multivariate_dataframe.

## 0.0.1

**Date** - 08/25/2020

**Release Tag** - [pd.0.0.1](https://github.com/datacommonsorg/api-python/releases/tag/pd0.0.1)

**Release Status** - Current head of branch [`master`](https://github.com/datacommonsorg/api-python/tree/master)

Added pandas wrapper functions.

-   `build_time_series` constructs a pd.Series for a given StatisticalVariable and Place, where the time series are indexed by date.
-   `build_time_series_dataframe` constructs a pd.DataFrame for a given StatisticalVariable and a set of Places. The DataFrame will have Places as the index and dates as the columns.
-   `build_multivariate_dataframe` constructs a pd.DataFrame for a set of StatisticalVariables and a set of Places. The DataFrame will have Places as index and StatisticalVariables as the columns. The values are the most recent values for the chosen StatVarObservation options.

For multi-place functions, when a StatisticalVariable has multiple StatVarObservation options,
Data Commons chooses a set of StatVarObservation options that covers the most places. This
ensures that the data fetched for a StatisticalVariable is comparable across places.
When there is a tie, we select the StatVarObservation options set with the latest date
data is available for any place.
