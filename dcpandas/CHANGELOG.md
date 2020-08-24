# Changelog

## 0.01

**Date** - 08/24/2020

**Release Tag** - [pd.0.0.1](https://github.com/datacommonsorg/api-python/releases/tag/pd0.0.1)

**Release Status** - Current head of branch [`master`](https://github.com/datacommonsorg/api-python/tree/master)

Added Pandas wrapper functions.

-   `build_time_series` will construct a pd.Series for a given StatisticalVariable and Place, where dates are the index for the time series.
-   `build_time_series_dataframe` will construct a pd.DataFrame for a given StatisticalVariable and a set of Places: where Places are the index and date are the columns.
-   `build_covariate_dataframe` will construct a covariate pd.DataFrame for a set of StatisticalVariables and a set of Places: with Places as index and StatisticalVariables as the columns. The values are the most recent values for the chosen StatVarObservation options.

For multi-place functions, when a StatisticalVariable has multiple StatVarObservation options,
Data Commons chooses a set of StatVarObservation options that covers the most geos. This
ensures that the data fetched for a StatisticalVariable is comparable across places.
When there is a tie, we select the StatVarObservation options set with the latest date
data is available for any place.
