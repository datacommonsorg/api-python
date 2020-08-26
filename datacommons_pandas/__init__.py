# Copyright 2020 Google Inc.
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

from datacommons_pandas.df_builder import build_time_series, build_time_series_dataframe, build_multivariate_dataframe

################################ SYMLINK FILES ################################
# We include symlinks to all user-facing functions from the datacommons pkg.  #
# This is so that users do not need to import both libraries for pd support.  #
# Please keep the below in sync with the __init__.py in the datacommons/ dir  #
# TODO: enforce this. https://github.com/datacommonsorg/api-python/issues/149 #
##############################################@################################
# Data Commons SPARQL query support
from datacommons_pandas.query import query

# Data Commons Python API
from datacommons_pandas.core import get_property_labels, get_property_values, get_triples
from datacommons_pandas.places import get_places_in, get_related_places, get_stats
from datacommons_pandas.populations import get_populations, get_observations, get_pop_obs, get_place_obs
from datacommons_pandas.stat_vars import get_stat_value, get_stat_series, get_stat_all

# Other utilities
from datacommons_pandas.utils import set_api_key
