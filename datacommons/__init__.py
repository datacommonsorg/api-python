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

################################## IMPORTANT #################################
# All user-facing functions in this package must be symlinked to the         #
# datacommons_pandas pkg. This is so that users do not need to import both   #
# libraries for pd support. Please keep the below imports in sync with the   #
# __init__.py in the datacommons_pandas/ dir, and add a symlink when         #
# creating a new file.                                                       #
# TODO: https://github.com/datacommonsorg/api-python/issues/149              #
##############################################################################

# Data Commons Python API
from datacommons.core import get_property_labels
from datacommons.core import get_property_values
from datacommons.core import get_triples
from datacommons.key import set_api_key
from datacommons.node import properties
from datacommons.node import property_values
from datacommons.node import triples
from datacommons.places import get_places_in
from datacommons.places import get_related_places
from datacommons.places import get_stats
# Data Commons SPARQL query support
from datacommons.sparql import query
from datacommons.stat_vars import get_stat_all
from datacommons.stat_vars import get_stat_series
from datacommons.stat_vars import get_stat_value
