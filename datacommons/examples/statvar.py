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
""" Data Commons Python API examples.

Basic demo for get_stat_value.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys
sys.path.append('../')
sys.path.append('../../')
import datacommons as dc


def main():
    # Dcid for Santa Clara County.
    sc = 'geoId/06085'

    # Get population.
    print('Get Count_Person')
    print(dc.get_stat_value(sc, 'Count_Person'))

    # TODO(boxu/tjann): better error msgs starting from REST
    # e.g. stat/value?place=geoId/x06&stat_var=Count_Person&date=2010
    print('Get Count_Person Fail')
    print(dc.get_stat_value('bogus_id', 'Count_Person'))


if __name__ == '__main__':
    main()
