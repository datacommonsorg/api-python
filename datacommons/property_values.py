# Copyright 2022 Google Inc.
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
""" API to request node information.
"""

from typing import List

from datacommons.requests import _post
from datacommons.utils import _get_direction


def property_values(nodes: List[str], property: str, out: bool = True):
    """Retrieves the property values for a list of nodes.
    Args:
        nodes: Node DCIDs.
        property: The property label to query for.
        out: Whether this is out going property.
    """
    resp = _post(f'/v1/bulk/property/values/{_get_direction(out)}', {
        'nodes': nodes,
        'property': property,
    })
    result = {}
    for item in resp.get('data', []):
        node, values = item['node'], item.get('values', [])
        result[node] = []
        for v in values:
            if 'dcid' in v:
                result[node].append(v['dcid'])
            else:
                result[node].append(v['value'])
    return result