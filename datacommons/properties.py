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


def properties(nodes: List[str], out: bool = True):
    """Retrieves all the properties for a list of nodes.
    Args:
        nodes: Node DCIDs.
        out: Whether this is out going property.
    """
    resp = _post(f'/v1/bulk/properties/{_get_direction(out)}', {
        'nodes': nodes,
    })
    result = {}
    for item in resp.get('data', []):
        node, properties = item['node'], item.get('properties', [])
        result[node] = properties
    return result