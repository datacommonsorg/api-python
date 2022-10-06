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


def triples(nodes: List[str], out: bool = True):
    """Retrieves the triples for a node.
    Args:
        node: Node DCID.
        out: Whether the returned property is out-going for the queried nodes.
    """
    resp = _post(f'/v1/bulk/triples/{_get_direction(out)}',
                 data={'nodes': nodes})
    result = {}
    for item in resp.get('data', []):
        node, triples = item['node'], item.get('triples', {})
        result[node] = {}
        for property, other_nodes in triples.items():
            result[node][property] = other_nodes.get('nodes', [])
    return result