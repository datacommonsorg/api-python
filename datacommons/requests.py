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
""" Send http requests to Data Commons REST API endpoints.
"""

import requests
from typing import Dict

import datacommons.key as key

# REST API endpoint root
_API_ROOT = "https://api.datacommons.org"


def _post(path: str, data={}) -> Dict:
    url = _API_ROOT + path
    headers = {'Content-Type': 'application/json'}
    api_key = key.get_api_key()
    if api_key:
        headers['x-api-key'] = api_key
    try:
        resp = requests.post(url, json=data, headers=headers)
        if resp.status_code != 200:
            raise Exception(
                f'{resp.status_code}: {resp.reason}\n{resp.json()["message"]}')
        return resp.json()
    except requests.exceptions.Timeout:
        raise Exception('data request timeout')
    except requests.exceptions.RequestException as e:
        raise e