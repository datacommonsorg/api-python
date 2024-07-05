# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""LLM Interface."""

import json
import logging
import time
from typing import Any

import requests

from data_gemma import base


_REQ_DATA = {
    'contents': [{
        'parts': [{
            'text': '',
        }],
    }],
    'generationConfig': {
        'temperature': 0.1,
    },
    'safetySettings': [
        {'category': 'HARM_CATEGORY_HARASSMENT', 'threshold': 'BLOCK_NONE'},
        {'category': 'HARM_CATEGORY_HATE_SPEECH', 'threshold': 'BLOCK_NONE'},
        {
            'category': 'HARM_CATEGORY_SEXUALLY_EXPLICIT',
            'threshold': 'BLOCK_NONE',
        },
        {
            'category': 'HARM_CATEGORY_DANGEROUS_CONTENT',
            'threshold': 'BLOCK_NONE',
        },
    ],
}


class GoogleAIStudio(base.LLM):
  """Google AI Studio."""

  def __init__(
      self,
      model: str,
      session: requests.Session,
      keys: list[str],
      options: base.Options,
  ):
    self.keys = keys
    self.session: requests.Session = session
    self.next_key_idx = 0
    self.options = options
    self.model = model

  def query(self, prompt: str) -> base.LLMCall:
    req_data = _REQ_DATA.copy()

    # set the params.
    req_data['generationConfig']['temperature'] = 0.1
    req_data['contents'][0]['parts'][0]['text'] = prompt

    # Make API request.
    req = json.dumps(req_data)

    start = time.time()
    self.options.vlog(
        f'... calling AIStudio {self.model} "{prompt[:50].strip()}..."'
    )
    resp = _call_api(self.session, self.model, self._get_key(), req)
    t = round(time.time() - start, 3)
    ans = ''
    err = ''
    if (
        'candidates' in resp
        and resp['candidates']
        and 'content' in resp['candidates'][0]
        and 'parts' in resp['candidates'][0]['content']
        and resp['candidates'][0]['content']['parts']
        and 'text' in resp['candidates'][0]['content']['parts'][0]
    ):
      ans = resp['candidates'][0]['content']['parts'][0]['text']
    elif 'error' not in resp:
      err = 'Got empty response'
      logging.warning(err)
    else:
      err = json.dumps(resp)
      logging.error('%s', err)

    return base.LLMCall(prompt=prompt, response=ans, duration_secs=t, error=err)

  def _get_key(self):
    key = self.keys[self.next_key_idx]
    self.next_key_idx += 1
    if self.next_key_idx >= len(self.keys):
      self.next_key_idx = 0
    return key


_BASE_URL = 'https://generativelanguage.googleapis.com/v1beta/models'
_API_HEADER = {'content-type': 'application/json'}


def _call_api(
    session: requests.Session, model: str, key: str, req_data: str
) -> Any:
  r = session.post(
      f'{_BASE_URL}/{model}:generateContent?key={key}',
      data=req_data,
      headers=_API_HEADER,
  )
  return r.json()
