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

"""RIG Flow."""

import copy
import logging
import re
import time

from data_gemma import base
from data_gemma import dc
from data_gemma import prompts
from data_gemma import validate


_DC_PATTERN = r'\[__DC__\("([^"]+)"\) --> "([^"]*)"\]?'

# 5% threshold
_DIFF_THRESHOLD = 0.05


class RIGFlow(base.Flow):
  """Retrieval Interleaved Answering."""

  def __init__(
      self,
      v_llm: base.LLM,
      ft_llm: base.LLM,
      datacommons: dc.DataCommons,
      options: base.Options,
  ):
    self.v_llm = v_llm
    self.ft_llm = ft_llm
    self.dc = datacommons
    self.options = options

  def query(
      self,
      query: str,
      in_context: bool = False,
      prompt1: str = '',
      prompt2: str = '',
  ) -> base.FlowResponse:
    assert in_context or self.ft_llm

    if in_context:
      self.options.vlog('... [RIG] Calling UNTUNED Model')
      prompt = prompt1 or prompts.RIG_IN_CONTEXT_PROMPT
      llm_resp = self.v_llm.query(prompt.format(sentence=query))
    else:
      self.options.vlog('... [RIG] Calling FINETUNED Model')
      llm_resp = self.ft_llm.query(query)
    if not llm_resp.response:
      logging.error('FAILED: %s', query)
      return base.FlowResponse(llm_calls=[llm_resp])

    # Make DC calls.
    llm_text = llm_resp.response
    q2llmval, q2resp, dc_duration = self._call_dc(llm_text)
    llm_calls = [llm_resp]

    # Sanity check DC call and response using LLM, and keep only the "good"
    # ones.
    if self.options.validate_dc_responses:
      q2resp = validate.run_validation(
          q2resp, self.v_llm, self.options, llm_calls
      )

    self.options.vlog('... [RIG] Calling DC Evaluate')
    llm_text, footnotes, dc_calls = self._evaluate(llm_text, q2llmval, q2resp)

    return base.FlowResponse(
        main_text=llm_text,
        footnotes='\n'.join(footnotes),
        llm_calls=llm_calls,
        dc_duration_secs=dc_duration,
        dc_calls=dc_calls,
    )

  def _call_dc(
      self, llm_text: str
  ) -> tuple[dict[str, list[str]], dict[str, base.DCCall], float]:
    """Calls DC."""

    start = time.time()

    q2llmval: dict[str, list[str]] = {}
    for match in re.findall(_DC_PATTERN, llm_text):
      q2llmval.setdefault(match[0], []).append(match[1])

    try:
      q2resp = self.dc.calln(list(q2llmval.keys()), self.dc.point)
    except Exception as e:
      logging.warning(e)
      q2resp = {}
      pass

    return q2llmval, q2resp, time.time() - start

  def _evaluate(
      self,
      text: str,
      q2llmval: dict[str, list[str]],
      q2resp: dict[str, base.DCCall],
  ) -> tuple[str, list[str], list[base.DCCall]]:
    """Evaluates a text contained DC Calls."""

    def _rtag(txt: str, r: base.DCCall) -> str:
      return f'[{base.DC}#{r.id}({txt})]'

    dc_calls = []
    footnote_map = {}
    for q, orig_resp in q2resp.items():
      llm_vals = q2llmval[q]

      for llmval in llm_vals:
        resp = copy.deepcopy(orig_resp)

        resp.id = len(dc_calls) + 1
        resp.llm_val = llmval
        dcval = resp.val_and_unit()

        idx = -1
        if dcval:
          idx = len(footnote_map) + 1
          if q not in footnote_map:
            footnote_map[q] = (idx, f'[{idx}] - {resp.footnote()}')
          else:
            idx = footnote_map[q][0]

        orig = f'[__DC__("{q}") --> "{llmval}"]'
        if not llmval:
          # If LLM answer was empty!
          if dcval:
            new = f'{dcval} [{idx}] ||'
          else:
            new = '--- || ---'
          text = text.replace(orig, _rtag(new, resp), 1)
        elif dcval:
          if _flag_value(resp.val, llmval):
            new = f'{dcval} [{idx}]* || {llmval}'
          else:
            new = f'{dcval} [{idx}] || {llmval}'
          text = text.replace(orig, _rtag(new, resp), 1)
        else:
          new = f'|| {llmval}'
          text = text.replace(orig, _rtag(new, resp), 1)

        dc_calls.append(resp)

    footnotes = [
        v[1] for v in sorted(footnote_map.values(), key=lambda x: x[0])
    ]

    return text, footnotes, dc_calls


def _clean_float(text: str) -> float:
  return float(re.sub(r'[^0-9.]', '', text))


def _flag_value(dcv: str, llmv: str) -> bool:
  """Compares dc and llm values and flags beyond a threshold."""
  try:
    for t, v in [
        (' million', 1000000),
        (' billion', 1000000000),
        (' trillion', 1000000000000),
    ]:
      if t in llmv:
        llmv = str(_clean_float(llmv.replace(' million', '')) * v)
        break
    llmv = _clean_float(llmv)
    dcv = float(dcv)
    pct_diff = ((dcv - llmv) / llmv) if llmv != 0 else 1.0
  except:
    return False
  return pct_diff > _DIFF_THRESHOLD or pct_diff < -_DIFF_THRESHOLD
