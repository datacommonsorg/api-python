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

"""Validation Flow."""

import logging

from data_gemma import base
from data_gemma import prompts


def run_validation(
    q2resp: dict[str, base.DCCall],
    llm: base.LLM,
    options: base.Options,
    llm_calls: list[base.LLMCall],
) -> dict[str, base.DCCall]:
  """Runs DC QA validation."""
  queries, input_text = _dc_qa_validation_input(
      {q: r.title for q, r in q2resp.items()}
  )
  if queries:
    llm_resp2 = llm.query(prompts.DC_QA_VALIDATION.format(input=input_text))
    options.vlog(f'... [Validate] {input_text}\n{llm_resp2.response}')
    if not llm_resp2.response:
      logging.error('FAILED: %s', input_text)
      queries = []
    else:
      llm_calls.append(llm_resp2)
      try:
        onum = len(queries)
        queries = _dc_qa_validation_check(llm_resp2.response, queries)
        if len(queries) < onum:
          options.vlog(
              f'... [Validate] Dropped answers: {onum} --> {len(queries)}'
          )
      except:
        logging.error('FAILED: %s', llm_resp2.response)
        queries = []
  else:
    options.vlog('... [Validate] empty queries!')

  queries = set(queries)
  q2resp = {
      q: r if q in queries else base.DCCall(query=q) for q, r in q2resp.items()
  }
  return q2resp


def _dc_qa_validation_input(q2a: dict[str, str]) -> tuple[list[str], str]:
  """Returns a list of questions and a prompt for DC QA validation."""
  parts = []
  queries = []
  i = 1
  for q, a in q2a.items():
    if not a.strip():
      continue
    queries.append(q)
    parts.append(f'[[QA{i}]]:\n  Question: {q}\n  Answer: {a}')
    i += 1
  return queries, '\n'.join(parts)


def _dc_qa_validation_check(llm_resp: str, queries: list[str]) -> list[str]:
  """Checks the DC QA validation response."""
  out_queries = []
  for qaid in llm_resp.strip().split('\n'):
    if qaid.startswith('[[QA') and qaid.endswith(']]'):
      idx = int(qaid.replace('[[QA', '').replace(']]', '')) - 1
      out_queries.append(queries[idx])
  return out_queries
