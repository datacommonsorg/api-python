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

"""RAG Flow."""

import logging
import time

from data_gemma import base
from data_gemma import dc
from data_gemma import prompts
from data_gemma import validate

_MAX_QUESTIONS = 25


class RAGFlow(base.Flow):
  """Retrieval Augmented Generation."""

  def __init__(
      self,
      v_llm_ques: base.LLM,
      v_llm_ans: base.LLM,
      ft_llm: base.LLM,
      datacommons: dc.DataCommons,
      options: base.Options,
      metrics_list: str = '',
  ):
    self.v_llm_ques = v_llm_ques
    self.v_llm_ans = v_llm_ans
    self.ft_llm = ft_llm
    self.dc = datacommons
    self.options = options
    self.metrics_list = metrics_list

  def query(
      self,
      query: str,
      in_context: bool = False,
      prompt1: str = '',
      prompt2: str = '',
  ) -> base.FlowResponse:
    assert in_context or self.ft_llm

    #
    # First call FT or V LLM model to get questions for Retrieval
    #
    if in_context:
      if self.metrics_list:
        prompt = prompt1 or prompts.RAG_IN_CONTEXT_PROMPT_WITH_VARS
        self.options.vlog(
            '... [RAG] Calling UNTUNED model for DC '
            'questions with all DC vars in prompt'
        )
        ques_resp = self.v_llm_ques.query(
            prompt.format(metrics_list=self.metrics_list, sentence=query)
        )
      else:
        prompt = prompt1 or prompts.RAG_IN_CONTEXT_PROMPT
        self.options.vlog('... [RAG] Calling UNTUNED model for DC questions')
        ques_resp = self.v_llm_ques.query(prompt.format(sentence=query))
    else:
      prompt = prompt1 or prompts.RAG_FINE_TUNED_PROMPT
      self.options.vlog('... [RAG] Calling FINETUNED model for DC questions')
      ques_resp = self.ft_llm.query(prompt.format(sentence=query))
    llm_calls = [ques_resp]
    if not ques_resp.response:
      return base.FlowResponse(llm_calls=llm_calls)

    questions = [q.strip() for q in ques_resp.response.split('\n') if q.strip()]
    questions = list(set(questions))[:_MAX_QUESTIONS]

    self.options.vlog('... [RAG] Making DC Calls')
    start = time.time()
    try:
      q2resp = self.dc.calln(questions, self.dc.table)
    except Exception as e:
      logging.warning(e)
      q2resp = {}
      pass
    dc_duration = time.time() - start

    if self.options.validate_dc_responses:
      q2resp = validate.run_validation(
          q2resp, self.v_llm_ans, self.options, llm_calls
      )

    table_parts: list[str] = []
    table_titles = set()
    dc_calls = []
    for resp in q2resp.values():
      tidx = len(dc_calls) + 1
      if resp.table and resp.title not in table_titles:
        table_parts.append(f'Table {tidx}: {resp.answer()}')
        table_titles.add(resp.title)
      resp.id = tidx
      dc_calls.append(resp)
    if table_parts:
      prompt = prompt2 or prompts.RAG_FINAL_ANSWER_PROMPT
      tables_str = '\n'.join(table_parts)
      final_prompt = prompt.format(sentence=query, table_str=tables_str)
    else:
      self.options.vlog('... [RAG] No stats found!')
      final_prompt = query
      tables_str = ''

    self.options.vlog('... [RAG] Calling UNTUNED model for final response')
    ans_resp = self.v_llm_ans.query(final_prompt)
    llm_calls.append(ans_resp)
    if not ans_resp.response:
      return base.FlowResponse(
          llm_calls=llm_calls, dc_duration_secs=dc_duration
      )

    return base.FlowResponse(
        main_text=ans_resp.response,
        tables_str=tables_str,
        llm_calls=llm_calls,
        dc_duration_secs=dc_duration,
        dc_calls=dc_calls,
    )
