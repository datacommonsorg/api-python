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

"""Basic Flow."""

from data_gemma import base


class BaselineFlow(base.Flow):
  """Baseline Flow."""

  def __init__(
      self,
      ut_llm: base.LLM,
      options: base.Options,
  ):
    self.ut_llm = ut_llm
    self.options = options

  def query(
      self,
      query: str,
      in_context: bool = False,
      prompt1: str = '',
      prompt2: str = '',
  ) -> base.FlowResponse:
    self.options.vlog('... [DEFAULT] Calling BASE model')
    resp = self.ut_llm.query(query)
    return base.FlowResponse(
        main_text=resp.response,
        llm_calls=[resp],
        dc_duration_secs=0,
        dc_calls=[],
    )
