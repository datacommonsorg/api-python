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

"""Base Types."""

import dataclasses
import time
from typing import Any, Protocol


DC = '__DC__'


@dataclasses.dataclass(frozen=True)
class Options:
  """Options for the Flow."""
  verbose: bool = True
  start_time: float = time.time()
  validate_dc_responses: bool = False

  def vlog(self, msg: str) -> None:
    if self.verbose:
      print(msg)

  def timeit(self, msg: str = '') -> None:
    e = time.time()
    d = e - self.start_time
    self.start_time = e
    if msg:
      print(f'{msg}: took {d:.2f}s')


@dataclasses.dataclass(frozen=True)
class LLMCall:
  prompt: str
  response: str
  duration_secs: float
  error: str | None = None

  def debug(self, i: int = 0) -> str:
    return (
        f'\n### Prompt {i} ###\n{self.prompt}\n'
        f'### Response {i} ###\n{self.response}\n'
        f'### LLM Duration {i} {self.duration_secs}s ###\n'
    )


@dataclasses.dataclass
class DCCall:
  """A single response from Data Commons."""

  id: int = 0
  query: str = ''

  # For point: val and date is set
  val: str = ''
  date: str = ''
  # For table: table is set
  table: str = ''

  unit: str = ''
  title: str = ''
  src: str = ''
  url: str = ''
  var: str = ''
  score: float = 0.0

  # The original LLM Value in case of RIG.
  llm_val: str = ''

  def footnote(self) -> str:
    return (
        f'Per {self.src}, value was {self.val}{self._dunit()} in {self.date}.'
        f' See more at {self.url}'
    )

  def debug(self) -> str:
    if not self.title:
      return ''
    if self.table:
      return self.answer()
    return (
        f'"{self.title}" was {self.val}{self._dunit()} in'
        f' {self.date} per {self.src} ({self.var}:{self.score})'
    )

  def answer(self) -> str:
    if self.table:
      return f'{self.header()}\n{self.table}'
    else:
      return (
          f'According to {self.src}, "{self.title}" was'
          f' {self.val}{self._dunit()} in {self.date}.'
      )

  def header(self) -> str:
    if self.table:
      if self.unit:
        header = f'{self.title} (unit: {self.unit})'
      else:
        header = f'{self.title}'
      return f'{header}, according to {self.src}'

    return self.title

  def val_and_unit(self) -> str:
    return f'{self.val}{self._dunit()}'

  def _dunit(self) -> str:
    return ' ' + self.unit if self.unit else ''


@dataclasses.dataclass(frozen=True)
class FlowResponse:
  """A response from Flow."""

  main_text: str = ''
  footnotes: str = ''
  tables_str: str = ''

  llm_calls: list[LLMCall] = dataclasses.field(default_factory=list)
  dc_calls: list[DCCall] = dataclasses.field(default_factory=list)
  dc_duration_secs: float = 0.0

  def duration_secs(self) -> float:
    return (
        sum([r.duration_secs for r in self.llm_calls]) + self.dc_duration_secs
    )

  def answer(self, include_aux: bool = True) -> str:
    """Returns a string representation of the response."""

    lines = []
    lines.append(self.main_text)

    if include_aux and self.footnotes:
      lines.append('\n#### FOOTNOTES ####')
      lines.append(self.footnotes)

    if include_aux and self.tables_str:
      lines.append('\n#### TABLES ####')
      lines.append(self.tables_str)

    return '\n'.join(lines)

  def debug(self) -> str:
    """Returns a string representation of the response."""

    lines = []
    lines.append('\n\n## LLM INTERACTIONS ##\n')
    for i, llm_response in enumerate(self.llm_calls):
      lines.append(llm_response.debug(i))

    lines.append('\n\n## DC INTERACTIONS ##\n')
    for dc_response in self.dc_calls:
      dbg = dc_response.debug()
      if dbg:
        lines.append(dbg)
    lines.append(f'\n\n## DC Duration {self.dc_duration_secs} ##')
    lines.append(f'\n\n## Total Duration {self.duration_secs()} ##')

    return '\n'.join(lines)

  def json(self) -> dict[str, Any]:
    return dataclasses.asdict(self)


class LLM(Protocol):

  def query(self, prompt: str) -> LLMCall:
    ...


class Flow(Protocol):

  # `prompt1`:
  #  - For RAG this is question generation prompt
  #  - For RIG this is used only if `in_context=True`
  # `prompt2`:
  #  - For RAG this is final-answer generation prompt
  #  - For RIG this is unused
  def query(
      self,
      query: str,
      in_context: bool = False,
      prompt1: str = '',
      prompt2: str = '',
  ) -> FlowResponse:
    ...
