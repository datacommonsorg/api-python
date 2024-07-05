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

"""Utils."""

import csv
import os
import textwrap


def get_header(in_file):
  with open(in_file, 'r') as f:
    csvr = csv.reader(f)
    header = next(csvr)
    return header


def round_float(v: str) -> str:
  try:
    v = int(v)
    return str(v)
  except Exception:
    try:
      v = float(v)
      return str(round(v, 4))
    except Exception:
      return v


#
# Returns IDs from links_file that match the given statuses.
#
def get_matched_ids(
    links_file: str, statuses: set[str], id_col: str, status_col: str
) -> set[str]:
  if not links_file or not statuses:
    return set()
  matched_ids = set()
  with open(links_file, 'r') as f:
    for row in csv.DictReader(f):
      s = row.get(status_col, '')
      if s in statuses:
        matched_ids.add(row[id_col])
  return matched_ids


def load_csv(
    csv_file: str, id_column: str, aux_id_column: str = ''
) -> dict[str, dict[str, str]]:
  """Loads an ID keyed csv file."""

  csv.field_size_limit(1048576)
  results = {}
  if os.path.exists(csv_file):
    with open(csv_file, 'r') as f:
      results = {}
      for row in csv.DictReader(f):
        k = row[id_column].strip()
        if aux_id_column:
          k = f'{k}/{row[aux_id_column].strip()}'
        if k:
          results[k] = row
  return results


def checkpoint_csv(
    csv_file: str, key2row: dict[str, dict[str, str]], header: list[str]
) -> None:
  """Checkpoint an ID keyed csv file."""
  with open(csv_file, 'w', newline='') as f:
    csvw = csv.DictWriter(f, fieldnames=header)
    csvw.writeheader()
    csvw.writerows([key2row[k] for k in sorted(key2row.keys())])


def clean_rig_in_context_response(text: str) -> str:
  parts = text.split('Answer:-', 1)
  if len(parts) > 1:
    return parts[1].strip()
  return parts[0].strip()


def narrow_print(text: str) -> str:
  wrapper = textwrap.TextWrapper(
      width=80, break_long_words=False, break_on_hyphens=False
  )
  parts = []
  for line in text.split('\n'):
    if not line:
      parts.append('')
    else:
      parts.append(wrapper.fill(line))
  return '\n'.join(parts)
