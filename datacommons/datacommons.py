# Copyright 2017 Google Inc.
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

"""Data Commons Public API.
"""

from collections import defaultdict
import ._auth
import pandas as pd


_CLIENT_ID = ('381568890662-ff9evnle0lj0oqttr67p2h6882d9ensr'
              '.apps.googleusercontent.com')
_CLIENT_SECRET = '77HJA4S5m48Z98UKkW_o-jAY'
_API_ROOT = 'https://datcom-api-sandbox.appspot.com'


class Client(object):
  """Provides Data Commons API."""

  def __init__(self,
               client_id=_CLIENT_ID,
               client_secret=_CLIENT_SECRET,
               api_root=_API_ROOT):
    self._service = _auth.do_auth(client_id, client_secret, api_root)
    self._inited = True

  def Query(self, datalog_query):
    """Performs a query returns results as a table.

    Args:
      datalog_query: string representing datalog query in [TODO(shanth): link]

    Returns:
      A pandas.DataFrame with the selected variables in the query as the
      the column names and each cell containing a list of values.

    Raises:
      RuntimeError: some problem with executing query (hint in the string)
    """
    assert self._inited, 'Initialization was unsuccessful, cannot execute Query'

    try:
      response = self._service.query(body={'query': datalog_query}).execute()
    except Exception as e:  # pylint: disable=broad-except
      raise RuntimeError('Failed to execute query: {}'.format(e))

    header = response.get('header', [])
    rows = response.get('rows', [])

    result_dict = defaultdict(list)
    for row in rows:
      cells = row.get('cells', [])

      if len(cells) != len(header):
        raise RuntimeError('Response #cells mismatches #header: {}'
                           .format(response))

      for key, cell in zip(header, cells):
        try:
          result_dict[key].append(cell['value'])
        except KeyError:
          raise RuntimeError('No value in cell: {}'.format(row))

    return pd.DataFrame(result_dict)[header]
