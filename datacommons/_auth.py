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
"""Helper for authenticating with datacommons.org API service.

Based on example in:
  https://cloud.google.com/endpoints/docs/frameworks/python/access_from_python
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

from googleapiclient import discovery
import httplib2
from oauth2client import client as authcli
from oauth2client import file as authfile
from oauth2client import tools as authtools

API_NAME = 'datacommons'
VERSION = 'v0.1'
SCOPE = 'https://www.googleapis.com/auth/userinfo.email'
USER_AGENT = 'datacommons_python_client/0.1'
OAUTH_DISPLAY_NAME = 'DataCommons Python Client'


def do_auth(client_id, client_secret, api_root):
  """Authenticates with DataCommons API service.

  Invoking this requires user interaction (to authenticate and feed in code).

  Args:
    client_id: oauth2 client ID
    client_secret: auth2 client secret
    api_root: URL of the service

  Returns:
    A service connections that can be used to issue calls on.
  """

  # Acquire and store oauth token.
  storage = authfile.Storage('datacommons_{}.dat'.format(client_id))
  credentials = storage.get()

  if credentials is None or credentials.invalid:
    flow = authcli.OAuth2WebServerFlow(
        client_id=client_id,
        client_secret=client_secret,
        scope=SCOPE,
        user_agent=USER_AGENT,
        oauth_displayname=OAUTH_DISPLAY_NAME,
        redirect_uri='urn:ietf:wg:oauth:2.0:oob')
    oauth_flags = authtools.argparser.parse_args(args=[])
    oauth_flags.noauth_local_webserver = True
    credentials = authtools.run_flow(flow, storage, oauth_flags)

  http = httplib2.Http()
  http = credentials.authorize(http)

  # Build a service object for interacting with the API.
  discovery_url = '{}/api/discovery/v1/apis/{}/{}/rest'.format(
      api_root, API_NAME, VERSION)
  logging.info('Calling %s', discovery_url)
  return discovery.build(
      API_NAME,
      VERSION,
      discoveryServiceUrl=discovery_url,
      http=http,
      cache_discovery=False)
