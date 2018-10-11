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

#!/bin/bash

# Fail on any error.
set -e

# Display commands to stderr.
set -x

PLATFORM=`uname | tr '[:upper:]' '[:lower:]'`
if [[ $PLATFORM != 'linux' ]]; then
  echo "This can run only on linux!"
  exit -1;
fi

echo "bazel binary: $(which bazel)"
bazel version

echo "python binary: $(which python)"
python --version

# Change to repo root
cd github/datacommons

# Build and test.
echo "Building..."
time bazel build //... || exit 1

echo "Testing..."
time bazel test //... || exit 1
