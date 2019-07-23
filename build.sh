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
#
# This script runs all unit tests for the Data Commons python client.
#

echo "\n--------------------------------------------------------------------------------\nSYSTEM INFORMATION\n--------------------------------------------------------------------------------\n"

echo "Python binary: $(which python)" && python --version
echo "Bazel binary: $(which bazel)" && bazel version

echo "\n--------------------------------------------------------------------------------\nBUILDING DATA COMMONS\n--------------------------------------------------------------------------------\n"

# Build the code
time bazel build --incompatible_disable_deprecated_attr_params=false //... || exit 1

echo "\n--------------------------------------------------------------------------------\nTESTING DATA COMMONS\n--------------------------------------------------------------------------------\n"

# Change to repo root
cd datacommons

# Fail on any error.
set -e

# Test the code
time bazel test --test_output=errors --incompatible_disable_deprecated_attr_params=false //... || exit 1
