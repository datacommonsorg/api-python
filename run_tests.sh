#!/bin/bash
# Copyright 2020 Google LLC
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

function setup_python {
  python3 -m venv .env
  source .env/bin/activate
  pip install --upgrade pip
  pip3 install -r requirements.txt -q
}

function run_py_test {
  setup_python
  python3 -m pytest -vv
  echo -e "#### Checking Python style"
  if ! yapf --recursive --diff --style=google -p datacommons/ datacommons_pandas/; then
    echo "Fix lint errors by running: ./run_test.sh -f"
    exit 1
  fi
}

function run_lint_fix {
  python3 -m venv .env
  source .env/bin/activate
  if ! command -v yapf &> /dev/null
  then
    pip3 install yapf -q
  fi
  yapf -r -i -p --style=google datacommons/ datacommons_pandas/
  deactivate
}

function help {
  echo "Usage: $0 -pf"
  echo "-p       Run python tests"
  echo "-f       Fix lint"
  exit 1
}

# Always reset the variable null.
while getopts tpwotblcsaf OPTION; do
  case $OPTION in
    p)
        echo -e "### Running python tests"
        run_py_test
        ;;
    f)
        echo -e "### Fix lint errors"
        run_lint_fix
        ;;
    *)
        help
    esac
done

if [ $OPTIND -eq 1 ]
then
  help
fi