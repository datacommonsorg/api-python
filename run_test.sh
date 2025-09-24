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

set -e  # Immediately exit with failure if any command fails.

YAPF_STYLE='{based_on_style: google, indent_width: 2}'
FORMAT_INCLUDE_PATHS="datacommons/ datacommons_client/ datacommons_pandas/"
FORMAT_EXCLUDE_PATH="**/.env/**"

function setup_python {
  python3 -m pip install --upgrade pip hatch
  # here temporarily while there is an incompatibility with hatch and the newest click version
  # see https://github.com/pypa/hatch/pull/2051 for status updates from Hatch
  python3 -m pip uninstall uninstall click -y
  python3 -m pip install click==8.2.1
  hatch env create
}

function run_py_test {
  pytest -vv
}

function run_yapf {
  EXTRA_ARGS=$@
  yapf $EXTRA_ARGS --recursive --parallel --style="$YAPF_STYLE" \
    --exclude="$FORMAT_EXCLUDE_PATH" $FORMAT_INCLUDE_PATHS
}

function run_isort {
  EXTRA_ARGS=$@
  isort $EXTRA_ARGS --profile=google --skip-glob="$FORMAT_EXCLUDE_PATH" \
    $FORMAT_INCLUDE_PATHS
}

function run_lint_test {
  if ! run_yapf --diff; then
    echo "Fix lint errors by running: ./run_test.sh -f"
    exit 1
  fi
  if ! run_isort --check-only; then
    echo "Fix Python import sort orders by running ./run_test.sh -f"
    exit 1
  fi
  echo "Python style checks passed."
}

function run_lint_fix {
  run_yapf --in-place
  run_isort
}

function run_all_tests {
  run_py_test
  run_lint_test
}

function help {
  echo "Usage: $0 -asplf"
  echo "-a       Run all tests"
  echo "-s       Set up python environment"
  echo "-p       Run python tests"
  echo "-l       Run lint tests"
  echo "-f       Fix lint"
  exit 1
}

while getopts asplf OPTION; do
  case $OPTION in
    a)
        echo -e "### Running all tests"
        run_all_tests
        ;;
    s)
        echo -e "### Setting up python environment"
        setup_python
        ;;
    p)
        echo -e "### Running python tests"
        run_py_test
        ;;
    l)
        echo -e "### Running lint tests"
        run_lint_test
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
