#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Checks that the quality gate passes, fails and refuses what it is meant to, using the fixture
# logs beside this script. It needs no cluster: a gate that only runs against a live benchmark is a
# gate nobody runs.

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
gate="$here/../vector-index-gate.py"
fixtures="$here/fixtures"

failures=0

expect() {
  local want="$1" name="$2"
  shift 2
  local got=0
  python3 "$gate" "$@" >/dev/null 2>&1 || got=$?
  if [[ "$got" != "$want" ]]; then
    echo "FAIL: $name exited $got, expected $want" >&2
    failures=$((failures + 1))
  else
    echo "ok: $name"
  fi
}

# a run that matches its baseline is not a regression
expect 0 "same run" "$fixtures/gate-same.log" --baseline "$fixtures/gate-baseline.log"

# ... and one where recall fell is
expect 1 "worse recall" "$fixtures/gate-worse-recall.log" --baseline "$fixtures/gate-baseline.log"

# ... as is one where the median rose past what the noise explains
expect 1 "worse latency" "$fixtures/gate-worse-latency.log" --baseline "$fixtures/gate-baseline.log"

# a cut-short run has fewer measurements, and fewer measurements read as an improvement
expect 1 "truncated run" "$fixtures/gate-truncated.log" --baseline "$fixtures/gate-baseline.log"

# an absolute bar works without an earlier run
expect 1 "min recall" "$fixtures/gate-baseline.log" --min-recall 0.95
expect 0 "min recall met" "$fixtures/gate-baseline.log" --min-recall 0.85

if [[ "$failures" != 0 ]]; then
  echo "$failures gate check(s) failed" >&2
  exit 1
fi

echo "all gate checks passed"
