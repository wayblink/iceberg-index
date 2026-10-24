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

# Reads what the index cache actually covered on the executors, for the arm that was just measured.
#
# The number this prints is what decides whether a derived budget is worth anything: a budget that
# covers 7 of 13 files per executor leaves the cache evicting what it just loaded, and the latency
# a benchmark reports is then the latency of not caching. It is read after the run rather than
# during it, because asking the executors costs a job of its own.
#
#   ./measure-coverage.sh <base-table> [arm-name]

set -euo pipefail

BASE=${1:?base table, for example hms.sparkann.sift100m_lance_zstd}
ARM=${2:-}

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
OUT=${OUT:-coverage-$(date +%Y%m%d-%H%M%S)-$(basename "$BASE")}

echo ">>> measuring cache coverage of $BASE${ARM:+ arm $ARM}"
IMG=${IMG:-hub.i.basemind.com/spark/sparkann-e2e:20261021b} \
  "$HERE/run-benchmark.sh" "$OUT" "$BASE" "$BASE" 10 2 2 1 100 "${ARM:--}" coverage \
  || true

# the run above is the cheapest query shape there is: it is not a measurement, it is what makes the
# executors load the arm's files so that the status call has something to report. What matters is
# the last line it prints.
LOG="$HERE/results/$OUT.log"
if [ -f "$LOG" ]; then
  grep -h "COVERAGE" "$LOG" | tail -5 || echo "no COVERAGE line in $LOG" >&2
else
  echo "no log at $LOG" >&2
  exit 1
fi
