#!/usr/bin/env python3
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
"""Fails when a benchmark run is worse than the run it is being compared with.

A recall or latency regression in the index is silent: the query still returns rows, and the
rows are still nearly right. Comparing two reports by eye is what has been done so far, which
means the comparison is skipped whenever there is no time for it. This reads a benchmark log --
what ``run-benchmark.sh`` writes -- and exits non-zero when a setting regressed by more than the
allowed amount, so a check can be put in front of a release or a change to the read path.

    ./vector-index-gate.py results/current.log --baseline results/previous.log
    ./vector-index-gate.py results/current.log --min-recall 0.95 --max-p50 20000

The two forms compose: a baseline says "no worse than it was", absolute thresholds say "good
enough at all". Both are checked when both are given.

Only settings that both runs measured are compared, because a setting one run skipped is not a
regression. A run whose log stops before its ``BENCHSUMMARY {"kind":"done"}`` line is refused
outright: a truncated run has fewer measurements, and fewer measurements read as an improvement.
"""

import argparse
import json
import statistics
import sys

RESULT = "BENCHRESULT"
SUMMARY = "BENCHSUMMARY"


def parse(path):
    """Returns the results of a log, keyed by (over, index), and whether the run finished."""
    results = {}
    finished = False
    with open(path, encoding="utf-8") as log:
        for line in log:
            line = line.strip()
            if not line:
                continue

            kind, _, payload = line.partition(" ")
            if kind == SUMMARY:
                summary = json.loads(payload)
                finished |= summary.get("kind") == "done"
            elif kind == RESULT:
                # a run measures one index unless it is given several arms; the label is what
                # run-benchmark.sh writes, and an unlabelled run is one arm
                measurement = json.loads(payload)
                key = (measurement["over"], measurement.get("arm", ""))
                results.setdefault(key, []).append(measurement)

    return results, finished


def summarize(measurements):
    """The recall to compare and how slow the setting was, taking the median over repeats."""
    recalls = [m["recall"] for m in measurements]
    latencies = [m["ms"] for m in measurements]
    return {
        "recall": statistics.median(recalls),
        "p50": statistics.median(latencies),
        "n": len(measurements),
    }


def compare(current, baseline, max_recall_drop, max_latency_increase):
    """The settings that got worse, as lines ready to be printed."""
    failures = []
    for key in sorted(set(current) & set(baseline)):
        now = summarize(current[key])
        was = summarize(baseline[key])
        over, arm = key
        where = "over=%s%s" % (over, (" arm=" + arm) if arm else "")

        drop = was["recall"] - now["recall"]
        if drop > max_recall_drop:
            failures.append(
                "%s: recall fell from %.4f to %.4f (-%.4f, allowed -%.4f)"
                % (where, was["recall"], now["recall"], drop, max_recall_drop)
            )

        if was["p50"] > 0:
            increase = (now["p50"] - was["p50"]) / float(was["p50"])
            if increase > max_latency_increase:
                failures.append(
                    "%s: median latency rose from %d ms to %d ms (+%.1f%%, allowed +%.1f%%)"
                    % (where, was["p50"], now["p50"], increase * 100, max_latency_increase * 100)
                )

    return failures


def check_thresholds(current, min_recall, max_p50):
    """The settings that miss an absolute threshold, as lines ready to be printed."""
    failures = []
    for (over, arm), measurements in sorted(current.items()):
        now = summarize(measurements)
        where = "over=%s%s" % (over, (" arm=" + arm) if arm else "")
        if min_recall is not None and now["recall"] < min_recall:
            failures.append(
                "%s: recall is %.4f, below the required %.4f"
                % (where, now["recall"], min_recall)
            )

        if max_p50 is not None and now["p50"] > max_p50:
            failures.append(
                "%s: median latency is %d ms, above the allowed %d ms"
                % (where, now["p50"], max_p50)
            )

    return failures


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("current", help="the benchmark log to check")
    parser.add_argument("--baseline", help="an earlier log to compare against")
    parser.add_argument(
        "--max-recall-drop",
        type=float,
        default=0.02,
        help="how much recall may fall against the baseline (default 0.02)",
    )
    parser.add_argument(
        "--max-latency-increase",
        type=float,
        default=0.20,
        help="how much median latency may rise against the baseline (default 0.20)",
    )
    parser.add_argument("--min-recall", type=float, help="every setting must reach this recall")
    parser.add_argument("--max-p50", type=int, help="no setting may have a median above this")
    args = parser.parse_args()

    current, finished = parse(args.current)
    if not finished:
        # a cut-short run has fewer measurements, and fewer measurements read as an improvement
        sys.exit("%s did not finish: its log has no BENCHSUMMARY done line" % args.current)

    if not current:
        sys.exit("%s has no %s lines" % (args.current, RESULT))

    failures = []
    if args.baseline:
        baseline, baseline_finished = parse(args.baseline)
        if not baseline_finished:
            sys.exit("%s did not finish, so it cannot be a baseline" % args.baseline)

        shared = set(current) & set(baseline)
        if not shared:
            sys.exit(
                "%s and %s measured no setting in common, so nothing was compared"
                % (args.current, args.baseline)
            )

        failures += compare(current, baseline, args.max_recall_drop, args.max_latency_increase)

    failures += check_thresholds(current, args.min_recall, args.max_p50)

    if failures:
        print("%s regressed:" % args.current, file=sys.stderr)
        for failure in failures:
            print("  " + failure, file=sys.stderr)

        return 1

    print(
        "%s: %d settings within the allowed change%s"
        % (
            args.current,
            len(current),
            (" against %s" % args.baseline) if args.baseline else "",
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
