#!/usr/bin/env bash
#
# Full-workspace test report: every suite plus a grand summary table.
#
# `cargo test` on its own is already formatted per test binary (see
# .cargo/config.toml and scripts/test_runner.sh); this script runs the whole
# workspace in raw mode and formats it as one report, so the totals across all
# crates appear in a single table.
#
# Usage:  ./scripts/test_report.sh [extra cargo test args...]
#         ./scripts/test_report.sh causal_graph      # filter to matching tests

set -uo pipefail
cd "$(dirname "$0")/.."

RAW=$(TEST_REPORT_RAW=1 cargo test --workspace --no-fail-fast --color never "$@" -- --test-threads=1 2>&1)
CARGO_STATUS=$?

# A build failure produces no test results at all — show cargo's own output.
if ! printf '%s\n' "$RAW" | grep -q '^test result:'; then
    printf '%s\n' "$RAW"
    exit $CARGO_STATUS
fi

printf '%s\n' "$RAW" | awk -v mode=workspace -f "$(dirname "$0")/lib/test_format.awk"

exit $CARGO_STATUS
