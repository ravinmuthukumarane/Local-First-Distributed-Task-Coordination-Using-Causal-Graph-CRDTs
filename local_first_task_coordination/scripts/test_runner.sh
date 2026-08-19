#!/usr/bin/env bash
#
# Cargo `runner` wrapper: reformats libtest output so that a plain `cargo test`
# prints categorised, human-readable suites instead of a flat list of names.
# Wired up in .cargo/config.toml; cargo invokes it as `test_runner.sh <bin> [args]`.
#
# Anything that is not a test binary (e.g. `cargo run`) is executed untouched,
# as are test invocations whose output we must not rewrite (--list, --nocapture,
# custom --format). Set TEST_REPORT_RAW=1 to bypass formatting entirely.

set -uo pipefail

BIN="$1"; shift
HERE="$(cd "$(dirname "$0")" && pwd)"

run_plain() { exec "$BIN" "$@"; }

# Test binaries live in target/<...>/deps/<name>-<hash>; `cargo run` targets do not.
case "$BIN" in
    */deps/*-[0-9a-f][0-9a-f][0-9a-f][0-9a-f]*) ;;
    *) run_plain "$@" ;;
esac

[ "${TEST_REPORT_RAW:-0}" = "1" ] && run_plain "$@"

has_threads=0
for arg in "$@"; do
    case "$arg" in
        --list|--nocapture|--show-output|--format|--format=*|--bench|-Z*) run_plain "$@" ;;
        --test-threads|--test-threads=*) has_threads=1 ;;
    esac
done

# Deterministic ordering keeps suites contiguous; skip if the caller chose a value.
[ "$has_threads" -eq 0 ] && set -- "$@" --test-threads=1

OUTPUT=$("$BIN" "$@" 2>&1)
STATUS=$?

# Strip the trailing hash to recover the crate / integration-test file name.
binname=$(basename "$BIN")
binname=${binname%-*}

printf '%s\n' "$OUTPUT" | awk -v mode=binary -v binname="$binname" -f "$HERE/lib/test_format.awk"

exit $STATUS
