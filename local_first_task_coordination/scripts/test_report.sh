#!/usr/bin/env bash
#
# Human-readable, categorised test report for the workspace.
#
# Runs `cargo test` across all crates and regroups the flat libtest output into
# labelled suites (CRDT core, task model, simulation), one line per test, with a
# per-suite and overall summary table.
#
# Usage:  ./scripts/test_report.sh [extra cargo test args...]
#         ./scripts/test_report.sh causal_graph      # filter to matching tests

set -uo pipefail
cd "$(dirname "$0")/.."

RAW=$(cargo test --workspace --no-fail-fast --color never "$@" -- --test-threads=1 2>&1)
CARGO_STATUS=$?

# A build failure produces no test results at all — show cargo's own output.
if ! printf '%s\n' "$RAW" | grep -q '^test result:'; then
    printf '%s\n' "$RAW"
    exit $CARGO_STATUS
fi

printf '%s\n' "$RAW" | awk '
function prettify(s) { gsub(/_/, " ", s); return s }

BEGIN {
    # Suite labels, keyed by "<crate>::<module>" for unit tests and by the
    # integration test file stem for tests/*.rs. Unlisted keys fall back to a
    # prettified version of the key itself.
    L["crdt_core::"]              = "[crdt_core]  Identifiers & events (unit)"
    L["crdt_core::causal_graph"]  = "[crdt_core]  Causal graph insert & merge (unit)"
    L["crdt_core::message"]       = "[crdt_core]  Sync messages (unit)"
    L["causal_graph_integration"] = "[crdt_core]  Causal graph integration"
    L["merge_properties"]         = "[crdt_core]  CRDT merge laws (integration)"
    L["task_model::"]             = "[task_model] State derivation & conflicts (unit)"
    L["semantic_validation"]      = "[task_model] Semantic validation (integration)"
    L["node_sim::node"]           = "[node_sim]   Node behaviour (unit)"
    L["node_sim::simulation"]     = "[node_sim]   Simulation engine (unit)"
    L["node_sim::scenarios"]      = "[node_sim]   Experiment scenarios (unit)"

    # Report order: primitives first, then the model built on them, then the
    # simulation that exercises both. Unknown suites keep discovery order.
    R["crdt_core::"] = 1; R["crdt_core::causal_graph"] = 2; R["crdt_core::message"] = 3
    R["causal_graph_integration"] = 4; R["merge_properties"] = 5
    R["task_model::"] = 6; R["semantic_validation"] = 7
    R["node_sim::node"] = 8; R["node_sim::simulation"] = 9; R["node_sim::scenarios"] = 10

    ncat = 0; total_pass = 0; total_fail = 0; total_skip = 0
    nfaildetail = 0; in_detail = 0
    WIDTH = 48
}

# "Running unittests src/lib.rs (target/debug/deps/crdt_core-0310d729b598c878)"
# "Running tests/merge_properties.rs (target/debug/deps/merge_properties-c338...)"
/^[[:space:]]+Running / {
    bin = $NF
    gsub(/[()]/, "", bin)
    n = split(bin, path, "/")
    bin = path[n]
    sub(/-[0-9a-f]+$/, "", bin)
    crate = bin
    is_unit = ($0 ~ /Running unittests/)
    in_detail = 0
    next
}

/^[[:space:]]+Doc-tests / { crate = ""; next }   # no doc-tests in this workspace

# "test causal_graph::tests::duplicate_insert_is_ignored ... ok"
/^test / && $2 != "result:" {
    name = $2
    result = $NF

    if (is_unit && index(name, "::")) {
        nparts = split(name, parts, "::")
        mod = (parts[1] == "tests") ? "" : parts[1]
        key = crate "::" mod
        disp = parts[nparts]
    } else {
        key = crate
        disp = name
    }

    if (!(key in seen)) { seen[key] = 1; order[++ncat] = key }

    if (result == "ok")           { mark = "  ok  "; pass[key]++; total_pass++ }
    else if (result == "ignored") { mark = " skip "; skip[key]++; total_skip++ }
    else                          { mark = " FAIL "; fail[key]++; total_fail++ }

    body[key] = body[key] sprintf("     [%s]  %s\n", mark, prettify(disp))
    next
}

# Failure output blocks, reprinted verbatim at the bottom.
/^---- .* ----$/          { in_detail = 1 }
/^test result:/           { in_detail = 0 }
in_detail                 { faildetail[++nfaildetail] = $0 }

END {
    # Selection sort of the discovered suites by their configured rank.
    for (i = 1; i <= ncat; i++) rank[order[i]] = (order[i] in R) ? R[order[i]] : 90 + i
    for (i = 1; i <= ncat; i++) {
        lo = i
        for (j = i + 1; j <= ncat; j++) if (rank[order[j]] < rank[order[lo]]) lo = j
        tmp = order[i]; order[i] = order[lo]; order[lo] = tmp
    }

    bar = ""
    for (i = 0; i < 78; i++) bar = bar "="

    printf "\n%s\n", bar
    printf "  TEST REPORT  --  local-first distributed task coordination\n"
    printf "%s\n", bar

    for (i = 1; i <= ncat; i++) {
        key = order[i]
        label = (key in L) ? L[key] : prettify(key)
        p = pass[key] + 0; f = fail[key] + 0; s = skip[key] + 0
        n = p + f + s

        dashes = ""
        pad = WIDTH - length(label)
        for (j = 0; j < (pad > 0 ? pad : 1); j++) dashes = dashes "-"

        printf "\n  %s %s  %d/%d passed", label, dashes, p, n
        if (f > 0) printf ", %d FAILED", f
        if (s > 0) printf ", %d skipped", s
        printf "\n%s", body[key]
    }

    printf "\n%s\n", bar
    printf "  SUMMARY\n"
    printf "%s\n", bar
    printf "  %-*s | %6s | %6s | %7s\n", WIDTH, "suite", "passed", "failed", "skipped"
    sep = ""
    for (j = 0; j < WIDTH + 1; j++) sep = sep "-"
    printf "  %s+%s+%s+%s\n", sep, "--------", "--------", "---------"

    for (i = 1; i <= ncat; i++) {
        key = order[i]
        label = (key in L) ? L[key] : prettify(key)
        printf "  %-*s | %6d | %6d | %7d\n", WIDTH, label, pass[key]+0, fail[key]+0, skip[key]+0
    }
    printf "  %s+%s+%s+%s\n", sep, "--------", "--------", "---------"
    printf "  %-*s | %6d | %6d | %7d\n", WIDTH, "TOTAL", total_pass, total_fail, total_skip

    if (nfaildetail > 0) {
        printf "\n%s\n  FAILURE DETAIL\n%s\n", bar, bar
        for (i = 1; i <= nfaildetail; i++) print "  " faildetail[i]
    }

    printf "\n"
    if (total_fail > 0)
        printf "  RESULT: %d of %d tests FAILED\n\n", total_fail, total_pass + total_fail + total_skip
    else
        printf "  RESULT: all %d tests passed\n\n", total_pass
}
'

exit $CARGO_STATUS
