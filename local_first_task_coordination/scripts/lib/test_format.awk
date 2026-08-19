# Formats libtest output into categorised, human-readable suites.
#
# Variables:
#   mode    "workspace" — input is a full `cargo test` run; suite membership is
#                         tracked from cargo's "Running ..." lines and a grand
#                         summary table is printed at the end.
#           "binary"    — input is one test binary's output; `binname` must be
#                         supplied and a single compact summary line is printed.
#   binname name of the test binary being formatted (binary mode only).
#
# Suites are keyed by "<crate>::<module>" for tests inside a module and by the
# binary name alone for integration tests, which sit at the crate root.

function prettify(s) { gsub(/_/, " ", s); return s }
function label_of(key) { return (key in L) ? L[key] : prettify(key) }

function record(name, result,   nparts, parts, modname, key, disp, mark) {
    if (index(name, "::")) {
        nparts = split(name, parts, "::")
        modname = (parts[1] == "tests") ? "" : parts[1]
        key = binname "::" modname
        disp = parts[nparts]
    } else {
        key = binname
        disp = name
    }

    if (!(key in seen)) { seen[key] = 1; order[++ncat] = key }

    if (result == "ok")           { mark = "  ok  "; pass[key]++; total_pass++ }
    else if (result == "ignored") { mark = " skip "; skip[key]++; total_skip++ }
    else                          { mark = " FAIL "; fail[key]++; total_fail++ }

    body[key] = body[key] sprintf("     [%s]  %s\n", mark, prettify(disp))
}

function render_suites(   i, key, label, p, f, s, n, dashes, pad, j) {
    for (i = 1; i <= ncat; i++) {
        key = order[i]; label = label_of(key)
        p = pass[key] + 0; f = fail[key] + 0; s = skip[key] + 0
        n = p + f + s

        dashes = ""; pad = WIDTH - length(label)
        for (j = 0; j < (pad > 0 ? pad : 1); j++) dashes = dashes "-"

        printf "\n  %s %s  %d/%d passed", label, dashes, p, n
        if (f > 0) printf ", %d FAILED", f
        if (s > 0) printf ", %d skipped", s
        printf "\n%s", body[key]
    }
}

function sort_suites(   i, j, lo, tmp) {
    for (i = 1; i <= ncat; i++) rank[order[i]] = (order[i] in R) ? R[order[i]] : 90 + i
    for (i = 1; i <= ncat; i++) {
        lo = i
        for (j = i + 1; j <= ncat; j++) if (rank[order[j]] < rank[order[lo]]) lo = j
        tmp = order[i]; order[i] = order[lo]; order[lo] = tmp
    }
}

BEGIN {
    # Readable name for each suite. Anything unlisted (a new module or test
    # file) still appears, labelled with a prettified version of its key.
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
    nfaildetail = 0; in_detail = 0; elapsed = ""
    WIDTH = 48
}

# "Running unittests src/lib.rs (target/debug/deps/crdt_core-0310d729b598c878)"
mode == "workspace" && /^[[:space:]]+Running / {
    binname = $NF
    gsub(/[()]/, "", binname)
    n = split(binname, path, "/")
    binname = path[n]
    sub(/-[0-9a-f]+$/, "", binname)
    in_detail = 0
    next
}

mode == "workspace" && /^[[:space:]]+Doc-tests / { binname = ""; next }

# "test causal_graph::tests::duplicate_insert_is_ignored ... ok"
/^test / && $2 != "result:" { record($2, $NF); next }

/^test result:/ { elapsed = $NF; sub(/s$/, "", elapsed); in_detail = 0; next }

# Panic output from failing tests, reprinted verbatim at the end.
/^---- .* ----$/ { in_detail = 1 }
in_detail        { faildetail[++nfaildetail] = $0 }

END {
    if (ncat == 0) exit                       # e.g. a binary with no tests

    sort_suites()

    bar = ""
    for (i = 0; i < 78; i++) bar = bar "="

    if (mode == "workspace") {
        printf "\n%s\n", bar
        printf "  TEST REPORT  --  local-first distributed task coordination\n"
        printf "%s\n", bar
    }

    render_suites()

    if (mode == "workspace") {
        printf "\n%s\n  SUMMARY\n%s\n", bar, bar
        printf "  %-*s | %6s | %6s | %7s\n", WIDTH, "suite", "passed", "failed", "skipped"
        sep = ""
        for (j = 0; j < WIDTH + 1; j++) sep = sep "-"
        printf "  %s+%s+%s+%s\n", sep, "--------", "--------", "---------"
        for (i = 1; i <= ncat; i++) {
            key = order[i]
            printf "  %-*s | %6d | %6d | %7d\n", WIDTH, label_of(key), pass[key]+0, fail[key]+0, skip[key]+0
        }
        printf "  %s+%s+%s+%s\n", sep, "--------", "--------", "---------"
        printf "  %-*s | %6d | %6d | %7d\n", WIDTH, "TOTAL", total_pass, total_fail, total_skip
    }

    if (nfaildetail > 0) {
        printf "\n  %s\n  FAILURE DETAIL\n  %s\n", "----------------------------", "----------------------------"
        for (i = 1; i <= nfaildetail; i++) print "  " faildetail[i]
    }

    printf "\n"
    if (mode == "workspace") {
        if (total_fail > 0)
            printf "  RESULT: %d of %d tests FAILED\n\n", total_fail, total_pass + total_fail + total_skip
        else
            printf "  RESULT: all %d tests passed\n\n", total_pass
    } else {
        printf "  %d passed, %d failed, %d skipped", total_pass, total_fail, total_skip
        if (elapsed != "") printf "  (%ss)", elapsed
        printf "\n"
    }
}
