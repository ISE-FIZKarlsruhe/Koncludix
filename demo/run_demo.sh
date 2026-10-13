#!/bin/bash
# Meeting demo: every result of the temporal work, one after the other. Linux / WSL.
#   usage:  bash demo/run_demo.sh [PATH_TO_KONCLUDE]            (default: Release/Konclude, then ~/kon_all2/Release/Konclude)
#   quick:  QUICK=1 bash demo/run_demo.sh                       (skips the two longest runs)
set -u
HERE="$(cd "$(dirname "$0")/.." && pwd)"
K="${1:-${KONCLUDE_BIN:-}}"
# the binary must be a build of this repository (it has the `interval` command)
if [ -z "$K" ]; then
  for c in "$HOME/kon_all2/Release/Konclude" "$HERE/Release/Konclude" "$HERE/Binaries/Konclude"; do
    if [ -x "$c" ] && "$c" --help 2>&1 | grep -q "interval"; then K="$c"; break; fi
  done
fi
[ -x "$K" ] && "$K" --help 2>&1 | grep -q "interval" || { echo "No Konclude build with the 'interval' command found: give the path of a build of this repository as the first argument"; exit 1; }
export KONCLUDE_BIN="$K"
step() { echo; echo "=============================================================================="; echo "$1"; echo "=============================================================================="; }

step "1. Time conflicts inside the reasoner: a cycle of 'before' (OWL axioms alone cannot see it)"
KONCLUDE_SUPERVISED=1 "$K" consistency -i "$HERE/tests/interval/02_cycle.ttl" 2>&1 | grep -v "^{info}" | head -12

step "2. Why? Konclude explain: the smallest set of facts behind a conflict between a claim and the time stamps"
KONCLUDE_SUPERVISED=1 "$K" explain -i "$HERE/tests/interval/10_functional_syntax.ofn" 2>&1 | grep -v "^{info}" | head -12

step "3. Rules over time stamps: no Allen relation in the file, Konclude derives them; the process-order rule is violated in sample 2"
KONCLUDE_SUPERVISED=1 "$K" explain -i "$HERE/tests/interval/11_rules_over_derived_relations.ofn" 2>&1 | grep -v "^{info}" | head -16

step "4. The temporal layer: facts with validity times, rules about time (always, sometime, until, deadlines)"
cd "$HERE/tests/temporal"
python3 ../../tools/temporal/tlayer.py --tbox hr_tbox.ofn --facts hr_facts.tl --timeline --validation --now 2025-12-31 --show problems --explain \
  --formula "G(Employee(?x) -> F[0,200] Manager(?x))" --why | head -70
cd "$HERE"

step "4b. A temporal definition: a class derived from the past goes back into the reasoner (derived FormerEmployee -> Alumnus, disjoint from Student)"
cd "$HERE/tests/temporal"
python3 ../../tools/temporal/tlayer.py --tbox hr_tbox_defs.ofn --facts hr_def_facts.tl --validation --explain \
  --define "FormerEmployee(?x) := O Employee(?x) & !Employee(?x)" --formula "F Alumnus(Bob)" | head -30
cd "$HERE"

step "4c. What takes the time and memory? The diagnosis tool on a small hobby ontology (a choice among 8 enumerated hobbies for every person)"
python3 "$HERE/tests/diagnose/make_hobby.py" /tmp/hobby_demo.ofn 1500 8
python3 "$HERE/tools/konclude_diagnose.py" "$K" /tmp/hobby_demo.ofn --task materialize --timeout 120 2>&1 | sed -n '/^DIAGNOSIS/,/Slowest by time/p' | head -30

step "4d. Nice examples of reasoning: the examples finder on a small university ontology (conclusions that need several axioms, in plain English)"
python3 "$HERE/tools/konclude_examples.py" "$K" "$HERE/tests/examples/university.ofn" --min-axioms 3 --top 3 2>&1 | grep -v '^   `' | sed -n '/^# Examples/,$p' | head -34

step "5. Real PMD data (Creep Testing Ontology): the order links point the wrong way (needs the workshop folder)"
PMD="${PMD_DATA:-/mnt/d/Github/PMDco-Reasoning-Workshop/part3-reasoners/data/cto_small.ttl}"
if [ -f "$PMD" ]; then python3 "$HERE/experiments/pmd_creep/pmd_order_check.py" "$K" "$PMD" /tmp/pmd_demo 2>&1 | grep -v "{info}" ; else echo "(skipped: set PMD_DATA to cto_small.ttl)"; fi

step "6. Tests: interval module (24 checks) and temporal layer (20 checks)"
python3 "$HERE/tests/interval/run_interval_tests.py" "$K" --random 50 2>&1 | tail -4
python3 "$HERE/tests/temporal/run_temporal_tests.py" "$K" --random 6 2>&1 | tail -16

if [ -z "${QUICK:-}" ]; then
  step "7. A campaign of 300 made-up samples with injected violations, checked against an independent day-by-day truth (about 3 minutes)"
  python3 "$HERE/experiments/temporal/pmd_like_eval.py" "$K" 300 7 2>&1 | tail -12
fi
echo; echo "Done. Reading: docs/simple-guide.md, docs/temporal-layer.md, docs/pmd-real-data.md"
