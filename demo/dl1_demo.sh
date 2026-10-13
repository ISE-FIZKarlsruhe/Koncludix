#!/bin/bash
# Demo of the Konclude features on the OWL2Bench DL-1 ontology (50 thousand axioms, 3,677 individuals). Linux / WSL.
#   usage:  bash demo/dl1_demo.sh [PATH_TO_KONCLUDE]
#   quick:  QUICK=1 bash demo/dl1_demo.sh        (skips the longest steps; about 5 minutes instead of about 12)
#   file:   DL1=/path/to/OWL2DL-1_Functional.owl bash demo/dl1_demo.sh     (OWL functional syntax or OWL/XML, from the OWL2Bench data)
set -u
HERE="$(cd "$(dirname "$0")/.." && pwd)"
K="${1:-${KONCLUDE_BIN:-$HOME/kon_all2/Release/Konclude}}"
DL1="${DL1:-/mnt/d/downloads/OWL2DL-1_Functional.owl}"
NS="https://kracr.iiitd.edu.in/OWL2Bench#"
W="$(mktemp -d /tmp/dl1demo.XXXX)"
export KONCLUDE_SUPERVISED=1
[ -f "$DL1" ] || { echo "OWL2Bench DL-1 not found at $DL1: set DL1=..."; exit 1; }
ln -s "$DL1" "$W/DL1.ofn" 2>/dev/null; DL1="$W/DL1.ofn"      # the .ofn name tells every tool that this is OWL functional syntax
[ -x "$K" ] || { echo "No Konclude found at $K: give the path of the build as first argument"; exit 1; }
step() { echo; echo "=============================================================================="; echo "$1"; echo "=============================================================================="; }
quiet() { grep -a -v "^{info}\|^{error}"; }

step "1. lint: what will be expensive or is not fully handled (reads the file, does not reason)"
$K lint -i "$DL1" 2>&1 | quiet | head -30

step "2. consistency: is the ontology consistent? (about 6 s)"
$K consistency -w 1 -i "$DL1" 2>&1 | grep -a "consistent"

step "3. classification: the class tree (about 13 s)"
$K classification -w 1 -i "$DL1" -o "$W/hierarchy.xml" 2>&1 | grep -a "Finished class classification"
echo "class tree written to $W/hierarchy.xml ($(wc -c < "$W/hierarchy.xml") bytes)"

step "4. explain: we add ONE wrong fact (a man is also asserted to be a woman; Man and Woman are disjoint)"
python3 - "$DL1" "$W/DL1_with_error.ofn" <<'PY'
import sys
t = open(sys.argv[1], encoding="utf-8", errors="replace").read()
i = t.rindex(")")
open(sys.argv[2], "w", encoding="utf-8").write(t[:i] + "ClassAssertion(owl2bench:Woman owl2bench:U0C0D0AP0)\n)\n")
PY
$K consistency -w 1 -i "$W/DL1_with_error.ofn" 2>&1 | grep -a "consistent"
echo "-- explain: the smallest set of axioms that makes it inconsistent (about 11 s):"
$K explain -i "$W/DL1_with_error.ofn" 2>&1 | quiet | sed 's#https://kracr.iiitd.edu.in/OWL2Bench\##:#g'

step "5. explain: why does U0C0D0AP0 count as a person with many hobbies? (about 50 s)"
echo "(PeopleWithManyHobbies is defined as: a Person who likes at least 3 Interests)"
$K explain -i "$DL1" --type "<${NS}U0C0D0AP0>" "<${NS}PeopleWithManyHobbies>" 2>&1 | quiet | sed 's#https://kracr.iiitd.edu.in/OWL2Bench\##:#g' | cut -c1-170 | head -14

step "6. materialize: write facts that follow; you choose which kinds"
cat > "$W/types-only.xml" <<'XML'
<?xml version="1.0" encoding="UTF-8"?>
<RequestMessage xmlns="http://www.owllink.org/owllink-xml#">
  <Set key="Konclude.CLI.Materialize.ObjectPropertyAssertions"><Literal>false</Literal></Set>
  <Set key="Konclude.CLI.Materialize.DataPropertyAssertions"><Literal>false</Literal></Set>
  <Set key="Konclude.CLI.Materialize.SameIndividuals"><Literal>false</Literal></Set>
  <Set key="Konclude.CLI.Materialize.ClassHierarchy"><Literal>false</Literal></Set>
  <Set key="Konclude.CLI.Materialize.PropertyHierarchy"><Literal>false</Literal></Set>
</RequestMessage>
XML
cat > "$W/no-objprops.xml" <<'XML'
<?xml version="1.0" encoding="UTF-8"?>
<RequestMessage xmlns="http://www.owllink.org/owllink-xml#">
  <Set key="Konclude.CLI.Materialize.ObjectPropertyAssertions"><Literal>false</Literal></Set>
</RequestMessage>
XML
s=$(date +%s); $K materialize -w 1 -c "$W/types-only.xml" -i "$DL1" -o "$W/types.nt" >/dev/null 2>&1
echo "types of individuals only:            $(( $(date +%s) - s )) s, $(wc -l < "$W/types.nt") facts"
s=$(date +%s); $K materialize -w 1 -c "$W/no-objprops.xml" -i "$DL1" -o "$W/noobj.nt" >/dev/null 2>&1
echo "everything except object properties:  $(( $(date +%s) - s )) s, $(wc -l < "$W/noobj.nt") facts"
if [ -z "${QUICK:-}" ]; then
  s=$(date +%s); $K materialize -w 1 -i "$DL1" -o "$W/all.nt" >/dev/null 2>&1
  echo "everything:                           $(( $(date +%s) - s )) s, $(wc -l < "$W/all.nt") facts"
  echo "facts that mention PeopleWithManyHobbies: $(grep -c PeopleWithManyHobbies "$W/all.nt")"
else echo "(everything: skipped in QUICK mode)"; fi

step "7. diagnose: where do the time and the memory go?"
python3 "$HERE/tools/konclude_diagnose.py" "$K" "$DL1" --task consistency -w 1 --timeout 120 2>&1 | head -40

if [ -z "${QUICK:-}" ]; then
  step "8. examples finder: multi-step reasoning examples about PeopleWithManyHobbies, in plain English (a few minutes)"
  python3 "$HERE/tools/konclude_examples.py" "$K" "$DL1" --classes "PeopleWithManyHobbies" --max-explain 3 --top 2 --timeout 150 2>&1 | head -40
else echo; echo "(step 8, examples finder: skipped in QUICK mode)"; fi

echo; echo "Files written: $W"
