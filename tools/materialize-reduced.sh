#!/usr/bin/env bash
# Optional ABox reduction for `materialize`: individuals that are indistinguishable (same asserted types, data shape and neighbour classes)
# are represented by one individual, the reduced ontology is materialized and the result is expanded to all individuals again.
# Useful when the ABox contains many individuals of the same kind (generated / repetitive data); gives no gain for densely, randomly linked data.
#
#   tools/materialize-reduced.sh input.owl output.nt [path/to/Konclude]
#
# Needs java (the bundled tools/openllet.jar provides the OWL API). Input: any OWL syntax the OWL API reads (RDF/XML, Turtle, OWL/XML, functional).
# Output: N-Triples. Individuals mentioned in the TBox, in sameAs/differentFrom axioms, in keys, or at roles with cardinality, functional,
# disjointness, transitivity or role-chain axioms are never merged, so the result equals the full materialization (checked on synthetic and fuzzed ontologies).
set -euo pipefail
if [ $# -lt 2 ]; then sed -n 2,12p "$0"; exit 1; fi
IN="$1"; OUT="$2"
HERE="$(cd "$(dirname "$0")" && pwd)"
KONCLUDE="${3:-$HERE/../Konclude}"
CP="$HERE/openllet.jar:$HERE"
[ -f "$HERE/AboxReduce.class" ] || javac -cp "$HERE/openllet.jar" -d "$HERE" "$HERE/AboxReduce.java"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
java -cp "$CP" AboxReduce reduce "$IN" "$TMP/reduced.ofn" "$TMP/reduced.map"
"$KONCLUDE" materialize -w 1 -i "$TMP/reduced.ofn" -o "$TMP/reduced.nt" > "$TMP/konclude.log" 2>&1 || true
if grep -q "is inconsistent" "$TMP/konclude.log" || [ ! -s "$TMP/reduced.nt" ]; then
  echo "The ontology is inconsistent (or Konclude failed); see the log below." >&2
  tail -5 "$TMP/konclude.log" >&2
  exit 2
fi
java -cp "$CP" AboxReduce expand "$IN" "$TMP/reduced.map" "$TMP/reduced.nt" "$OUT"
echo "materialized (with ABox reduction) -> $OUT"
