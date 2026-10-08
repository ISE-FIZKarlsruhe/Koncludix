#!/usr/bin/env python3
"""Explanations for Konclude: a minimal set of axioms (a justification) that makes an ontology inconsistent or that entails a statement.

  tools/explain.py ontology.owl [--konclude ./Konclude]                         why is the ontology inconsistent?
  tools/explain.py ontology.owl --subclass "<http://ex#A>" "<http://ex#B>"       why is A a subclass of B?  (class expressions in functional syntax)
  tools/explain.py ontology.owl --type "<http://ex#i>" "<http://ex#C>"           why is individual i an instance of C?

Konclude has no explanation facility, so this tool uses Konclude as a black box: it removes axioms (delta debugging) as long as the ontology stays inconsistent
(for entailments: with the negated statement added) and prints what is left: removing any one of the printed axioms breaks the result (1-minimal justification).
Needs java (tools/openllet.jar provides the OWL API; used once to write the ontology as one axiom per line) and a Konclude binary.
"""
import argparse, os, subprocess, sys, tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sep = ";" if os.name == "nt" else ":"
ap = argparse.ArgumentParser()
ap.add_argument("ontology")
ap.add_argument("--konclude", default=os.path.join(HERE, "..", "Konclude"))
ap.add_argument("--subclass", nargs=2, metavar=("SUB", "SUPER"))
ap.add_argument("--type", nargs=2, metavar=("INDIVIDUAL", "CLASS"))
ap.add_argument("--timeout", type=int, default=60, help="seconds per Konclude call")
args = ap.parse_args()

tmp = tempfile.mkdtemp(prefix="konclude-explain-")
cp = sep.join([os.path.join(HERE, "openllet.jar"), HERE])
if not os.path.exists(os.path.join(HERE, "AboxReduce.class")):
    subprocess.run(["javac", "-cp", os.path.join(HERE, "openllet.jar"), "-d", HERE, os.path.join(HERE, "AboxReduce.java")], check=True)
flat = os.path.join(tmp, "flat.ofn")
subprocess.run(["java", "-cp", cp, "AboxReduce", "reduce", args.ontology, flat, os.path.join(tmp, "flat.map"), "noop"], check=True, capture_output=True)

lines = [l.rstrip("\n") for l in open(flat, encoding="utf-8")]
head = [l for l in lines if l.startswith("Prefix(") or l.startswith("Ontology(")]
decl = [l for l in lines if l.startswith("Declaration(")]
axioms = [l for l in lines if l.strip() and not l.startswith(("Prefix(", "Ontology(", "Declaration(", "#")) and l != ")"]
extra = []
if args.subclass:
    extra = ["ClassAssertion(%s <urn:konclude:explain#x>)" % args.subclass[0], "ClassAssertion(ObjectComplementOf(%s) <urn:konclude:explain#x>)" % args.subclass[1]]
elif args.type:
    extra = ["ClassAssertion(ObjectComplementOf(%s) %s)" % (args.type[1], args.type[0])]

calls = 0
def inconsistent(ax):
    global calls
    calls += 1
    f = os.path.join(tmp, "t.ofn")
    with open(f, "w", encoding="utf-8") as fh:
        fh.write("\n".join(head + decl + ax + extra + [")"]) + "\n")
    try:
        p = subprocess.run([args.konclude, "consistency", "-w", "1", "-i", f], capture_output=True, text=True, errors="replace", timeout=args.timeout)
    except subprocess.TimeoutExpired:
        return False
    return "is inconsistent" in p.stdout + p.stderr

what = "is inconsistent" if not extra else "entails the statement"
if not inconsistent(axioms):
    print("Nothing to explain: the ontology %s" % ("is not inconsistent (or Konclude did not answer in time)." if not extra else "does not entail the statement (or Konclude did not answer in time)."))
    sys.exit(1)

n = 2
while len(axioms) >= 2:
    chunk = max(1, len(axioms) // n)
    subsets = [axioms[i:i + chunk] for i in range(0, len(axioms), chunk)]
    reduced = False
    for i in range(len(subsets)):
        comp = [a for j, s in enumerate(subsets) if j != i for a in s]
        if comp and inconsistent(comp):
            axioms = comp; n = max(n - 1, 2); reduced = True; break
    if not reduced:
        if n >= len(axioms): break
        n = min(len(axioms), n * 2)
if len(axioms) == 1 and inconsistent([]):
    axioms = []
print("The ontology %s because of these %d axiom(s) (minimal; %d Konclude calls):" % (what, len(axioms), calls))
for a in axioms: print("  " + a)
if extra: print("(test axioms added for the check: %s)" % "; ".join(extra))
