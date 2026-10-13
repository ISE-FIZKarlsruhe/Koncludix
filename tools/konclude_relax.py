#!/usr/bin/env python3
"""konclude_relax.py - write a relaxed copy of an ontology that is cheaper to reason about, and say exactly what may change.

Some constructs make a tableau reasoner guess (a choice at every node). Two of them are often not needed for the answers a user cares about:
  covers        DisjointUnion(W A B C ...)         every member of W is exactly one of the parts. Relaxed to: A, B, C are subclasses of W and are disjoint
                                                    (the "every member of W must be one of the parts" half is dropped)
  enumerations  EquivalentClasses(C ObjectOneOf(a b c ...))   C is the closed list {a, b, c}. Relaxed to: a, b, c are asserted members of C
                                                    (the "nothing else can be a member of C" half is dropped)
The relaxed ontology never says more than the original (it is weaker), so everything it entails is also entailed by the original;
what may be lost are conclusions that need the dropped halves. The report lists what was relaxed and what that can change.

usage: konclude_relax.py KONCLUDE INPUT_ONTOLOGY OUTPUT.ofn [--no-covers] [--no-enumerations] [--report FILE]
(any format that Konclude can read; the output is OWL functional syntax)"""
import argparse, os, re, subprocess, sys, tempfile

IRI = r"<[^<>\s]+>"
RE_DISJOINT_UNION = re.compile(r"^DisjointUnion\(((?:%s\s*)+)\)\s*$" % IRI)
RE_ENUM_A = re.compile(r"^EquivalentClasses\((%s) ObjectOneOf\(((?:%s\s*)+)\)\)\s*$" % (IRI, IRI))
RE_ENUM_B = re.compile(r"^EquivalentClasses\(ObjectOneOf\(((?:%s\s*)+)\) (%s)\)\s*$" % (IRI, IRI))
RE_CLASS_ASSERTION = re.compile(r"^ClassAssertion\((%s) (%s)\)\s*$" % (IRI, IRI))

WHAT_COVERS = ("The ontology no longer says that every member of the whole class is one of its parts. May change: (1) an individual that is a member of the whole class "
               "but of none of the parts is no longer an inconsistency, and (2) a conclusion that needs a case analysis over the parts (every member must be one part, "
               "and each part leads to the conclusion) is no longer drawn. Facts that are asserted, and conclusions that do not need the covering, are unchanged.")
WHAT_ENUM = ("The class is no longer closed. May change: (1) other individuals may now also be members of the class (the ontology no longer says 'only these'), "
             "(2) an individual outside the list that is asserted to be a member is no longer an inconsistency, and (3) a conclusion that needs 'every member is one of the listed "
             "individuals' (for example a case analysis over the list) is no longer drawn. The listed individuals are asserted to be members, so what follows from their membership is unchanged.")

def flatten(konclude, src, dst):
    env = dict(os.environ, KONCLUDE_SUPERVISED="1")
    r = subprocess.run([konclude, "explain", "-i", src, "--flatten", dst], capture_output=True, text=True, env=env, timeout=3600)
    if not os.path.exists(dst): sys.exit("Konclude could not read the ontology:\n" + (r.stdout + r.stderr)[-600:])

def main():
    ap = argparse.ArgumentParser(description="write a relaxed copy of an ontology and report what may change")
    ap.add_argument("konclude"); ap.add_argument("ontology"); ap.add_argument("output")
    ap.add_argument("--no-covers", action="store_true", help="keep DisjointUnion axioms as they are")
    ap.add_argument("--no-enumerations", action="store_true", help="keep closed lists (EquivalentClasses with ObjectOneOf) as they are")
    ap.add_argument("--report", help="also write the report to this file")
    a = ap.parse_args()
    work = tempfile.mkdtemp(prefix="relax_"); flat = os.path.join(work, "flat.ofn")
    flatten(a.konclude, a.ontology, flat)
    lines = open(flat, encoding="utf-8", errors="replace").read().splitlines()
    asserted = set()
    for ln in lines:
        m = RE_CLASS_ASSERTION.match(ln)
        if m: asserted.add((m.group(1), m.group(2)))
    out, covers, enums, added, other_enum = [], [], [], 0, 0
    for ln in lines:
        m = None if a.no_covers else RE_DISJOINT_UNION.match(ln)
        if m:
            parts = re.findall(IRI, m.group(1)); whole, kids = parts[0], parts[1:]
            covers.append((whole, kids))
            out += ["SubClassOf(%s %s)" % (k, whole) for k in kids] + ["DisjointClasses(%s)" % " ".join(kids)]
            continue
        m = None if a.no_enumerations else (RE_ENUM_A.match(ln) or RE_ENUM_B.match(ln))
        if m:
            cls, members = (m.group(1), re.findall(IRI, m.group(2))) if RE_ENUM_A.match(ln) else (m.group(2), re.findall(IRI, m.group(1)))
            enums.append((cls, members))
            for x in members:
                if (cls, x) not in asserted:
                    out.append("ClassAssertion(%s %s)" % (cls, x)); added += 1; asserted.add((cls, x))
            continue
        out.append(ln)
    other_enum = sum(1 for ln in out if "ObjectOneOf" in ln)
    open(a.output, "w", encoding="utf-8").write("\n".join(out) + "\n")
    short = lambda i: i.strip("<>").split("#")[-1].split("/")[-1]
    rep = ["Relaxed copy written to %s" % a.output, ""]
    if covers:
        rep += ["covers: %d DisjointUnion axiom(s) relaxed:" % len(covers)] + ["   %s = disjoint union of %d parts (%s%s)" % (short(w), len(k), ", ".join(short(x) for x in k[:4]), ", ..." if len(k) > 4 else "") for w, k in covers[:8]]
        rep += ["   what may change: " + WHAT_COVERS, ""]
    if enums:
        rep += ["enumerations: %d closed list(s) relaxed (%d membership assertion(s) added):" % (len(enums), added)] + ["   %s = one of %d individuals" % (short(c), len(m)) for c, m in enums[:8]]
        rep += ["   what may change: " + WHAT_ENUM, ""]
    if not covers and not enums: rep += ["nothing to relax: no DisjointUnion and no closed list found (or both switched off)"]
    if other_enum: rep += ["note: %d other axiom(s) use enumerations inside a larger expression; they were left unchanged." % other_enum]
    print("\n".join(rep))
    if a.report: open(a.report, "w", encoding="utf-8").write("\n".join(rep) + "\n")

if __name__ == "__main__":
    main()
