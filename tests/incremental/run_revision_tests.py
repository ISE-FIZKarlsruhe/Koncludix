#!/usr/bin/env python3
"""Tests of OWLlink revisions (Tell / Retract): after an update the answer must be the same as for a fresh load of the same data.
usage: run_revision_tests.py PATH_TO_KONCLUDE [CONFIG.xml]      (the optional OWLlink Set file is given to every run, for example experiments/incremental/incr_cfg_on.xml for IncrementalRebuild)

Background: when a revision was rebuilt, new individuals got IDs that were already used by old individuals and overwrote them in the individual vector,
so persons disappeared from instance lists after a Tell (the types of the lost persons were still right). Fixed in CConcreteOntologyUpdateBuilder::buildIndividualIDs."""
import os, re, subprocess, sys, tempfile
K = sys.argv[1]
CONFIG = sys.argv[2] if len(sys.argv) > 2 else ""
NS = "http://hobby#"; KB = "http://hobby/kb"
env = dict(os.environ, KONCLUDE_SUPERVISED="1")
cls = lambda n: '<owl:Class IRI="%s%s"/>' % (NS, n)
ind = lambda n: '<owl:NamedIndividual IRI="%s%s"/>' % (NS, n)
HEAD = '<?xml version="1.0" encoding="UTF-8"?>\n<RequestMessage xmlns="http://www.owllink.org/owllink-xml#" xmlns:owl="http://www.w3.org/2002/07/owl#">\n'

def axioms(persons, hobbies=None):
    ax = []
    for name, k in persons:
        ax.append("<owl:ClassAssertion>%s%s</owl:ClassAssertion>" % (cls("Person"), ind(name)))
        for j in range(k):
            ax.append('<owl:ObjectPropertyAssertion><owl:ObjectProperty IRI="%shasHobby"/>%s%s</owl:ObjectPropertyAssertion>' % (NS, ind(name), ind("hh%d" % (j % 8 + 1))))
    return "\n".join(ax)

def tbox(variant):
    t = []
    if variant != "none":
        t.append('<owl:EquivalentClasses>%s<owl:ObjectOneOf>%s</owl:ObjectOneOf></owl:EquivalentClasses>' % (cls("Hobby"), "".join(ind("hh%d" % i) for i in range(1, 9))))
        t += ["<owl:ClassAssertion>%s%s</owl:ClassAssertion>" % (cls("Hobby"), ind("hh%d" % i)) for i in range(1, 9)]
    if variant == "mincard":
        t.append('<owl:SubClassOf>%s<owl:ObjectMinCardinality cardinality="3"><owl:ObjectProperty IRI="%shasHobby"/>%s</owl:ObjectMinCardinality></owl:SubClassOf>' % (cls("Person"), NS, cls("Hobby")))
    return "\n".join(t)

Q = '<GetFlattenedInstances kb="%s">%s</GetFlattenedInstances>' % (KB, cls("Person"))
def run(req_body, tag):
    d = tempfile.mkdtemp(); rq, rs = os.path.join(d, "q.xml"), os.path.join(d, "r.xml")
    open(rq, "w", newline="\n").write(HEAD + '<CreateKB kb="%s"/>\n' % KB + req_body + "</RequestMessage>\n")
    r = subprocess.run([K, "owllinkfile", "-w", "1"] + (["-c", CONFIG] if CONFIG else []) + ["-i", rq, "-o", rs], capture_output=True, text=True, env=env, timeout=300)
    t = open(rs).read() if os.path.exists(rs) else ""
    return [set(re.findall(r'#(p\d+)"', s)) for s in re.findall(r'<SetOfIndividuals.*?</SetOfIndividuals>', t, re.S)]

def tell(ax): return '<Tell kb="%s">\n%s\n</Tell>\n' % (KB, ax)
def retract(ax): return '<Retract kb="%s">\n%s\n</Retract>\n' % (KB, ax)

results = []
def check(name, ok, info=""):
    results.append(ok); print("%s  %s" % ("PASS" if ok else "FAIL", name))
    if not ok: print("      " + str(info)[:800])

import random
rnd = random.Random(3)
for variant in ("none", "oneof", "mincard"):
    for n_base, n_delta in ((1, 1), (8, 2), (20, 5), (100, 10), (500, 5)):
        base = [("p%d" % i, rnd.choice((0, 1, 2, 3, 4))) for i in range(n_base)]
        delta = [("p%d" % (n_base + i), rnd.choice((0, 1, 2, 3, 4))) for i in range(n_delta)]
        # one request: tell base, ask, tell delta, ask, retract delta, ask   against fresh loads of the three states
        upd = run(tell(tbox(variant) + "\n" + axioms(base)) + Q + tell(axioms(delta)) + Q + retract(axioms(delta)) + Q, "upd")
        fresh_base = run(tell(tbox(variant) + "\n" + axioms(base)) + Q, "fb")
        fresh_all = run(tell(tbox(variant) + "\n" + axioms(base + delta)) + Q, "fa")
        want = [fresh_base[0], fresh_all[0], fresh_base[0]] if fresh_base and fresh_all else None
        ok = len(upd) == 3 and want is not None and upd == want
        info = ""
        if not ok and len(upd) == 3 and want: info = "after the Tell lost %s, after the Retract lost %s" % (sorted(want[1] - upd[1])[:5], sorted(want[2] - upd[2])[:5])
        check("%-7s base %3d + %2d: the instances after Tell and after Retract equal a fresh load" % (variant, n_base, n_delta), ok, info or (len(upd), [len(u) for u in upd]))
# two updates in a row, and a Tell that changes the TBox as well
base = [("p%d" % i, 2) for i in range(10)]; d1 = [("p%d" % (10 + i), 1) for i in range(3)]; d2 = [("p%d" % (13 + i), 4) for i in range(4)]
upd = run(tell(tbox("oneof") + "\n" + axioms(base)) + Q + tell(axioms(d1)) + Q + tell(axioms(d2)) + Q, "multi")
fresh = [run(tell(tbox("oneof") + "\n" + axioms(base + x)) + Q, "f")[0] for x in ([], d1, d1 + d2)]
check("three revisions in a row (two Tells): every answer equals a fresh load", len(upd) == 3 and upd == fresh, [len(u) for u in upd])
# a fact that was retracted and is told again must come back (with IncrementalRebuild on the second Tell used to be ignored)
base = [("p%d" % i, 3) for i in range(6)]; delta = [("p%d" % (6 + i), 3) for i in range(3)]
upd = run(tell(tbox("mincard") + "\n" + axioms(base + delta)) + Q + retract(axioms(delta)) + Q + tell(axioms(delta)) + Q, "retell")
fa = run(tell(tbox("mincard") + "\n" + axioms(base + delta)) + Q, "fa")[0]; fb = run(tell(tbox("mincard") + "\n" + axioms(base)) + Q, "fb")[0]
check("Tell, Retract, then Tell the same facts again: the instances come back (equal a fresh load each time)", len(upd) == 3 and upd == [fa, fb, fa], [len(u) for u in upd])
# a Retract followed by a Tell with no question in between (two revisions that are built together)
base = [("p%d" % i, 2) for i in range(6)]; gone = [base[1], base[2]]; new = [("p%d" % (6 + i), 2) for i in range(2)]
upd = run(tell(tbox("oneof") + "\n" + axioms(base)) + Q + retract(axioms(gone)) + tell(axioms(new)) + Q, "rt")
keep = [x for x in base if x not in gone]
fresh = [run(tell(tbox("oneof") + "\n" + axioms(x)) + Q, "f")[0] for x in (base, keep + new)]
check("Retract and then Tell without a question in between: the answer equals a fresh load", len(upd) == 2 and upd == fresh, [len(u) for u in upd])
print("\n%d of %d checks passed" % (sum(results), len(results)))
sys.exit(0 if all(results) else 1)
