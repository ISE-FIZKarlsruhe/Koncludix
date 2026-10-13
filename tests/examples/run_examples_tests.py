#!/usr/bin/env python3
"""Tests of tools/konclude_examples.py.   usage: run_examples_tests.py PATH_TO_KONCLUDE"""
import json, os, re, subprocess, sys, tempfile
HERE = os.path.dirname(os.path.abspath(__file__))
TOOL = os.path.join(HERE, "..", "..", "tools", "konclude_examples.py")
K = sys.argv[1]
ONT = os.path.join(HERE, "university.ofn")
results = []
def check(name, ok, info=""):
    results.append(ok); print("%s  %s" % ("PASS" if ok else "FAIL", name))
    if not ok: print("      " + str(info)[:1500])

def run(extra, ont=ONT):
    td = tempfile.mkdtemp(); js = os.path.join(td, "e.json"); md = os.path.join(td, "e.md")
    r = subprocess.run([sys.executable, TOOL, K, ont, "--json", js, "--out", md] + extra, capture_output=True, text=True, timeout=900)
    ex = json.load(open(js)) if os.path.exists(js) else None
    return ex, r.stdout + r.stderr, (open(md).read() if os.path.exists(md) else "")

ex, out, md = run(["--min-axioms", "3", "--top", "10"])
conc = {(e["kind"], e["x"].split("#")[-1], e["y"].split("#")[-1]): e["n"] for e in ex} if ex else {}
want = {("type", "anna", "BusyPerson"): 9, ("type", "cs", "ProjectGroup"): 5, ("type", "faculty", "ProjectGroup"): 4, ("type", "bob", "Teacher"): 3}
check("default run: the four expected examples with the expected number of axioms (9, 5, 4, 3)", conc == want, conc)
check("ranked by score: the 9-axiom example comes first", ex and ex[0]["x"].endswith("anna") and [e["score"] for e in ex] == sorted([e["score"] for e in ex], reverse=True), [(e["x"], e["score"]) for e in ex or []])
check("plain subclass chains are left out by default (GradStudent is a Person)", not any(e["kind"] == "subclass" for e in ex or []), conc)
ex2, out2, _ = run(["--min-axioms", "2", "--include-chains", "--kind", "subclasses"])
check("with --include-chains the chain GradStudent < Student < Person appears (2 axioms)", ex2 and any(e["x"].endswith("GradStudent") and e["y"].endswith("Person") and e["n"] == 2 for e in ex2), [(e["x"], e["y"], e["n"]) for e in ex2 or []])
exb, outb, mdb = run(["--classes", "busy person", "--min-axioms", "2"])
check("--classes with a label: only conclusions about that class (busy person)", exb and len(exb) == 1 and exb[0]["y"].endswith("BusyPerson"), [(e["x"], e["y"]) for e in exb or []])
ext, outt, _ = run(["--classes", "Teacher", "--min-axioms", "2"])
check("--classes with a local name: only Teacher examples (bob with 3 axioms, carl with 2)", ext and sorted((e["x"].split("#")[-1], e["n"]) for e in ext) == [("bob", 3), ("carl", 2)] and all(e["y"].endswith("Teacher") for e in ext), [(e["x"], e["y"], e["n"]) for e in ext or []])
exn, outn, _ = run(["--classes", "NoSuchClass"])
check("--classes with an unknown name: a warning and no examples", "WARNING: no class matches" in outn and exn == [], outn[-400:])
check("labels are used in the text (is a member of, graduate student, busy person)", all(w in md for w in ("graduate student", "is a member of", "A busy person is defined as")), md[:600])
check("the English text names the conclusion in words", "anna is a busy person." in md and "bob is a teacher." in md, md[:300])

exd, outd, _ = run(["--classes", "Department", "--min-axioms", "3"])
exi, outi, _ = run(["--classes", "Department", "--involving", "--min-axioms", "3", "--max-explain", "20"])
check("--classes without --involving: nothing concludes something about Department; with --involving the project-group examples appear (Department is in their justification)",
      exd == [] and exi and {e["y"].split("#")[-1] for e in exi} == {"ProjectGroup"}, ([(e["x"], e["y"]) for e in exd or []], [(e["x"], e["y"]) for e in exi or []]))

# independent check: the justification alone reproduces the conclusion, and every axiom of it is needed (explain on the justification gives the same set)
def justification_is_tight(e):
    td = tempfile.mkdtemp(); f = os.path.join(td, "j.ofn")
    axs = list(e["axioms"])
    open(f, "w").write("Ontology(<http://j>\n" + "\n".join(axs) + "\n)\n")
    args = ["--type", "<%s>" % e["x"], "<%s>" % e["y"]] if e["kind"] == "type" else ["--subclass", "<%s>" % e["x"], "<%s>" % e["y"]]
    r = subprocess.run([K, "explain", "-i", f] + args, capture_output=True, text=True, env=dict(os.environ, KONCLUDE_SUPERVISED="1"), timeout=300)
    m = re.search(r"because of these (\d+) axiom", r.stdout + r.stderr)
    return bool(m) and int(m.group(1)) == len(axs)
check("every justification is tight: on its own it still entails the conclusion and no axiom can be dropped", ex and all(justification_is_tight(e) for e in ex), "")

# an ontology without anything interesting
td = tempfile.mkdtemp(); small = os.path.join(td, "s.ofn")
open(small, "w").write("Prefix(:=<http://s#>)\nOntology(<http://s>\nDeclaration(Class(:A)) Declaration(Class(:B))\nSubClassOf(:A :B)\nClassAssertion(:A :x)\n)\n")
exs, outs, _ = run(["--min-axioms", "3"], ont=small)
check("an ontology with only plain chains gives no examples and no error", exs == [] and "Traceback" not in outs, outs[-400:])
print("\n%d of %d checks passed" % (sum(results), len(results)))
sys.exit(0 if all(results) else 1)
