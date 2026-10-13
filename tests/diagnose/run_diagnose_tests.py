#!/usr/bin/env python3
"""Tests of the profiler and of tools/konclude_diagnose.py.   usage: run_diagnose_tests.py PATH_TO_KONCLUDE"""
import json, os, subprocess, sys, tempfile
HERE = os.path.dirname(os.path.abspath(__file__))
TOOL = os.path.join(HERE, "..", "..", "tools", "konclude_diagnose.py")
K = sys.argv[1]
results = []
def check(name, ok, info=""):
    results.append(ok); print("%s  %s" % ("PASS" if ok else "FAIL", name))
    if not ok: print("      " + str(info)[:1200])

def diagnose(ont, extra, js):
    r = subprocess.run([sys.executable, TOOL, K, ont, "--json", js] + extra, capture_output=True, text=True, timeout=600)
    return r.stdout + r.stderr, (json.load(open(js)) if os.path.exists(js) else None)

td = tempfile.mkdtemp()
env = dict(os.environ, KONCLUDE_SUPERVISED="1")

# 1. the profiler is off by default: no table, no extra output
r = subprocess.run([K, "consistency", "-w", "1", "-i", os.path.join(HERE, "hobby_200.ofn")], capture_output=True, text=True, env=env, timeout=300)
check("profiler off by default: no table in the output, same verdict", "KONCLUDE-PROFILE" not in r.stdout + r.stderr and "is consistent" in r.stdout + r.stderr, r.stdout[-300:])

# 2. the profiler writes the table to the file when asked
pf = os.path.join(td, "p.txt")
outfile = os.path.join(td, "real.owx")
r = subprocess.run([K, "materialize", "-w", "1", "-i", os.path.join(HERE, "hobby_200.ofn"), "-o", outfile], capture_output=True, text=True,
                   env=dict(env, KONCLUDE_PROFILE="1", KONCLUDE_PROFILE_FILE=pf), timeout=300)
txt = open(pf).read() if os.path.exists(pf) else ""
check("profiler on: the table is written at the end of the run and the output file is still written", txt.startswith("KONCLUDE-PROFILE v1") and os.path.exists(outfile) and os.path.getsize(outfile) > 1000, txt[:300] + r.stdout[-300:])

# 3. the diagnosis finds the hobby pattern
js = os.path.join(td, "d.json")
out, rep = diagnose(os.path.join(HERE, "hobby_200.ofn"), ["--task", "materialize", "--timeout", "120"], js)
top = rep["profile"][0] if rep and rep.get("profile") else {}
check("diagnosis: the run finishes and the top concept is the enumeration of the hobbies (a choice among 8 enumerated individuals)",
      rep and rep["outcome"] == "finished" and "enumerated individuals" in out and "choice among 8" in out and top.get("branches", 0) > 0, out[-1500:])
check("diagnosis: the minimum cardinality over hasHobby appears with new nodes, and the lint finding about it is reported",
      any("hasHobby" in r_["concept"] and r_["nodes"] > 0 for r_ in rep["profile"]) and "Minimum cardinality over an enumerated class" in out, out[-1500:])
check("diagnosis: the numbers are consistent (branches and nodes of the rows add up to at most the totals)",
      sum(r_["branches"] for r_ in rep["profile"]) <= int(rep["profile_header"]["branches"]) and sum(r_["nodes"] for r_ in rep["profile"]) <= int(rep["profile_header"]["nodes"]), rep["profile_header"])

# 4. a run that has to be stopped still leaves the last table
bigf = os.path.join(td, "hobby_big.ofn")
subprocess.run([sys.executable, os.path.join(HERE, "make_hobby.py"), bigf, "4000", "8"], check=True)
out, rep = diagnose(bigf, ["--task", "materialize", "--timeout", "4", "--interval", "1"], os.path.join(td, "d2.json"))
check("a run that is stopped by the time limit: the report says so and still shows what the reasoner was working on",
      rep and rep["outcome"].startswith("stopped: time limit") and rep.get("profile") and "The run did not finish" in out, out[-1200:])
out, rep = diagnose(bigf, ["--task", "materialize", "--timeout", "60", "--max-memory-mb", "150", "--interval", "1"], os.path.join(td, "d3.json"))
check("a run that is stopped by the memory limit is reported as such", rep and "memory limit" in rep["outcome"], out[-800:])

print("\n%d of %d checks passed" % (sum(results), len(results)))
sys.exit(0 if all(results) else 1)
