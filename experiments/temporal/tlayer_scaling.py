#!/usr/bin/env python3
"""Scaling of the temporal layer: N people, each with a few role intervals over 100 days; cache against no cache.
usage: tlayer_scaling.py KONCLUDE N [N ...]"""
import random, subprocess, sys, os, time, json, tempfile
K = sys.argv[1]; Ns = [int(x) for x in sys.argv[2:]]
HERE = os.path.dirname(os.path.abspath(__file__))
TL = os.path.join(HERE, "..", "..", "tools", "temporal", "tlayer.py")
TB = os.path.join(HERE, "..", "..", "tests", "temporal", "hr_tbox.ofn")
for n in Ns:
    rng = random.Random(5)
    lines = []
    for i in range(n):
        s = rng.randint(1, 60)
        lines.append("Employee(P%d) [%d, %d]" % (i, s, s + rng.randint(5, 40)))
        if rng.random() < 0.5:
            s2 = rng.randint(1, 80); lines.append("Manager(P%d) [%d, %d]" % (i, s2, s2 + rng.randint(1, 20)))
        if rng.random() < 0.3:
            s3 = rng.randint(1, 80); lines.append("Contractor(P%d) [%d, %d]" % (i, s3, s3 + rng.randint(1, 15)))
        lines.append("worksFor(P%d, ACME) [%d, %d]" % (i, s, s + 30))
    td = tempfile.mkdtemp(); fa = os.path.join(td, "f.tl"); open(fa, "w").write("\n".join(lines))
    row = []
    for extra in ([], ["--no-cache"]):
        o = os.path.join(td, "o.json")
        t0 = time.time()
        r = subprocess.run([sys.executable, TL, "--tbox", TB, "--facts", fa, "--konclude", K, "-o", o, "--closed", "--validation",
                            "--formula", "G(Employee(?x) -> F Manager(?x))", "--show", "problems"] + extra, capture_output=True, text=True)
        dt = time.time() - t0
        d = json.load(open(o))
        viol = sum(1 for x in d["formulas"][0]["results"] if x["result"] == "FALSE")
        row.append((dt, sum(d["calls"].values()), d["stats"]["components"], d["stats"]["cache_hits"], len(d["states"]), viol))
    (t1, c1, comp, hits, st, v1), (t2, c2, _, _, _, v2) = row
    print("N=%4d  states=%3d  components=%6d  cache hits=%6d | with cache: %6.1f s, %4d Konclude calls | no cache: %6.1f s, %4d calls | violations %d / %d" % (n, st, comp, hits, t1, c1, t2, c2, v1, v2))
