#!/usr/bin/env python3
"""Heavier reasoning per snapshot (>=3 likes.Interest, private interests per person): cache against no cache.
usage: tlayer_heavy.py KONCLUDE N [N ...]"""
import random, subprocess, sys, os, time, json, tempfile
K = sys.argv[1]; Ns = [int(x) for x in sys.argv[2:]]
HERE = os.path.dirname(os.path.abspath(__file__))
TL = os.path.join(HERE, "..", "..", "tools", "temporal", "tlayer.py")
TB = os.path.join(HERE, "tlayer_heavy_tbox.ofn")
for n in Ns:
    rng = random.Random(3)
    lines = []
    for i in range(n):
        s = rng.randint(1, 60); e = s + rng.randint(10, 40)
        lines.append("Person(P%d) [%d, %d]" % (i, s, e))
        k = rng.choice([2, 3, 3, 4])
        for h in range(k):
            hs = rng.randint(s, e); he = min(e, hs + rng.randint(5, 30))
            lines.append("likes(P%d, I%d_%d) [%d, %d]" % (i, i, h, hs, he))
            lines.append("%s(I%d_%d) [%d, %d]" % (rng.choice(["Sport", "Art"]), i, h, hs, he))
    td = tempfile.mkdtemp(); fa = os.path.join(td, "f.tl"); open(fa, "w").write("\n".join(lines))
    row = []
    for extra in ([], ["--no-cache"]):
        o = os.path.join(td, "o.json")
        t0 = time.time()
        subprocess.run([sys.executable, TL, "--tbox", TB, "--facts", fa, "--konclude", K, "-o", o, "--max-bindings", "100000",
                        "--formula", "F PeopleWithManyHobbies(P0)", "--formula", "G(Person(P1) -> F PeopleWithManyHobbies(P1))"] + extra, capture_output=True, text=True)
        dt = time.time() - t0
        d = json.load(open(o))
        row.append((dt, sum(d["calls"].values()), d["stats"]["components"], d["stats"]["cache_hits"], len(d["states"]), [r["result"] for f in d["formulas"] for r in f["results"]]))
    (t1, c1, comp, hits, st, a1), (t2, c2, _, _, _, a2) = row
    print("N=%5d  states=%3d  components=%6d  cache hits=%6d | with cache: %6.1f s (%d calls) | no cache: %6.1f s (%d calls) | same answers: %s" % (n, st, comp, hits, t1, c1, t2, c2, a1 == a2))
