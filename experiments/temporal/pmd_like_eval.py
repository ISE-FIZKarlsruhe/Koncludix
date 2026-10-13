#!/usr/bin/env python3
"""
A realistic test of the temporal layer on made-up materials-testing data (PMD style), with known violations.

Every sample goes through Casting -> Forging -> Heat treatment (Annealing, Quenching, Tempering) -> Machining -> Tensile test / Hardness test, and is archived afterwards.
The data are written as 'Under<Step>(sample) [from, to]' facts (days). Violations are injected into about 18 % of the samples, one kind per sample:
  order        a step starts while an earlier step is still running, or before it (the make_sample() injection of timelines.py)
  no_test      the sample is archived but was never tensile-tested
  quarantine   the sample is put on hold (Quarantined) while its tensile test runs; the ontology says these two are disjoint -> inconsistent period
  late_test    the tests start 40 to 70 days late (deadline: a tensile test must start within 35 days after machining)
  overdue      not injected: a sample counts as overdue while its machining ended at least 36 days ago (no machining in the last 36 days) and no tensile test has started. This is a TEMPORAL DEFINITION
               (Overdue(?x) := H[0,35] !UnderMachining(?x) & O UnderMachining(?x) & !O UnderTensileTest(?x)); the ontology says Overdue is a NeedsAttention, and the rule  G(!NeedsAttention(?x))  is checked.
               It shows that a derived class goes through the reasoner (class hierarchy) and the result is exact.
The ground truth is computed by a literal day-by-day simulation of the rules (it does not use the layer, Konclude or Allen's algebra).

Rules (checked with --validation, a fact that is not recorded counts as false; classes follow the hierarchy of the ontology):
  G(Under<Later>(?x) -> (!Under<Earlier>(?x) & O Under<Earlier>(?x)))        for five (Later, Earlier) pairs
  G(Archived(?x) -> O UnderTensileTest(?x))
  G((UnderTensileTest(?x) & !Y UnderTensileTest(?x)) -> O[0,35] UnderMachining(?x))              deadline (needs the day on which the observation ends)

usage (WSL): pmd_like_eval.py KONCLUDE N_SAMPLES SEED [--now-day D] [--interval]
  --now-day D   the history ends at day D (monitoring while the campaign is running)
  --interval    also run the interval module + DL rules route on the same samples (process-form) and compare
"""
import datetime, json, os, random, re, subprocess, sys, tempfile, time
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import timelines
from timelines import CLASSES, RULES, make_sample, violations, is_a

BASE = datetime.date(2025, 1, 1)
def D(d): return (BASE + datetime.timedelta(days=d)).isoformat()

def ancestors(c):
    out = []
    while c: out.append(c); c = CLASSES[c]
    return out

def generate(n, seed):
    rnd = random.Random(seed)
    samples = []
    for k in range(n):
        r = rnd.random()
        kind = "clean" if r < 0.78 else ("order" if r < 0.88 else ("no_test" if r < 0.92 else ("quarantine" if r < 0.96 else "late_test")))
        steps = make_sample(rnd, kind == "order")
        if kind == "late_test":
            shift = rnd.randint(40, 70)
            steps = [(c, a + shift, b + shift) if c in ("TensileTest", "HardnessTest") else (c, a, b) for c, a, b in steps]
        if kind == "no_test": steps = [s for s in steps if s[0] != "TensileTest"]
        last = max(e for _, _, e in steps)
        arch = last + rnd.randint(3, 30)
        quarantine = None
        if kind == "quarantine":
            ts, te = next((s, e) for c, s, e in steps if c == "TensileTest")
            q0 = rnd.randint(ts, te - 1)
            quarantine = (q0, q0 + rnd.randint(2, 10))
        samples.append(dict(id=k, kind=kind, steps=steps, archived=arch, quarantine=quarantine))
    return samples

def truth_by_simulation(s, now):
    """Literal reading of the rules, day by day. Steps are [start, end) in days; a step is active on the days start .. end-1."""
    def active(cls, t):   # any step whose class is cls or below
        return any(is_a(c, cls) and a <= t < b for c, a, b in s["steps"])
    def once(cls, t):
        return any(is_a(c, cls) and a <= t0 < b for c, a, b in s["steps"] for t0 in range(a, min(b, t + 1)))
    viol = set()
    horizon = min(now, max(b for _, _, b in s["steps"]) + 40) if now is not None else max(b for _, _, b in s["steps"]) + 40
    first = min(a for _, a, _ in s["steps"])
    for t in range(first, horizon + 1):
        for i, (later, earlier) in enumerate(RULES):
            if active(later, t) and not (not active(earlier, t) and once(earlier, t)): viol.add("order")
        if s["archived"] <= t and not once("TensileTest", t): viol.add("no_test")
        if s["quarantine"] and s["quarantine"][0] <= t <= s["quarantine"][1] and active("TensileTest", t): viol.add("quarantine")
        if active("TensileTest", t) and not active("TensileTest", t - 1) and not any(active("Machining", j) for j in range(t - 35, t + 1)): viol.add("late_test")
    # overdue: from the 36th day after the last machining day on, as long as no tensile test has started
    m_last = [b - 1 for c, a, b in s["steps"] if is_a(c, "Machining")]
    t_days = [a for c, a, b in s["steps"] if is_a(c, "TensileTest")]
    if m_last:
        od_from = max(m_last) + 36
        od_to = (min(t_days) - 1) if t_days else horizon
        if od_from <= min(od_to, horizon): viol.add("overdue")
    return viol

def write_tbox(path):
    L = ["Prefix(:=<http://pmd.example/o#>)", "Ontology(<http://pmd.example/o>"]
    for c in CLASSES: L.append("Declaration(Class(:Under%s))" % c)
    L += ["Declaration(Class(:Overdue))", "Declaration(Class(:NeedsAttention))", "SubClassOf(:Overdue :NeedsAttention)"]
    for c, p in CLASSES.items():
        if p: L.append("SubClassOf(:Under%s :Under%s)" % (c, p))
    L += ["Declaration(Class(:Quarantined))", "Declaration(Class(:Archived))", "DisjointClasses(:Quarantined :UnderTensileTest)", ")"]
    open(path, "w").write("\n".join(L) + "\n")

def write_facts(path, samples, now):
    L = []
    for s in samples:
        for c, a, b in s["steps"]:
            if now is not None and a > now: continue
            L.append("Under%s(s%d) [%s, %s]" % (c, s["id"], D(a), D(b - 1)))
        if s["quarantine"]: L.append("Quarantined(s%d) [%s, %s]" % (s["id"], D(s["quarantine"][0]), D(s["quarantine"][1])))
        if now is None or s["archived"] <= now: L.append("Archived(s%d) [%s, inf)" % (s["id"], D(s["archived"])))
    open(path, "w").write("\n".join(L) + "\n")
    return len(L)

def main():
    K, n, seed = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
    now = int(sys.argv[sys.argv.index("--now-day") + 1]) if "--now-day" in sys.argv else None
    samples = generate(n, seed)
    now_eff = now if now is not None else max(s["archived"] for s in samples) + 10
    td = tempfile.mkdtemp(prefix="pmdlike_")
    tb, fa = os.path.join(td, "tbox.ofn"), os.path.join(td, "facts.tl")
    write_tbox(tb); nf = write_facts(fa, samples, now)
    truth = {s["id"]: truth_by_simulation(s, now_eff) for s in samples}
    kinds = {}
    for s in samples: kinds[s["kind"]] = kinds.get(s["kind"], 0) + 1
    print("samples %d, facts %d, injected: %s%s" % (n, nf, kinds, ("  (history ends at day %d = %s)" % (now, D(now))) if now is not None else ""))
    print("ground truth by day-by-day simulation: violating samples: %s" % {k: sum(1 for v in truth.values() if k in v) for k in ("order", "no_test", "quarantine", "late_test", "overdue")})
    forms = []
    for later, earlier in RULES:
        forms.append(("order", "G(Under%s(?x) -> (!Under%s(?x) & O Under%s(?x)))" % (later, earlier, earlier)))
    forms.append(("no_test", "G(Archived(?x) -> O UnderTensileTest(?x))"))
    forms.append(("late_test", "G((UnderTensileTest(?x) & !Y UnderTensileTest(?x)) -> O[0,35] UnderMachining(?x))"))
    forms.append(("overdue", "G(!NeedsAttention(?x))"))
    out = os.path.join(td, "res.json")
    cmd = [sys.executable, os.path.join(HERE, "..", "..", "tools", "temporal", "tlayer.py"), "--tbox", tb, "--facts", fa, "--konclude", K, "--validation", "-o", out, "--show", "problems"]
    cmd += ["--now", D(now_eff)]
    cmd += ["--define", "Overdue(?x) := H[0,35] !UnderMachining(?x) & O UnderMachining(?x) & !O UnderTensileTest(?x)"]
    for _, f in forms: cmd += ["--formula", f]
    t0 = time.time()
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=3000)
    dt = time.time() - t0
    if not os.path.exists(out): print(r.stdout[-1500:], r.stderr[-1500:]); return 1
    d = json.load(open(out))
    found = {s["id"]: set() for s in samples}
    for (kind, _), fr in zip(forms, d["formulas"]):
        for x in fr["results"]:
            if x["result"] == "FALSE": found[int(x["binding"]["?x"][1:])].add(kind)
    for st in d["states"]:
        for ind in st["inconsistent"]: found[int(ind[1:])].add("quarantine")
    print("temporal layer: %d states, %d Konclude calls, %d of %d component checks answered from the cache, %.0f s in total" % (
          len(d["states"]), sum(d["calls"].values()), d["stats"]["cache_hits"], d["stats"]["components"], dt))
    ok_all = True
    for k in ("order", "no_test", "quarantine", "late_test", "overdue"):
        T = {i for i, v in truth.items() if k in v}; Fd = {i for i, v in found.items() if k in v}
        ok = T == Fd; ok_all &= ok
        print("  %-10s truth %3d, found %3d, missed %d, false alarms %d  %s" % (k, len(T), len(Fd), len(T - Fd), len(Fd - T), "exact" if ok else "DIFFERENT"))
        if not ok: print("     missed: %s   false: %s" % (sorted(T - Fd)[:8], sorted(Fd - T)[:8]))
    if "--interval" in sys.argv:
        lines = timelines.theory(); lines.append("Declaration(ObjectProperty(:inSample))")
        PROV = "http://www.w3.org/ns/prov#"; XSD = "http://www.w3.org/2001/XMLSchema#"
        for s in samples:
            for c, a, b in s["steps"]:
                ind = ":s%d_%s" % (s["id"], c)
                lines += ["ClassAssertion(:%s %s)" % (c, ind), 'DataPropertyAssertion(<%sstartedAtTime> %s "%d"^^<%sinteger>)' % (PROV, ind, a, XSD),
                          'DataPropertyAssertion(<%sendedAtTime> %s "%d"^^<%sinteger>)' % (PROV, ind, b, XSD), "ObjectPropertyAssertion(:inSample %s :sample%d)" % (ind, s["id"])]
        path = os.path.join(td, "process_form.ofn")
        open(path, "w", newline="\n").write("\n".join(lines) + "\n)\n")
        allen_truth = [s["id"] for s in samples if violations(s["steps"])]
        json.dump(dict(samples=n, violating=allen_truth, steps=[]), open(path + ".truth.json", "w"))
        env = dict(os.environ, KONCLUDE_BIN=K)
        f = subprocess.run([sys.executable, os.path.join(HERE, "find_violations.py"), path], capture_output=True, text=True, env=env, timeout=7200)
        print("interval module + DL rules route (Allen relations derived by Konclude from the time stamps), ordering rules only:\n  " + (f.stdout.splitlines()[0] if f.stdout else f.stderr[-400:]))
        order_truth_sim = {i for i, v in truth.items() if "order" in v}
        print("  the two ways of stating the order rule agree on which samples violate it: %s (simulation %d, Allen %d)" % (order_truth_sim == set(allen_truth), len(order_truth_sim), len(allen_truth)))
        if order_truth_sim != set(allen_truth): print("     only simulation: %s   only Allen: %s" % (sorted(order_truth_sim - set(allen_truth))[:8], sorted(set(allen_truth) - order_truth_sim)[:8]))
    print("\nRESULT: %s   (files in %s)" % ("the layer found exactly the injected violations" if ok_all else "differences, see above", td))
    return 0 if ok_all else 1

if __name__ == "__main__":
    sys.exit(main())
