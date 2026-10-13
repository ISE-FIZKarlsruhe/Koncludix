#!/usr/bin/env python3
"""
Tests of the temporal layer (tools/temporal/tlayer.py).

  python3 tests/temporal/run_temporal_tests.py PATH_TO_KONCLUDE [--random N]

Part 1 (no reasoner): the formula evaluator against a brute-force definition of the finite-trace semantics
         (closed history) and against all short continuations (monitoring: TRUE/FALSE are never wrong).
Part 2 (with Konclude): the HR example with known answers; inconsistent snapshots; the cache never changes an answer.
"""
import itertools, json, os, random, subprocess, sys, tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "tools", "temporal"))
import tlayer as tl

# ------------------------------------------------------------------ part 1: the evaluator

ATOMS = ["p", "q"]

def rand_formula(rng, depth, fut=True):
    """Random formula; past operators only over future-free parts (the layer's rule)."""
    if depth == 0 or rng.random() < 0.2:
        return ("atom", rng.choice(ATOMS), ("a",))
    ops = ["not", "and", "or", "Y", "O", "H", "S", "Om", "Hm"] + (["X", "F", "G", "U", "Fm", "Gm"] if fut else [])
    op = rng.choice(ops)
    if op == "not": return tl.mk_not(rand_formula(rng, depth - 1, fut))
    if op in ("and", "or"): return tl._nary(op, [rand_formula(rng, depth - 1, fut), rand_formula(rng, depth - 1, fut)])
    if op == "U": return (op, rand_formula(rng, depth - 1, fut), rand_formula(rng, depth - 1, fut))
    if op == "S": return (op, rand_formula(rng, depth - 1, False), rand_formula(rng, depth - 1, False))
    if op in ("Y", "O", "H"): return (op, rand_formula(rng, depth - 1, False))
    if op in ("Fm", "Gm", "Om", "Hm"):
        a = rng.randint(0, 2); return (op, rand_formula(rng, depth - 1, False), a, a + rng.randint(0, 3))
    return (op, rand_formula(rng, depth - 1, fut))

def direct(f, trace, i):
    """The textbook finite-trace (LTLf) semantics; strong next. trace: list of dicts atom -> bool."""
    k = f[0]
    n = len(trace)
    if k == "c": return f[1] == "T"
    if k == "atom": return trace[i][f[1]]
    if k == "not": return not direct(f[1], trace, i)
    if k == "and": return all(direct(x, trace, i) for x in f[1])
    if k == "or": return any(direct(x, trace, i) for x in f[1])
    if k == "X": return i + 1 < n and direct(f[1], trace, i + 1)
    if k == "F": return any(direct(f[1], trace, j) for j in range(i, n))
    if k == "G": return all(direct(f[1], trace, j) for j in range(i, n))
    if k == "U": return any(direct(f[2], trace, j) and all(direct(f[1], trace, m) for m in range(i, j)) for j in range(i, n))
    if k == "Y": return i > 0 and direct(f[1], trace, i - 1)
    if k == "O": return any(direct(f[1], trace, j) for j in range(0, i + 1))
    if k == "H": return all(direct(f[1], trace, j) for j in range(0, i + 1))
    if k == "S": return any(direct(f[2], trace, j) and all(direct(f[1], trace, m) for m in range(j + 1, i + 1)) for j in range(0, i + 1))
    if k == "Fm": return any(direct(f[1], trace, j) for j in range(i + f[2], min(i + f[3], n - 1) + 1))
    if k == "Gm": return all(direct(f[1], trace, j) for j in range(i + f[2], min(i + f[3], n - 1) + 1))
    if k == "Om": return i - f[2] >= 0 and any(direct(f[1], trace, j) for j in range(max(0, i - f[3]), i - f[2] + 1))
    if k == "Hm": return i - f[2] < 0 or all(direct(f[1], trace, j) for j in range(max(0, i - f[3]), i - f[2] + 1))
    raise ValueError(k)

def monitor_result(f, trace):
    def av(name, args, i): return tl.T if trace[i][name] else tl.F
    mon = tl.Monitor(av)
    R, _ = mon.run(f, 0, len(trace))
    return R

def part1(n_random):
    rng = random.Random(7)
    bad_closed = bad_sound = checked = 0
    exts = [list(e) for L in (1, 2, 3) for e in itertools.product([dict(p=x, q=y) for x in (0, 1) for y in (0, 1)], repeat=L)]
    for _ in range(n_random):
        f = rand_formula(rng, 3)
        trace = [dict(p=rng.random() < .5, q=rng.random() < .5) for _ in range(rng.randint(1, 5))]
        R = monitor_result(f, trace)
        # closed history: finish(R) equals the direct finite-trace value
        fin = tl.finish(R)
        want = direct(f, trace, 0)
        if fin != (tl.T if want else tl.F): bad_closed += 1
        # monitoring: TRUE / FALSE hold for every continuation
        st = tl.status(R)
        if st in ("TRUE", "FALSE"):
            for e in exts:
                if direct(f, trace + e, 0) != (st == "TRUE"):
                    bad_sound += 1; break
        checked += 1
    return checked, bad_closed, bad_sound

def part_support(n_random):
    """The explanation over time: the cells returned by minimal_support decide the answer on their own, and no single cell can be left out."""
    rng = random.Random(21)
    checked = bad_sufficient = bad_minimal = 0
    for _ in range(n_random):
        f = rand_formula(rng, 3)
        n = rng.randint(1, 6)
        trace = [dict(p=rng.random() < .5, q=rng.random() < .5) for _ in range(n)]
        av = lambda name, args, i, tr=trace: tl.T if tr[i][name] else tl.F
        for closed in (False, True):
            answer, cells = tl.minimal_support(f, av, 0, n, closed)
            if cells is None: continue
            checked += 1
            def decided_by(keep):
                keep = set(keep)
                mon = tl.Monitor(lambda name, args, i: av(name, args, i) if (name, args, i) in keep else tl.U)
                R, _ = mon.run(f, 0, n)
                return tl.status(tl.finish(R) if closed else R) == answer and (closed or R[0] == "c")
            if not decided_by(cells): bad_sufficient += 1
            elif any(decided_by(cells[:i] + cells[i + 1:]) for i in range(len(cells))): bad_minimal += 1
    return checked, bad_sufficient, bad_minimal

# ------------------------------------------------------------------ part 2: with Konclude

def run_layer(konclude, tbox, facts, formulas, extra=(), raw_output=False):
    out = tempfile.mktemp(suffix=".json")
    cmd = [sys.executable, os.path.join(HERE, "..", "..", "tools", "temporal", "tlayer.py"), "--tbox", os.path.join(HERE, tbox), "--facts", os.path.join(HERE, facts),
           "--konclude", konclude, "-o", out]
    for f in formulas: cmd += ["--formula", f]
    cmd += list(extra) + os.environ.get("TLAYER_EXTRA", "").split()      # for example TLAYER_EXTRA=--live runs every check in live mode
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if not os.path.exists(out): return None, r.stdout + r.stderr
    d = json.load(open(out)); os.remove(out)
    return d, r.stdout

def res(d, i, **binding):
    for r in d["formulas"][i]["results"]:
        if all(r["binding"].get("?" + k) == v for k, v in binding.items()): return r
    return None

CASES = []
def case(name):
    def deco(fn): CASES.append((name, fn)); return fn
    return deco

HR = ("hr_tbox.ofn", "hr_facts.tl")

@case("F: an inferred fact (Alice is an employee because she is a manager): TRUE, fixed in the first state")
def _(k):
    d, _o = run_layer(k, *HR, ["F Employee(Alice)"])
    r = d["formulas"][0]["results"][0]
    return r["result"] == "TRUE" and r["decided_at"] == "2025-01-01", r

@case("three values: entailed / not entailed / entailed negation (disjoint classes)")
def _(k):
    d, _o = run_layer(k, *HR, ["Employee(Eve)", "!Employee(Dan)", "Employee(Dan)"])
    a, b, c = (d["formulas"][i]["results"][0]["result"] for i in range(3))
    return (a, b, c) == ("UNKNOWN", "TRUE", "FALSE"), (a, b, c)

@case("G over all individuals: unmet obligations are PENDING while history is open, UNKNOWN for Bob in the open world")
def _(k):
    d, _o = run_layer(k, *HR, ["G(Employee(?x) -> F Manager(?x))"])
    a, b = res(d, 0, x="Alice"), res(d, 0, x="Bob")
    ok = a["result"] == "PENDING" and a["if_history_ends_now"] == "TRUE" and b["result"] == "PENDING" and b["if_history_ends_now"] == "UNKNOWN"
    return ok, (a, b)

@case("validation mode + closed history: Bob and Carla violate, Alice does not")
def _(k):
    d, _o = run_layer(k, *HR, ["G(Employee(?x) -> F Manager(?x))"], ["--validation", "--closed"])
    got = {x: res(d, 0, x=x)["result"] for x in ("Alice", "Bob", "Carla", "Dan")}
    return got == {"Alice": "TRUE", "Bob": "FALSE", "Carla": "FALSE", "Dan": "FALSE"}, got

@case("past operators (--at May): Dan was a student before; Alice an employee; Dan was never a manager (his student status excludes it)")
def _(k):
    d, _o = run_layer(k, *HR, ["O Student(Dan)", "Manager(Alice) -> O Employee(Alice)", "H !Manager(Dan)"], ["--at", "2025-05-01"])
    got = [d["formulas"][i]["results"][0]["result"] for i in range(3)]
    return got == ["TRUE", "TRUE", "TRUE"], got

@case("object property atoms: worksFor(Alice, ACME) holds until she leaves")
def _(k):
    d, _o = run_layer(k, *HR, ["worksFor(Alice, ACME)", "F !worksFor(Alice, ACME)", "worksFor(Alice, ACME) U Manager(Alice)"])
    got = [d["formulas"][i]["results"][0]["result"] for i in range(3)]
    # the second one: the property is never entailed false (open world) -> UNKNOWN; the third: the until is met only if the property holds up to the manager state
    return got[0] == "TRUE" and got[1] in ("UNKNOWN", "PENDING"), got

@case("--now cuts the history: with now = 2025-04-15 Alice is not yet a manager, F Manager(Alice) is PENDING")
def _(k):
    d, _o = run_layer(k, *HR, ["F Manager(Alice)"], ["--now", "2025-04-15"])
    r = d["formulas"][0]["results"][0]
    return r["result"] == "PENDING" and len(d["states"]) == 4, (r, len(d["states"]))

@case("an inconsistent snapshot is reported and does not hide the other individuals")
def _(k):
    d, o = run_layer(k, "hr_tbox.ofn", "hr_bad_facts.tl", ["F Employee(Bob)", "F Employee(Dan)"])
    ok = d is not None and d["formulas"][0]["results"][0]["result"] == "TRUE" and d["formulas"][1]["results"][0]["result"] in ("UNKNOWN", "PENDING") \
        and "inconsistent for Dan" in o and "DisjointClasses" in o
    return ok, o[-900:]

@case("deadlines: F[0,60] Manager: the deadline passes on the day start + 60 (FALSE is fixed then), a longer deadline is met")
def _(k):
    import datetime
    d, _o = run_layer(k, *HR, ["G(Employee(?x) -> F[0,60] Manager(?x))", "G(Employee(?x) -> F[0,200] Manager(?x))"], ["--now", "2025-12-31", "--validation"])
    def day(s, n): return (datetime.date.fromisoformat(s) + datetime.timedelta(days=n)).isoformat()
    a = {x: res(d, 0, x=x) for x in ("Alice", "Bob", "Carla", "Dan")}
    b = {x: res(d, 1, x=x) for x in ("Alice", "Bob")}
    ok = (a["Bob"]["result"] == "FALSE" and a["Bob"]["decided_at"] == day("2025-02-01", 60) and a["Carla"]["decided_at"] == day("2025-03-01", 60)
          and a["Dan"]["decided_at"] == day("2025-07-01", 60) and a["Alice"]["decided_at"] == day("2025-01-01", 60)
          and b["Alice"]["result"] != "FALSE" and b["Bob"]["result"] == "FALSE" and b["Bob"]["decided_at"] == day("2025-02-01", 200))
    return ok, (a, b)

@case("deadline operators need --now")
def _(k):
    d, o = run_layer(k, *HR, ["G(Employee(?x) -> F[0,60] Manager(?x))"])
    return d is None and "needs --now" in o, o[-300:]

@case("temporal definition feeds the reasoner: FormerEmployee (past formula) is derived, the TBox makes it an Alumnus, F Alumnus(Bob) is TRUE")
def _(k):
    d, o = run_layer(k, "hr_tbox_defs.ofn", "hr_def_facts.tl", ["F Alumnus(Bob)", "F Alumnus(Alice)"],
                     ["--validation", "--define", "FormerEmployee(?x) := O Employee(?x) & !Employee(?x)"])
    if d is None: return False, o
    inds = sorted({x["individual"] for x in d["derived"]})
    bob = [x for x in d["derived"] if x["individual"] == "Bob"]
    r0, r1 = d["formulas"][0]["results"][0], d["formulas"][1]["results"][0]
    ok = inds == ["Bob", "Yan"] and bob == [{"definition": "FormerEmployee", "individual": "Bob", "from": "2025-04-01", "to": "∞"}]         and r0["result"] == "TRUE" and r0["decided_at"] == "2025-04-01" and r1["result"] != "TRUE"
    return ok, (inds, bob, r0, r1)

@case("a derived class can make a period inconsistent (Alumnus and Student are disjoint) and the cause names the definition")
def _(k):
    d, o = run_layer(k, "hr_tbox_defs.ofn", "hr_def_facts.tl", ["F Alumnus(Bob)"], ["--validation", "--define", "FormerEmployee(?x) := O Employee(?x) & !Employee(?x)"])
    if d is None: return False, o
    bad = [st for st in d["states"] if "Yan" in st["inconsistent"]]
    ok = bad and bad[0]["from"] == "2025-05-01" and "derived by the definition of FormerEmployee" in o and "DisjointClasses" in o
    return ok, (bad[:2], o[-700:])

@case("a definition with a deadline: Stale = was an employee at least 30 days ago and is not a manager")
def _(k):
    import datetime
    d, o = run_layer(k, *HR, ["Stale(Carla)"], ["--validation", "--now", "2025-12-31", "--define", "Stale(?x) := O[30,inf] Employee(?x) & !Manager(?x)"])
    if d is None: return False, o
    def day(s, n): return (datetime.date.fromisoformat(s) + datetime.timedelta(days=n)).isoformat()
    got = {(x["individual"], x["from"], x["to"]) for x in d["derived"]}
    want = {("Alice", day("2025-01-01", 30), "2025-05-31"), ("Bob", day("2025-02-01", 30), "2025-12-31"), ("Carla", day("2025-03-01", 30), "2025-12-31"), ("Dan", day("2025-07-01", 30), "2025-12-31")}
    return got == want, (sorted(got), sorted(want))

@case("a definition may only look at the past, and cannot use another definition")
def _(k):
    d1, o1 = run_layer(k, *HR, [], ["--define", "Bad(?x) := F Manager(?x)"])
    d2, o2 = run_layer(k, *HR, [], ["--define", "A(?x) := Employee(?x)", "--define", "B(?x) := A(?x) & Manager(?x)"])
    return d1 is None and "only look at the present and the past" in o1 and d2 is None and "another defined class" in o2, (o1[-200:], o2[-200:])

@case("--why: the smallest set of atom values over time behind a missed deadline (Bob: employee from February, never a manager during the whole deadline window)")
def _(k):
    d, o = run_layer(k, *HR, ["G(Employee(?x) -> F[0,200] Manager(?x))"], ["--now", "2025-12-31", "--validation", "--why"])
    if d is None: return False, o
    bob = res(d, 0, x="Bob")
    got = [(c["atom"], c["value"], c["from"], c["to"], c["assumed"]) for c in bob["why"]]
    want = [("Employee(Bob)", "true", "2025-02-01", "2025-02-28", False), ("Manager(Bob)", "false", "2025-02-01", "2025-08-31", True)]
    return got == want, got

@case("the cache: answers unchanged, fewer Konclude calls")
def _(k):
    f = ["G(Employee(?x) -> F Manager(?x))", "F Employee(Alice)", "H !Student(Dan)"]
    a, _o = run_layer(k, *HR, f, ["--closed", "--validation"])
    b, _o = run_layer(k, *HR, f, ["--closed", "--validation", "--no-cache"])
    same = a["formulas"] == b["formulas"]
    if "--live" in os.environ.get("TLAYER_EXTRA", ""):      # in live mode there are no Konclude process calls to count: compare the component checks answered from the cache
        return same and a["stats"]["cache_hits"] > 0 and b["stats"]["cache_hits"] == 0, (a["stats"], b["stats"])
    return same and sum(a["calls"].values()) < sum(b["calls"].values()), (a["calls"], b["calls"])

def random_case(n):
    def fn(k):
        rng = random.Random(11)
        classes = ["A", "B", "C", "D"]
        tbox = ("Prefix(:=<http://r.example/o#>)\nOntology(<http://r.example/o>\n"
                "Declaration(Class(:A))\nDeclaration(Class(:B))\nDeclaration(Class(:C))\nDeclaration(Class(:D))\nDeclaration(ObjectProperty(:r))\n"
                "SubClassOf(:A :B)\nDisjointClasses(:C :D)\nSubClassOf(:B ObjectAllValuesFrom(:r :C))\nSubClassOf(ObjectSomeValuesFrom(:r :D) :A)\n)\n")
        bad = 0; total = 0; consistent_tl = 0; bad_whole = 0; bad_live = 0
        for t in range(n):
            lines = []
            for _ in range(rng.randint(5, 12)):
                s = rng.randint(0, 12); e = s + rng.randint(0, 8)
                x = "i%d" % rng.randint(0, 4)
                if rng.random() < 0.7: lines.append("%s(%s) [%d, %d]" % (rng.choice(classes), x, s, e))
                else: lines.append("r(%s, i%d) [%d, %d]" % (x, rng.randint(0, 4), s, e))
            with tempfile.TemporaryDirectory() as td:
                tb, fa = os.path.join(td, "t.ofn"), os.path.join(td, "f.tl")
                open(tb, "w").write(tbox); open(fa, "w").write("\n".join(lines))
                outs = []
                for extra in ([], ["--no-cache"], ["--whole-snapshots"], ["--live", "--live-timeout", "60"]):
                    o = os.path.join(td, "o%d.json" % len(outs))
                    cmd = [sys.executable, os.path.join(HERE, "..", "..", "tools", "temporal", "tlayer.py"), "--tbox", tb, "--facts", fa, "--konclude", k, "-o", o,
                           "--formula", "G(B(?x) -> F D(?x))", "--formula", "O C(?x) -> A(?x)", "--formula", "A(?x) U C(?x)", "--formula", "H !D(?x)"] + extra
                    r = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
                    if not os.path.exists(o): return False, r.stdout + r.stderr
                    outs.append(json.load(open(o)))
                total += 1
                if outs[0]["formulas"] != outs[1]["formulas"] or outs[0]["states"] != outs[1]["states"]: bad += 1
                if not any(st["inconsistent"] for st in outs[0]["states"]):   # consistent timeline: components and whole snapshots must agree
                    consistent_tl += 1
                    if outs[0]["formulas"] != outs[2]["formulas"]: bad_whole += 1
                if outs[0]["formulas"] != outs[3]["formulas"] or outs[0]["states"] != outs[3]["states"]: bad_live += 1     # live mode (one running Konclude, Tell/Retract) must give the same answers, also with inconsistent periods
        return bad == 0 and bad_whole == 0 and bad_live == 0, "%d random timelines: cached and uncached answers differ in %d; of the %d consistent timelines, per-component and whole-snapshot answers differ in %d; live mode differs from the normal mode in %d" % (total, bad, consistent_tl, bad_whole, bad_live)
    return fn

def main():
    if len(sys.argv) < 2:
        print(__doc__); return 2
    k = sys.argv[1]
    n_random = int(sys.argv[sys.argv.index("--random") + 1]) if "--random" in sys.argv else 15
    ok_all = True
    checked, bc, bs = part1(2000)
    ok = bc == 0 and bs == 0
    ok_all &= ok
    print("%s  evaluator (with deadline operators): %d random formulas on random traces, closed-history value differs from the brute-force semantics in %d, TRUE/FALSE wrong for some continuation in %d" % ("PASS" if ok else "FAIL", checked, bc, bs))
    c3, bs, bm = part_support(1500)
    ok = bs == 0 and bm == 0
    ok_all &= ok
    print("%s  explanation over time: %d decided answers on random traces and formulas, the returned cells fail to decide the answer in %d, a cell could be left out in %d" % ("PASS" if ok else "FAIL", c3, bs, bm))
    for name, fn in CASES + [("random timelines: the component cache never changes an answer", random_case(n_random))]:
        try:
            ok, info = fn(k)
        except Exception as e:
            ok, info = False, repr(e)
        ok_all &= bool(ok)
        print("%s  %s" % ("PASS" if ok else "FAIL", name))
        if not ok: print("      " + str(info)[:1500])
    print("\n%s" % ("all checks passed" if ok_all else "SOME CHECKS FAILED"))
    return 0 if ok_all else 1

if __name__ == "__main__":
    sys.exit(main())
