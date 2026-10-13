#!/usr/bin/env python3
"""Test cases of the `Konclude interval` command (interval theory inside Konclude).

usage: run_interval_tests.py PATH_TO_KONCLUDE [--random N]
Runs every case of this directory, checks the report on the standard output and the derived N-Triples, and finally compares the verdict of the command with an
exact ground truth on N random qualitative networks of four intervals (default 200). Exit code 0 if everything passes."""
import itertools, os, random, subprocess, sys, tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
TIME = "http://www.w3.org/2006/time#"
EX = "http://ex.org/"


def tri(s, p, o, ns=TIME): return "<%s%s> <%s%s> <%s%s> ." % (EX, s, ns, p, EX, o)


CASES = [
    dict(name="01 time stamps give the Allen relations", file="01_timestamps.ttl",
         stdout=["Interval result: consistent", "derived relations: 3 from time stamps"],
         derived=[tri("casting", "intervalMeets", "forging"), tri("casting", "intervalBefore", "testing"), tri("forging", "intervalOverlaps", "testing")], derived_count=3),
    dict(name="02 a cycle of before (conflict only through composition)", file="02_cycle.ttl",
         stdout=["Interval result: inconsistent (1 conflict)", "3 facts contradict each other", "asserted: a before b", "asserted: b before c", "asserted: c before a"]),
    dict(name="03 a claim contradicts the time stamps", file="03_claim_vs_timestamps.ttl",
         stdout=["Interval result: inconsistent (1 conflict)", "3 facts contradict each other", "asserted: a after b", "time stamps: a [", "time stamps: b ["]),
    dict(name="04 composition: meets o meets gives before", file="04_composition.ttl",
         stdout=["Interval result: consistent", "1 from the composition of asserted relations"], derived=[tri("a", "intervalBefore", "c")], derived_count=1),
    dict(name="05 two relations for the same pair", file="05_direct_conflict.ttl",
         stdout=["Interval result: inconsistent (1 conflict)", "2 facts contradict each other"]),
    dict(name="06 OWL-Time intervals and instants", file="06_owltime.ttl",
         stdout=["Interval result: consistent", "3 individuals with usable time stamps"],
         derived=[tri("i1", "intervalMeets", "i2"), tri("i1", "intervalBefore", "i3"), tri("i2", "intervalContains", "i3")], derived_count=3),
    dict(name="07 time stamps and two claims about an interval without time stamps", file="07_mixed_network.ttl",
         stdout=["Interval result: inconsistent (1 conflict)", "4 facts contradict each other", "asserted: c during a", "asserted: c after b"]),
    dict(name="08 warnings: zero length and unreadable time", file="08_zero_length_and_unreadable.ttl",
         stdout=["interval ignored (the end is not after the start): x", "could not be read", "Interval result: consistent"], derived=[tri("w", "intervalAfter", "z")], derived_count=1),
    dict(name="09 groups: relations only inside a sample", file="09_groups.ttl", config="09_groups.config.xml",
         stdout=["Interval result: consistent", "derived relations: 2 from time stamps"],
         derived=[tri("s1a", "before", "s1b", "http://konclude.com/interval#"), tri("s2a", "overlaps", "s2b", "http://konclude.com/interval#")], derived_count=2),
    dict(name="10 functional syntax (axioms instead of triples)", file="10_functional_syntax.ofn",
         stdout=["Interval result: inconsistent (1 conflict)", "3 facts contradict each other"]),
    dict(name="13 BFO relations (PMD): a cycle of 'precedes'", file="13_bfo_precedes_cycle.ttl",
         stdout=["Interval result: inconsistent (1 conflict)", "3 facts contradict each other", "asserted: a precedes b", "asserted: b precedes c", "asserted: c precedes a"]),
    dict(name="14 BFO relations: 'preceded by' twice gives 'after' (derived)", file="14_bfo_preceded_by_chain.ttl",
         stdout=["Interval result: consistent", "1 from the composition of asserted relations"], derived_count=1),
    dict(name="15 BFO relations: 'preceded by' and 'precedes' for the same pair", file="15_bfo_both_directions.ttl",
         stdout=["Interval result: inconsistent (1 conflict)", "2 facts contradict each other", "precededBy", "precedes"]),
    dict(name="16 BFO relation against time stamps", file="16_bfo_vs_timestamps.ttl",
         stdout=["Interval result: inconsistent (1 conflict)", "3 facts contradict each other", "asserted: a precedes b", "time stamps: a [", "time stamps: b ["]),
    # ---- the interval theory inside consistency and explain ----
    dict(name="C1 consistency: a cycle of before makes the ontology inconsistent", cmd="consistency", file="02_cycle.ttl",
         stdout=["Ontology '", "' is inconsistent.", "[CONFLICT 1] 3 facts contradict each other", "asserted: a before b"]),
    dict(name="C2 consistency: a claim against the time stamps", cmd="consistency", file="03_claim_vs_timestamps.ttl",
         stdout=["' is inconsistent.", "asserted: a after b", "time stamps: b ["]),
    dict(name="C3 consistency: consistent time stamps stay consistent", cmd="consistency", file="01_timestamps.ttl",
         stdout=["' is consistent.", "Interval theory: 3 individuals with time stamps"]),
    dict(name="C4 consistency: the interval theory can be switched off", cmd="consistency", file="02_cycle.ttl", config="off.config.xml",
         stdout=["' is consistent."], not_stdout=["CONFLICT"]),
    dict(name="C5 consistency: rules over relations derived from time stamps (violation)", cmd="consistency", file="11_rules_over_derived_relations.ofn",
         stdout=["' is inconsistent.", "derived relations are added to the ontology"]),
    dict(name="C6 consistency: the same rule, the order is kept", cmd="consistency", file="12_rules_ok.ofn",
         stdout=["' is consistent.", "derived relations are added to the ontology"]),
    dict(name="C7 consistency: without the derived relations the rule does not see the order", cmd="consistency", file="11_rules_over_derived_relations.ofn", config="noderive.config.xml",
         stdout=["' is consistent."], not_stdout=["derived relations are added"]),
    dict(name="E1 explain: the justification of an interval conflict (the claim and the time stamps)", cmd="explain", file="10_functional_syntax.ofn",
         stdout=["inconsistent because of these 5 axiom(s)", "ObjectPropertyAssertion(<http://example.org/time#after> <http://ex.org/a> <http://ex.org/b>)",
                 "startedAtTime> <http://ex.org/a>", "endedAtTime> <http://ex.org/b>"]),
    dict(name="E2 explain: the justification of a rule violation over derived relations", cmd="explain", file="11_rules_over_derived_relations.ofn",
         stdout=["inconsistent because of these", "SubClassOf(<http://pmd.example/proc#Test> ObjectAllValuesFrom(<http://pmd.example/proc#notAfter>", "ClassAssertion(<http://pmd.example/proc#Machining> <http://pmd.example/proc#machining2>)",
                 "startedAtTime> <http://pmd.example/proc#test2>", "SubObjectPropertyOf(<http://pmd.example/proc#overlappedBy> <http://pmd.example/proc#notAfter>)"],
         not_stdout=["machining1", "test1"]),
]


def run(konclude, args, cmd="interval"):
    env = dict(os.environ, KONCLUDE_SUPERVISED="1")
    return subprocess.run([konclude, cmd] + args, capture_output=True, text=True, env=env, timeout=300)


def run_case(konclude, c):
    out = tempfile.mktemp(suffix=".nt")
    cmd = c.get("cmd", "interval")
    args = []
    if c.get("config"): args += ["-c", os.path.join(HERE, c["config"])]
    args += ["-i", os.path.join(HERE, c["file"])]
    if cmd == "interval": args += ["-o", out]
    r = run(konclude, args, cmd)
    text = r.stdout + r.stderr
    derived = open(out).read().splitlines() if os.path.exists(out) else []
    problems = [("missing in the report: " + s) for s in c.get("stdout", []) if s not in text]
    problems += [("unexpected in the report: " + s) for s in c.get("not_stdout", []) if s in text]
    problems += [("missing derived: " + s) for s in c.get("derived", []) if s not in derived]
    if "derived_count" in c and len(derived) != c["derived_count"]:
        problems.append("derived relations: %d, expected %d" % (len(derived), c["derived_count"]))
    return problems, text


# ---- random networks against an exact ground truth ----
REL = ["before", "meets", "overlaps", "starts", "during", "finishes", "equals", "after", "metBy", "overlappedBy", "startedBy", "contains", "finishedBy"]
PAIRS = [(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)]


def relation(i, j):
    a, b = i; c, d = j
    if b < c: return 0
    if b == c: return 1
    if d < a: return 7
    if d == a: return 8
    if a == c and b == d: return 6
    if a == c: return 3 if b < d else 10
    if b == d: return 5 if a > c else 12
    if c < a and b < d: return 4
    if a < c and d < b: return 11
    if a < c < b < d: return 2
    return 9


def achievable():
    """all complete relation labellings of four intervals: enumerate the weak orders of the eight end points"""
    out = set()
    def rec(i, used, cur):
        if i == 8:
            yield tuple(cur); return
        for v in range(used + 1):
            cur.append(v); yield from rec(i + 1, used + 1 if v == used else used, cur); cur.pop()
    seen = set()
    for canon in rec(0, 0, []):
        k = max(canon) + 1
        for perm in itertools.permutations(range(k)): seen.add(tuple(perm[c] for c in canon))
    for w in seen:
        iv = [(w[0], w[1]), (w[2], w[3]), (w[4], w[5]), (w[6], w[7])]
        if any(a >= b for a, b in iv): continue
        out.add(tuple(relation(iv[i], iv[j]) for i, j in PAIRS))
    return out


def random_networks(konclude, n):
    ach = achievable()
    rnd = random.Random(2026)
    agree = wrong = 0
    incons = 0
    for _ in range(n):
        k = rnd.randint(3, 6)
        chosen = rnd.sample(range(6), k)
        asg = {c: rnd.randrange(13) for c in chosen}
        truth = any(all(lab[c] == r for c, r in asg.items()) for lab in ach)
        incons += (not truth)
        ttl = ["@prefix t: <http://example.org/time#> .", "@prefix ex: <%s> ." % EX]
        for c, r in asg.items():
            i, j = PAIRS[c]
            ttl.append("ex:i%d t:%s ex:i%d ." % (i, REL[r], j))
        fd, path = tempfile.mkstemp(suffix=".ttl"); os.close(fd)
        open(path, "w").write("\n".join(ttl) + "\n")
        res = run(konclude, ["-i", path])
        os.remove(path)
        verdict = "Interval result: consistent" in res.stdout + res.stderr
        if verdict == truth: agree += 1
        else: wrong += 1
    return agree, wrong, incons


def main():
    if len(sys.argv) < 2:
        print(__doc__); return 2
    konclude = sys.argv[1]
    n_random = int(sys.argv[sys.argv.index("--random") + 1]) if "--random" in sys.argv else 200
    failed = 0
    for c in CASES:
        problems, text = run_case(konclude, c)
        print("%s  %s" % ("PASS" if not problems else "FAIL", c["name"]))
        for p in problems: print("        " + p)
        if problems:
            failed += 1
            print("        --- report ---\n" + "\n".join("        " + l for l in text.splitlines() if not l.startswith("{info}")))
    if n_random > 0:
        agree, wrong, incons = random_networks(konclude, n_random)
        ok = wrong == 0
        print("%s  random qualitative networks of four intervals against an exact ground truth: %d networks (%d inconsistent), verdicts agree in %d, differ in %d" % ("PASS" if ok else "FAIL", n_random, incons, agree, wrong))
        failed += (not ok)
    print("\n%d of %d checks passed" % (len(CASES) + (1 if n_random > 0 else 0) - failed, len(CASES) + (1 if n_random > 0 else 0)))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
