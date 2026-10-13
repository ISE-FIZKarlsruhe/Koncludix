"""PMD-style process timelines -> OWL (functional syntax) for Konclude, plus an independent ground truth.

Each sample has process steps (intervals with start and end). All Allen relations between the steps of one sample are derived from the timestamps and
asserted as object property assertions (one direction; the inverse follows from the axioms). The theory (all Allen relations as SIMPLE roles, inverse pairs,
asymmetry, irreflexivity, pairwise disjointness) is in `theory()`. Process-order rules of the form "a step of class Later must not start before a step of class Earlier
has ended" are the axioms  Later ⊑ ∀notAfter.¬Earlier  where notAfter covers every relation r(later, earlier) except after and metBy. They work over the class
hierarchy (a rule for HeatTreatment also holds for Quenching) and are what a plain interval network cannot express.
usage: timelines.py OUT.ofn N_SAMPLES VIOLATION_RATE SEED   (writes OUT.ofn and OUT.truth.json)"""
import json, random, sys
sys.path.insert(0, __file__.rsplit("/", 1)[0] if "/" in __file__ else ".")
from allen import REL, INV, relation

NS = "http://pmdstyle/t#"
CLASSES = {"Process": None, "Casting": "Process", "Forging": "Process", "HeatTreatment": "Process", "Quenching": "HeatTreatment", "Annealing": "HeatTreatment",
           "Tempering": "HeatTreatment", "Machining": "Process", "Test": "Process", "TensileTest": "Test", "HardnessTest": "Test"}
# (later, earlier): the earlier class must have ended (before or meets) when the later one starts
RULES = [("Forging", "Casting"), ("HeatTreatment", "Forging"), ("Machining", "HeatTreatment"), ("Test", "Machining"), ("Tempering", "Quenching")]
ALLOWED = {"after", "metBy"}                  # relation(later, earlier) allowed
BASE = ["Casting", "Forging", "Annealing", "Quenching", "Tempering", "Machining", "TensileTest", "HardnessTest"]


def is_a(c, anc):
    while c:
        if c == anc: return True
        c = CLASSES[c]
    return False


def make_sample(rnd, violate):
    steps, t = [], rnd.randint(0, 1000)
    for k, c in enumerate(BASE):
        if c == "Annealing" and rnd.random() < 0.4: continue          # optional step
        gap = rnd.choice([0, 0, 5, 10, 30])
        if c == "HardnessTest" and steps and steps[-1][0] == "TensileTest":
            start = steps[-1][1] + rnd.randint(1, 20)                  # may overlap the tensile test (no rule between tests)
        else:
            start = t + gap
        end = start + rnd.randint(10, 120)
        steps.append((c, start, end)); t = max(t, end)
    if violate:
        pairs = [(l, e) for l, e in RULES if any(is_a(c, l) for c, _, _ in steps) and any(is_a(c, e) for c, _, _ in steps)]
        later, earlier = rnd.choice(pairs)
        li = [i for i, (c, _, _) in enumerate(steps) if is_a(c, later)]
        ei = [i for i, (c, _, _) in enumerate(steps) if is_a(c, earlier)]
        i, j = rnd.choice(li), rnd.choice(ei)
        if i != j:
            c, s, e = steps[i]
            ce, se, ee = steps[j]
            new_s = rnd.randint(se, ee - 1)                            # the later step now starts while the earlier one still runs
            steps[i] = (c, new_s, max(e, new_s + 5))
    return steps


def violations(steps):
    out = []
    for i, (ci, si, ei) in enumerate(steps):
        for j, (cj, sj, ej) in enumerate(steps):
            if i == j: continue
            for later, earlier in RULES:
                if is_a(ci, later) and is_a(cj, earlier) and relation((si, ei), (sj, ej)) not in ALLOWED:
                    out.append((i, j, later, earlier, relation((si, ei), (sj, ej))))
    return out


def theory():
    L = ["Prefix(:=<%s>)" % NS, "Ontology(<http://pmdstyle/theory>"]
    for r in REL: L.append("Declaration(ObjectProperty(:%s))" % r)
    for c in CLASSES: L.append("Declaration(Class(:%s))" % c)
    for a, b in (("before", "after"), ("meets", "metBy"), ("overlaps", "overlappedBy"), ("starts", "startedBy"), ("during", "contains"), ("finishes", "finishedBy")):
        L.append("InverseObjectProperties(:%s :%s)" % (a, b))
    for r in REL:
        if r != "equals": L.append("IrreflexiveObjectProperty(:%s)" % r)
    L.append("SymmetricObjectProperty(:equals)")
    for r in ("before", "meets", "overlaps", "starts", "during", "finishes"): L.append("AsymmetricObjectProperty(:%s)" % r)
    L.append("DisjointObjectProperties(%s)" % " ".join(":" + r for r in REL))
    L.append("Declaration(ObjectProperty(:notAfter))")
    for r in REL:
        if r not in ALLOWED: L.append("SubObjectPropertyOf(:%s :notAfter)" % r)
    for c, p in CLASSES.items():
        if p: L.append("SubClassOf(:%s :%s)" % (c, p))
    for later, earlier in RULES:
        L.append("SubClassOf(:%s ObjectAllValuesFrom(:notAfter ObjectComplementOf(:%s)))" % (later, earlier))
    return L


def abox(samples):
    L = []
    for n, steps in enumerate(samples):
        for i, (c, s, e) in enumerate(steps):
            L.append("ClassAssertion(:%s :s%d_%s)" % (c, n, c))
        for i in range(len(steps)):
            for j in range(i + 1, len(steps)):
                (ci, si, ei), (cj, sj, ej) = steps[i], steps[j]
                L.append("ObjectPropertyAssertion(:%s :s%d_%s :s%d_%s)" % (relation((si, ei), (sj, ej)), n, ci, n, cj))
    return L


if __name__ == "__main__":
    out, n, rate, seed = sys.argv[1], int(sys.argv[2]), float(sys.argv[3]), int(sys.argv[4])
    rnd = random.Random(seed)
    samples = [make_sample(rnd, rnd.random() < rate) for _ in range(n)]
    truth = [i for i, st in enumerate(samples) if violations(st)]
    open(out, "w", newline="\n").write("\n".join(theory() + abox(samples)) + "\n)\n")
    json.dump(dict(samples=n, violating=truth, steps=[st for st in samples[:0]]), open(out + ".truth.json", "w"))
    print("samples %d, violating %d (%.1f %%), axioms+assertions %d" % (n, len(truth), 100.0 * len(truth) / n, len(theory()) + len(abox(samples))))
