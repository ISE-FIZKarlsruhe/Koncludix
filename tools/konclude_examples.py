#!/usr/bin/env python3
"""
konclude_examples.py - find nice examples of reasoning in an ontology.

  python3 tools/konclude_examples.py KONCLUDE ONTOLOGY [--classes "Heat treatment,Creep test"] [--top 10] [--min-axioms 3] [--out examples.md]

What it does:
  1. `Konclude materialize` gives everything that follows (class hierarchy and the types of every individual).
  2. Conclusions that you did not write down are the candidates (conclusions that only follow a chain of asserted SubClassOf axioms are boring and left out
     unless --include-chains). With --classes only conclusions about those classes are looked at.
  3. For the candidates `Konclude explain` finds the smallest set of axioms that makes the conclusion follow (a justification).
  4. The examples are ranked: more axioms and a wider mix of kinds of axioms (definitions, restrictions, inverse or transitive properties, cardinalities,
     disjointness, enumerations) rank higher than a plain chain. The best ones are written out in plain English, using the labels of your ontology.

Formats: OWL functional syntax (.ofn), OWL/XML (.owx, .xml), and RDF formats (.ttl, .rdf, .owl, .nt) if the Python package rdflib is installed.
"""
import argparse, collections, concurrent.futures, itertools, json, os, random, re, subprocess, sys, tempfile, time
import xml.etree.ElementTree as ET

OWL = "http://www.w3.org/2002/07/owl#"; RDFS = "http://www.w3.org/2000/01/rdf-schema#"; RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
XSD = "http://www.w3.org/2001/XMLSchema#"; SKOS = "http://www.w3.org/2004/02/skos/core#"
THING, NOTHING = OWL + "Thing", OWL + "Nothing"
LABEL_PROPS = (RDFS + "label", SKOS + "prefLabel")

# ---------------------------------------------------------------- functional syntax: parser

class Iri(str): pass
class Lit(str): pass

TOKEN = re.compile(r'\s*(<[^>]*>|"(?:[^"\\]|\\.)*"(?:\^\^<[^>]*>|\^\^[^\s()]+|@[A-Za-z-]+)?|\(|\)|[^\s()<>"]+)')

def tokenize(text):
    text = re.sub(r"(?m)^\s*#.*$", "", text)
    return [m.group(1) for m in TOKEN.finditer(text)]

def parse_fs(text, prefixes=None):
    """Parses OWL functional syntax into nested tuples (head, [args]); atoms are Iri, Lit or plain strings."""
    prefixes = {} if prefixes is None else prefixes
    toks = tokenize(text); pos = [0]
    def atom(t):
        if t.startswith("<"): return Iri(t[1:-1])
        if t.startswith('"'): return Lit(t)
        if ":" in t and not t.endswith(":="):
            p, local = t.split(":", 1)
            if p in prefixes: return Iri(prefixes[p] + local)
        return t
    def node():
        t = toks[pos[0]]; pos[0] += 1
        if pos[0] < len(toks) and toks[pos[0]] == "(":
            pos[0] += 1; args = []
            while toks[pos[0]] != ")": args.append(node())
            pos[0] += 1
            if t == "Prefix" and len(args) >= 2:
                a0 = args[0]
                if isinstance(a0, str) and a0.endswith(":="): prefixes[a0[:-2]] = str(args[1])
                return (t, args)
            return (t, args)
        return atom(t)
    out = []
    while pos[0] < len(toks): out.append(node())
    return out

# ---------------------------------------------------------------- what the ontology asserts

class Asserted:
    def __init__(self):
        self.sub = collections.defaultdict(set)       # named subclass edges (also named equivalences, both ways)
        self.types = collections.defaultdict(set)     # asserted named types of individuals
        self.labels = {}                              # the label shown (English if there is one)
        self.all_labels = collections.defaultdict(list)   # every label of every language, for matching names
        self.label_lang = {}
        self.defined = set()                          # classes with a complex definition (EquivalentClasses with an expression)
        self.complex_sub = set()                      # classes with a SubClassOf to a complex expression
        self.ok = False

def load_asserted(path):
    a = Asserted(); ext = os.path.splitext(path)[1].lower()
    try:
        if ext in (".ofn", ".fss", ".ofs"):
            load_ofn(open(path, encoding="utf-8", errors="replace").read(), a)
        elif ext in (".owx", ".xml") or open(path, encoding="utf-8", errors="replace").read(2000).lstrip().startswith("<?xml") and "owl" in open(path, encoding="utf-8", errors="replace").read(4000) and "rdf:RDF" not in open(path, encoding="utf-8", errors="replace").read(4000):
            load_owx(path, a)
        else:
            load_rdf(path, a)
        a.ok = True
    except Exception as e:
        print("   (note: could not read the asserted axioms: %s; every candidate will be explained and chains are recognised afterwards)" % e, file=sys.stderr)
    return a

def add_label(a, iri, text, lang):
    iri = str(iri)
    a.all_labels[iri].append(text)
    lang = (lang or "").lower()
    if iri not in a.labels or (lang.startswith("en") and not a.label_lang.get(iri, "").startswith("en")):
        a.labels[iri] = text; a.label_lang[iri] = lang

def named(x): return isinstance(x, Iri)

def load_ofn(text, a):
    prefixes = {"owl": OWL, "rdfs": RDFS, "rdf": RDF, "xsd": XSD, "skos": SKOS}
    for top in parse_fs(text, prefixes):
        if isinstance(top, tuple) and top[0] == "Ontology":
            for ax in top[1]: add_ofn_axiom(ax, a)

def add_ofn_axiom(ax, a):
    if not isinstance(ax, tuple): return
    h, args = ax
    if h == "SubClassOf" and len(args) == 2:
        if named(args[0]) and named(args[1]): a.sub[args[0]].add(args[1])
        elif named(args[0]): a.complex_sub.add(args[0])
    elif h == "EquivalentClasses":
        nm = [x for x in args if named(x)]
        for x, y in itertools.permutations(nm, 2): a.sub[x].add(y)
        for x in nm:
            if any(not named(y) for y in args): a.defined.add(x)
    elif h == "ClassAssertion" and len(args) == 2 and named(args[0]) and named(args[1]): a.types[args[1]].add(args[0])
    elif h == "AnnotationAssertion" and len(args) >= 3 and args[0] in LABEL_PROPS and named(args[1]) and isinstance(args[2], Lit):
        m = re.match(r'"((?:[^"\\]|\\.)*)"(?:@([A-Za-z-]+))?', args[2])
        if m: add_label(a, args[1], m.group(1), m.group(2))

def load_owx(path, a):
    ns = "{" + OWL + "}"
    def iri(el): return el.get("IRI") if el is not None else None
    for _, el in ET.iterparse(path):
        t = el.tag.replace(ns, "")
        kids = list(el)
        if t == "SubClassOf" and len(kids) == 2:
            x, y = kids
            if x.tag == ns + "Class" and y.tag == ns + "Class": a.sub[iri(x)].add(iri(y))
            elif x.tag == ns + "Class": a.complex_sub.add(iri(x))
        elif t == "EquivalentClasses":
            nm = [iri(k) for k in kids if k.tag == ns + "Class"]
            for x, y in itertools.permutations(nm, 2): a.sub[x].add(y)
            if len(nm) < len(kids):
                for x in nm: a.defined.add(x)
        elif t == "ClassAssertion" and len(kids) == 2 and kids[0].tag == ns + "Class" and kids[1].tag == ns + "NamedIndividual":
            a.types[iri(kids[1])].add(iri(kids[0]))
        elif t == "AnnotationAssertion" and len(kids) == 3:
            prop = iri(kids[0]); lit = kids[2]
            if prop in LABEL_PROPS and kids[1].get("IRI") and lit.tag == ns + "Literal" and lit.text:
                add_label(a, kids[1].get("IRI"), lit.text, lit.get("{http://www.w3.org/XML/1998/namespace}lang"))
        if t in ("SubClassOf", "EquivalentClasses", "ClassAssertion", "AnnotationAssertion"): el.clear()

def load_rdf(path, a):
    import rdflib
    from rdflib.namespace import RDF as R, RDFS as RS, OWL as O
    g = rdflib.Graph(); g.parse(path)
    skip = {str(O.Class), str(O.NamedIndividual), str(O.ObjectProperty), str(O.DatatypeProperty), str(O.AnnotationProperty), str(O.Ontology), str(O.Restriction), str(RS.Class), str(O.Thing)}
    for s, p, o in g:
        if not isinstance(s, rdflib.URIRef): continue
        ps = str(p)
        if p == RS.subClassOf:
            if isinstance(o, rdflib.URIRef): a.sub[str(s)].add(str(o))
            else: a.complex_sub.add(str(s))
        elif p == O.equivalentClass:
            if isinstance(o, rdflib.URIRef): a.sub[str(s)].add(str(o)); a.sub[str(o)].add(str(s))
            else: a.defined.add(str(s))
        elif p == R.type and isinstance(o, rdflib.URIRef) and str(o) not in skip: a.types[str(s)].add(str(o))
        elif ps in LABEL_PROPS and isinstance(o, rdflib.Literal): add_label(a, s, str(o), o.language)

def local(iri):
    return re.split(r"[#/]", iri.rstrip("/"))[-1] or iri

# ---------------------------------------------------------------- Konclude

def run(cmd, timeout):
    env = dict(os.environ, KONCLUDE_SUPERVISED="1")
    return subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=timeout)

def materialize(konclude, ont, work, timeout):
    cfg = os.path.join(work, "mat.xml"); out = os.path.join(work, "mat.owx")
    open(cfg, "w").write('<?xml version="1.0" encoding="UTF-8"?>\n<RequestMessage xmlns="http://www.owllink.org/owllink-xml#">\n'
                         '<Set key="Konclude.CLI.Materialize.PropertyHierarchy"><Literal>false</Literal></Set>\n'
                         '<Set key="Konclude.CLI.Materialize.ObjectPropertyAssertions"><Literal>false</Literal></Set>\n'
                         '<Set key="Konclude.CLI.Materialize.DataPropertyAssertions"><Literal>false</Literal></Set>\n</RequestMessage>\n')
    r = run([konclude, "materialize", "-w", "1", "-c", cfg, "-i", ont, "-o", out], timeout)
    # a file that does not end in .ofn/.owx is first tried as XML, which prints "XML parsing error at 1:1" before the right parser takes over: that line is not a failure
    real_errors = [l for l in (r.stdout + r.stderr).splitlines() if "{error}" in l and "XML parsing error at 1:1" not in l]
    if real_errors or not os.path.exists(out): raise RuntimeError("materialize failed:\n" + (r.stdout + r.stderr)[-600:])
    ns = "{" + OWL + "}"
    sub, types, equiv = set(), collections.defaultdict(set), []
    for _, el in ET.iterparse(out):
        t = el.tag.replace(ns, ""); kids = list(el)
        if t == "SubClassOf" and len(kids) == 2 and kids[0].tag == ns + "Class" and kids[1].tag == ns + "Class": sub.add((kids[0].get("IRI"), kids[1].get("IRI")))
        elif t == "EquivalentClasses": equiv.append([k.get("IRI") for k in kids if k.tag == ns + "Class"])
        elif t == "ClassAssertion" and len(kids) == 2 and kids[0].tag == ns + "Class" and kids[1].tag == ns + "NamedIndividual": types[kids[1].get("IRI")].add(kids[0].get("IRI"))
        if t in ("SubClassOf", "EquivalentClasses", "ClassAssertion"): el.clear()
    return sub, types, equiv

EXPLAIN_HEAD = re.compile(r"entails the statement because of these (\d+) axiom")
def explain(konclude, ont, kind, x, y, timeout):
    args = ["--subclass", "<%s>" % x, "<%s>" % y] if kind == "subclass" else ["--type", "<%s>" % x, "<%s>" % y]
    t0 = time.time()
    try:
        r = run([konclude, "explain", "-i", ont] + args, timeout)
    except subprocess.TimeoutExpired:
        return None
    out = r.stdout + r.stderr
    m = EXPLAIN_HEAD.search(out)
    if not m: return None
    axs = []
    for ln in out.splitlines():
        if ln.startswith("  ") and ln.strip() and not ln.startswith("  ("): axs.append(ln.strip())
    calls = re.search(r"minimal; (\d+) Konclude calls", out)
    return {"axioms": axs, "calls": int(calls.group(1)) if calls else None, "seconds": round(time.time() - t0, 1)}

# ---------------------------------------------------------------- verbalisation

def article(word):
    return "an" if word[:1].lower() in "aeiou" else "a"

class Verbal:
    def __init__(self, labels): self.labels = labels
    def gen(self, iri):
        l = local(str(iri))
        return re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", l.replace("_", " "))
    def cls(self, iri):          # a class: the label as written, or the local name split into lower-case words
        lab = self.labels.get(str(iri))
        return lab if lab else self.gen(iri).lower()
    def pname(self, iri):        # a property
        lab = self.labels.get(str(iri))
        return lab if lab else self.gen(iri).lower()
    def name(self, iri):         # an individual (or anything else): as written
        lab = self.labels.get(str(iri))
        return lab if lab else local(str(iri))
    def ce(self, x):
        if isinstance(x, Iri): return self.cls(x)
        if not isinstance(x, tuple): return str(x)
        h, a = x
        if h == "ObjectIntersectionOf": return " and ".join(self.ce(i) for i in a)
        if h == "ObjectUnionOf": return " or ".join(self.ce(i) for i in a)
        if h == "ObjectComplementOf": return "not " + self.ce(a[0])
        if h == "ObjectSomeValuesFrom": return "something that %s some %s" % (self.prop(a[0]), self.ce(a[1]))
        if h == "ObjectAllValuesFrom": return "something that %s only %s" % (self.prop(a[0]), self.ce(a[1]))
        if h == "ObjectHasValue": return "something that %s %s" % (self.prop(a[0]), self.name(a[1]))
        if h in ("ObjectMinCardinality", "ObjectMaxCardinality", "ObjectExactCardinality"):
            word = {"ObjectMinCardinality": "at least", "ObjectMaxCardinality": "at most", "ObjectExactCardinality": "exactly"}[h]
            filler = (" " + self.ce(a[2])) if len(a) > 2 else ""
            return "something that %s %s %s%s" % (self.prop(a[1]), word, a[0], filler)
        if h == "ObjectOneOf": return "one of {%s}" % ", ".join(self.name(i) for i in a)
        if h == "ObjectHasSelf": return "something that %s itself" % self.prop(a[0])
        return "%s(%s)" % (h, ", ".join(self.ce(i) for i in a))
    def prop(self, p):
        if isinstance(p, tuple) and p[0] == "ObjectInverseOf": return "is %s of" % self.pname(p[1][0])
        return self.pname(p)
    def a_class(self, x):        # "a student", "something that attends at least 2 course"
        t = self.ce(x)
        return ("%s %s" % (article(t), t)) if isinstance(x, Iri) else t
    def axiom(self, ax):
        if not isinstance(ax, tuple): return str(ax)
        h, a = ax
        n, pn = self.name, self.pname
        try:
            if h == "SubClassOf": return "Every %s is %s." % (self.ce(a[0]), self.a_class(a[1]))
            if h == "EquivalentClasses": return "%s is defined as: %s." % (self.a_class(a[0])[0].upper() + self.a_class(a[0])[1:], " = ".join(self.a_class(i) for i in a[1:]))
            if h == "DisjointClasses": return "Nothing is both %s." % " and ".join(self.a_class(i) for i in a)
            if h == "DisjointUnion": return "%s is exactly one of: %s (and these do not overlap)." % (self.a_class(a[0]), ", ".join(self.ce(i) for i in a[1:]))
            if h == "ClassAssertion": return "%s is %s." % (n(a[1]), self.a_class(a[0]))
            if h == "ObjectPropertyAssertion": return "%s %s %s." % (n(a[1]), self.prop(a[0]), n(a[2]))
            if h == "DataPropertyAssertion": return "%s has %s = %s." % (n(a[1]), pn(a[0]), a[2])
            if h == "InverseObjectProperties": return "'%s' and '%s' are inverse: x %s y means y %s x." % (pn(a[0]), pn(a[1]), pn(a[0]), pn(a[1]))
            if h == "ObjectPropertyDomain": return "Anything that %s something is %s." % (self.prop(a[0]), self.a_class(a[1]))
            if h == "ObjectPropertyRange": return "Anything that something %s is %s." % (self.prop(a[0]), self.a_class(a[1]))
            if h == "TransitiveObjectProperty": return "'%s' is transitive: x %s y and y %s z give x %s z." % (pn(a[0]), pn(a[0]), pn(a[0]), pn(a[0]))
            if h == "SymmetricObjectProperty": return "'%s' is symmetric." % pn(a[0])
            if h == "AsymmetricObjectProperty": return "'%s' is asymmetric." % pn(a[0])
            if h == "ReflexiveObjectProperty": return "'%s' is reflexive." % pn(a[0])
            if h == "IrreflexiveObjectProperty": return "'%s' is irreflexive." % pn(a[0])
            if h == "FunctionalObjectProperty": return "'%s' is functional: at most one value." % pn(a[0])
            if h == "InverseFunctionalObjectProperty": return "'%s' is inverse functional." % pn(a[0])
            if h == "SubObjectPropertyOf":
                if isinstance(a[0], tuple) and a[0][0] == "ObjectPropertyChain": return "If x %s then x %s." % (" and then ".join("%s something" % pn(p) for p in a[0][1]), self.prop(a[1]))
                return "'%s' implies '%s'." % (self.prop(a[0]), self.prop(a[1]))
            if h == "DifferentIndividuals": return "%s are different individuals." % " and ".join(n(i) for i in a)
            if h == "SameIndividual": return "%s are the same individual." % " and ".join(n(i) for i in a)
        except Exception:
            pass
        return "%s(%s)" % (h, ", ".join(self.ce(i) for i in a))

KINDS = [("definition", ("EquivalentClasses", "DisjointUnion")), ("restriction", ("ObjectSomeValuesFrom", "ObjectAllValuesFrom", "ObjectHasValue", "ObjectHasSelf")),
         ("cardinality", ("ObjectMinCardinality", "ObjectMaxCardinality", "ObjectExactCardinality")), ("enumeration", ("ObjectOneOf",)),
         ("disjointness", ("DisjointClasses", "ObjectComplementOf", "DisjointObjectProperties")),
         ("property", ("InverseObjectProperties", "TransitiveObjectProperty", "SymmetricObjectProperty", "AsymmetricObjectProperty", "ReflexiveObjectProperty", "IrreflexiveObjectProperty",
                       "FunctionalObjectProperty", "InverseFunctionalObjectProperty", "ObjectPropertyDomain", "ObjectPropertyRange", "ObjectPropertyChain", "SubObjectPropertyOf", "EquivalentObjectProperties")),
         ("data", ("DataPropertyAssertion", "DataSomeValuesFrom", "DataHasValue", "DataPropertyDomain", "DataPropertyRange"))]

def contains_head(node, heads):
    if not isinstance(node, tuple): return False
    if node[0] in heads: return True
    return any(contains_head(c, heads) for c in node[1])

def analyse(axiom_lines):
    nodes = []
    for ln in axiom_lines:
        parsed = parse_fs(ln)
        nodes.append(parsed[0] if parsed else ln)
    kinds = set(); facts = rules = 0
    for nd in nodes:
        if not isinstance(nd, tuple): continue
        if nd[0] in ("ClassAssertion", "ObjectPropertyAssertion", "DataPropertyAssertion", "DifferentIndividuals", "SameIndividual"):
            facts += 1
            if nd[0] != "ClassAssertion": kinds.add("fact about a pair")
            if nd[0] == "ClassAssertion" and not isinstance(nd[1][0], Iri): kinds.add("restriction")
            continue
        rules += 1
        found = False
        for k, heads in KINDS:
            if contains_head(nd, heads): kinds.add(k); found = True
        if nd[0] == "SubClassOf" and not found:
            kinds.add("subclass") if all(isinstance(x, Iri) for x in nd[1]) else kinds.add("definition")
    return nodes, kinds, facts, rules

def score(n_axioms, kinds):
    rich = kinds - {"subclass", "fact about a pair"}
    return n_axioms + 3 * len(rich) + (1 if "fact about a pair" in kinds else 0)

# ---------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser(description="find nice examples of reasoning in an ontology")
    ap.add_argument("konclude"); ap.add_argument("ontology")
    ap.add_argument("--classes", help="comma separated class names or labels: only examples that conclude something about these classes")
    ap.add_argument("--involving", action="store_true", help="with --classes: also examples where the class only appears somewhere in the justification (not in the conclusion); needs more explanations (raise --max-explain)")
    ap.add_argument("--with-subclasses", action="store_true", help="also conclusions about the subclasses of those classes")
    ap.add_argument("--kind", default="all", choices=["all", "types", "subclasses"], help="conclusions about individuals (types), about classes (subclasses), or both")
    ap.add_argument("--top", type=int, default=10, help="how many examples to write out")
    ap.add_argument("--min-axioms", type=int, default=3, help="smallest justification that counts as an example")
    ap.add_argument("--include-chains", action="store_true", help="also conclusions that follow plain chains of SubClassOf axioms")
    ap.add_argument("--max-explain", type=int, default=40, help="how many candidates are explained at most")
    ap.add_argument("--jobs", type=int, default=2, help="explanations in parallel")
    ap.add_argument("--timeout", type=int, default=120, help="seconds per explanation")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--no-merge", action="store_true", help="do not merge examples that follow the same pattern (same rules, different individuals)")
    ap.add_argument("--out", help="write the examples as markdown")
    ap.add_argument("--json", help="write the examples as JSON")
    a = ap.parse_args()

    if not os.path.exists(a.ontology): sys.exit("ontology not found: " + a.ontology)
    if not os.path.exists(a.konclude): sys.exit("Konclude binary not found: " + a.konclude)
    work = tempfile.mkdtemp(prefix="examples_")
    print("1. Reading what the ontology asserts ...")
    asserted = load_asserted(a.ontology)
    if asserted.ok: print("   %d class names with asserted superclasses, %d individuals with asserted types, %d labels" % (len(asserted.sub), len(asserted.types), len(asserted.labels)))
    print("2. Computing everything that follows (Konclude materialize) ...")
    sub, types, equiv = materialize(a.konclude, a.ontology, work, 3600)
    all_classes = {x for p in sub for x in p} | {c for ts in types.values() for c in ts}
    unsat = {x for g in equiv if NOTHING in g for x in g if x != NOTHING} | {x for (x, y) in sub if y == NOTHING and x != NOTHING}
    print("   %d subclass conclusions, %d individuals with types, %d unsatisfiable class(es)" % (len(sub), len(types), len(unsat)))
    V = Verbal(asserted.labels)

    # classes of interest
    interest = set()
    if a.classes:
        for tok in [t.strip() for t in a.classes.split(",") if t.strip()]:
            tl = tok.lower()
            names_of = lambda c: [local(c).lower()] + [t.lower() for t in asserted.all_labels.get(c, [])]
            exact = [c for c in all_classes if tok == c or tl in names_of(c)]
            found = exact or [c for c in all_classes if any(tl in n for n in names_of(c))]
            if not found: print("   WARNING: no class matches '%s'" % tok)
            for c in found[:12]: interest.add(c)
            print("   '%s' -> %s" % (tok, ", ".join(V.name(c) for c in found[:6]) + (" ..." if len(found) > 6 else "")))
        if a.with_subclasses:
            interest |= {x for (x, y) in sub if y in interest}
    def wanted_concl(*classes):
        if a.classes: return any(c in interest for c in classes)       # a class filter that matched nothing finds nothing
        return True
    def wanted(*classes):
        return True if (a.involving and a.classes) else wanted_concl(*classes)

    # asserted chains
    def reach(x, y):
        seen, todo = {x}, [x]
        while todo:
            c = todo.pop()
            for d in asserted.sub.get(c, ()):
                if d == y: return True
                if d not in seen: seen.add(d); todo.append(d)
        return False

    cands = []
    if a.kind in ("all", "subclasses"):
        closure = {p for p in sub if p[0] != p[1] and p[1] not in (THING,) and p[0] != NOTHING}
        supers = collections.defaultdict(set)
        for x, y in closure: supers[x].add(y)
        for x, y in closure:
            if not a.include_chains and any(y in supers[m] for m in supers[x] if m != y): continue     # keep only the direct inferred superclasses
            if y == NOTHING or not wanted(x, y): continue
            if y in asserted.sub.get(x, ()): continue
            chain = asserted.ok and reach(x, y)
            cands.append({"kind": "subclass", "x": x, "y": y, "chain": chain, "direct": wanted_concl(x, y), "hint": (2 if (x in asserted.defined or y in asserted.defined) else 0)})
        for x in unsat:
            if wanted(x): cands.append({"kind": "subclass", "x": x, "y": NOTHING, "chain": False, "direct": wanted_concl(x), "hint": 3})
    if a.kind in ("all", "types"):
        super_of = collections.defaultdict(set)
        for x, y in sub: super_of[x].add(y)
        for ind, ts in types.items():
            ts = {t for t in ts if t not in (THING, NOTHING)}
            specific = {t for t in ts if not any(t in super_of[u] and u != t for u in ts)}      # the most specific inferred types
            for c in specific:
                if c in asserted.types.get(ind, ()) or not wanted(c): continue
                chain = asserted.ok and any(c == t0 or reach(t0, c) for t0 in asserted.types.get(ind, ()))
                cands.append({"kind": "type", "x": ind, "y": c, "chain": chain, "direct": wanted_concl(c), "hint": (2 if c in asserted.defined else 0)})
    n_all = len(cands)
    if not a.include_chains: cands = [c for c in cands if not c["chain"]]
    rnd = random.Random(a.seed); rnd.shuffle(cands)
    cands.sort(key=lambda c: (-int(c["direct"] and bool(a.classes)), -c["hint"]))
    chosen = cands[:a.max_explain]
    print("3. %d candidate conclusions (%d after leaving out plain chains); explaining %d ..." % (n_all, len(cands), len(chosen)))

    results = []
    def work_one(c):
        e = explain(a.konclude, a.ontology, "subclass" if c["kind"] == "subclass" else "type", c["x"], c["y"], a.timeout)
        return c, e
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, a.jobs)) as pool:
        for c, e in pool.map(work_one, chosen):
            if not e: continue
            nodes, kinds, facts, rules = analyse(e["axioms"])
            n = len(e["axioms"])
            pure_chain = kinds <= {"subclass"} and facts <= 1
            if pure_chain and not a.include_chains: continue
            if n < a.min_axioms: continue
            if a.classes and not c["direct"] and not any(("<%s>" % i) in " ".join(e["axioms"]) for i in interest): continue
            results.append(dict(c, axioms=e["axioms"], nodes=nodes, kinds=sorted(kinds), n=n, facts=facts, rules=rules, seconds=e["seconds"], score=score(n, kinds)))
    results.sort(key=lambda r: -r["score"])
    FACT_HEADS = ("ClassAssertion", "ObjectPropertyAssertion", "DataPropertyAssertion", "DifferentIndividuals", "SameIndividual")
    if not a.no_merge:
        groups = collections.OrderedDict()
        for r in results:
            rule_axioms = tuple(sorted(ax for ax, nd in zip(r["axioms"], r["nodes"]) if not (isinstance(nd, tuple) and nd[0] in FACT_HEADS)))
            groups.setdefault((r["kind"], r["y"], rule_axioms), []).append(r)
        merged = []
        for g in groups.values():
            best = g[0]; best["similar"] = [V.name(o["x"]) if o["kind"] == "type" else V.cls(o["x"]) for o in g[1:]]
            merged.append(best)
        results = merged
    results = results[:a.top]

    # report
    lines = ["# Examples of reasoning in %s" % os.path.basename(a.ontology), ""]
    if interest: lines += ["Classes of interest: %s" % ", ".join(sorted(V.name(c) for c in interest)[:12]), ""]
    lines += ["%d example(s), ranked by the number of axioms involved and the variety of kinds of axioms. Every justification is minimal: no axiom can be left out." % len(results), ""]
    for i, r in enumerate(results, 1):
        if r["kind"] == "type": concl = "%s is %s." % (V.name(r["x"]), V.a_class(Iri(r["y"]))) if r["y"] != NOTHING else "%s is impossible." % V.name(r["x"])
        else: concl = "Every %s is %s." % (V.cls(r["x"]), V.a_class(Iri(r["y"]))) if r["y"] != NOTHING else "'%s' can have no members (it is unsatisfiable)." % V.cls(r["x"])
        r["conclusion"] = concl
        sim = r.get("similar") or []
        pattern = ("The same pattern also applies to: %s%s." % (", ".join(sim[:8]), (" and %d more" % (len(sim) - 8)) if len(sim) > 8 else "")) if sim else ""
        lines += ["## Example %d: %s" % (i, concl), "", "**%d axioms; kinds: %s; found with %s explanation seconds**" % (r["n"], ", ".join(r["kinds"]) or "plain", r["seconds"]), "",
                  *([pattern, ""] if pattern else []),
                  "This conclusion is not written in the ontology. It follows from:", ""]
        order = sorted(range(len(r["nodes"])), key=lambda k: (isinstance(r["nodes"][k], tuple) and r["nodes"][k][0] in ("ClassAssertion", "ObjectPropertyAssertion", "DataPropertyAssertion", "DifferentIndividuals", "SameIndividual")))
        for step, k in enumerate(order, 1): lines.append("%d. %s  \n   `%s`" % (step, V.axiom(r["nodes"][k]), r["axioms"][k][:260]))
        lines.append("")
    text = "\n".join(lines)
    if a.out: open(a.out, "w", encoding="utf-8").write(text + "\n")
    print("\n" + "\n".join(l for l in lines if not l.startswith("   `")))
    if a.json:
        json.dump([{k: v for k, v in r.items() if k != "nodes"} for r in results], open(a.json, "w"), indent=1, default=str)
    if a.out: print("\nWritten to %s" % a.out)
    return 0

if __name__ == "__main__":
    sys.exit(main())
