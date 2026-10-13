#!/usr/bin/env python3
"""Temporal layer on the real PMD creep-test ontology: one creep test record arrives per period.
Runs tools/temporal/tlayer.py twice on the same timeline - normal (a new Konclude process per period: the terminology is prepared again every time) and --live (one running
Konclude, one knowledge base, only the changes are told) - and compares the answers and the time.
usage: tlayer_live_pmd.py KONCLUDE [N_RECORDS]     data: PMD_DATA (folder with cto_onlytbox.owx and cto_full.owx; default: the PMD workshop files)"""
import json, os, re, subprocess, sys, tempfile, time
import xml.etree.ElementTree as ET

K = sys.argv[1]; N = int(sys.argv[2]) if len(sys.argv) > 2 else 8
DATA = os.environ.get("PMD_DATA", "/mnt/d/Github/PMDco-Reasoning-Workshop/part3-reasoners/data")
HERE = os.path.dirname(os.path.abspath(__file__))
TLAYER = os.path.join(HERE, "..", "..", "tools", "temporal", "tlayer.py")
OWL = "{http://www.w3.org/2002/07/owl#}"
BASE = "https://w3id.org/pmd/cto/"
WINDOW = int(os.environ.get("WINDOW", "0"))      # 0: records stay valid; n: a record is valid for n periods
INFERRED = BASE + "CTO_0000022"         # 'creep test piece chemical composition': its instances are found only through the definition of the class

def full(i): return i if re.match(r"^[A-Za-z][\w+.\-]*:", i) else BASE + i
ENTITY = {"Class", "ObjectProperty", "DataProperty", "NamedIndividual", "Datatype", "AnnotationProperty"}
def fs(el):
    """OWL/XML element -> OWL functional syntax (annotations are left out)"""
    t = el.tag.replace(OWL, "")
    if t in ENTITY:
        if el.get("IRI") is None:         # abbreviated IRI such as xsd:string
            pre, _, local = el.get("abbreviatedIRI").partition(":")
            return "<%s%s>" % ({"xsd": "http://www.w3.org/2001/XMLSchema#", "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#", "rdfs": "http://www.w3.org/2000/01/rdf-schema#", "owl": "http://www.w3.org/2002/07/owl#"}[pre], local)
        return "<%s>" % full(el.get("IRI"))
    if t == "Literal": return '"%s"^^<%s>' % ((el.text or "").replace("\\", "\\\\").replace('"', '\\"'), el.get("datatypeIRI") or "http://www.w3.org/2001/XMLSchema#string")
    if t == "AnonymousIndividual": return "_:" + el.get("nodeID")
    if t == "Declaration": return "Declaration(%s(%s))" % (el[0].tag.replace(OWL, ""), fs(el[0]))
    if t == "FacetRestriction": return "<%s> %s" % (el.get("facet"), fs(el[0]))
    kids = [fs(k) for k in el if k.tag.replace(OWL, "") != "Annotation"]
    card = [el.get("cardinality")] if el.get("cardinality") is not None else []
    return "%s(%s)" % (t, " ".join(card + kids))

TBOX_AXIOMS = {"SubClassOf", "EquivalentClasses", "DisjointClasses", "Declaration", "ObjectPropertyDomain", "ObjectPropertyRange", "SubObjectPropertyOf", "InverseObjectProperties",
               "TransitiveObjectProperty", "SymmetricObjectProperty", "FunctionalObjectProperty", "DisjointUnion", "SubDataPropertyOf", "DataPropertyDomain", "DataPropertyRange",
               "DisjointObjectProperties", "AsymmetricObjectProperty", "ReflexiveObjectProperty", "IrreflexiveObjectProperty", "InverseFunctionalObjectProperty", "FunctionalDataProperty", "EquivalentObjectProperties"}
def tbox_text():
    ax = []
    for _, el in ET.iterparse(os.path.join(DATA, "cto_onlytbox.owx")):
        t = el.tag.replace(OWL, "")
        if t in TBOX_AXIOMS and not (t == "Declaration" and el[0].tag.replace(OWL, "") == "AnnotationProperty"):
            ax.append(fs(el))
        if t in TBOX_AXIOMS or t == "AnnotationAssertion": el.clear()
    return "Prefix(:=<%s>)\nOntology(<%sontology>\n%s\n)\n" % (BASE, BASE, "\n".join(ax)), len(ax)

def facts_text(n):
    """class and object property assertions of the first n creep test records (the record of period k holds from period k on)"""
    recs, lines = {}, []
    items = []
    for _, el in ET.iterparse(os.path.join(DATA, "cto_full.owx")):
        t = el.tag.replace(OWL, "")
        if t in ("ClassAssertion", "ObjectPropertyAssertion"):
            s = ET.tostring(el, encoding="unicode")
            m = re.search(r"Vh5205_C-\d+", s)
            if m: items.append((m.group(), el.tag.replace(OWL, ""), [fs(k) for k in el]))
            el.clear()
    order = sorted({r for r, _, _ in items})[:n]
    for r, t, kids in items:
        if r not in order: continue
        k = order.index(r) + 1
        end = "%d]" % (k + WINDOW - 1) if WINDOW else "inf)"        # WINDOW=3: a record is valid for 3 periods only, so facts end and are retracted
        if t == "ClassAssertion": lines.append("%s(%s) [%d, %s" % (kids[0][1:-1], kids[1][1:-1], k, end))
        else: lines.append("%s(%s, %s) [%d, %s" % (kids[0][1:-1], kids[1][1:-1], kids[2][1:-1], k, end))
    return "\n".join(lines) + "\n", len(order)

work = os.environ.get("WORKDIR") or tempfile.mkdtemp(prefix="tlayer_pmd_")      # WORKDIR keeps the converted files; GEN_ONLY=1 stops after writing them
os.makedirs(work, exist_ok=True)
tb, nax = tbox_text(); fa, nrec = facts_text(N)
open(os.path.join(work, "t.ofn"), "w", encoding="utf-8").write(tb)
open(os.path.join(work, "f.tl"), "w", encoding="utf-8").write(fa)
print("%d terminology axioms, %d records, %d facts; period k brings record k   (files in %s)" % (nax, nrec, fa.count("\n"), work))
if os.environ.get("GEN_ONLY"): sys.exit(0)

def run(extra, name):
    out = os.path.join(work, name + ".json")
    cmd = [sys.executable, TLAYER, "--tbox", os.path.join(work, "t.ofn"), "--facts", os.path.join(work, "f.tl"), "--konclude", K, "-o", out,
           "--formula", "F %s(?x)" % INFERRED] + ([] if os.environ.get("OPEN") else ["--validation"]) + extra       # OPEN=1: open-world mode (entailed negations are used); default: validation mode
    t0 = time.time()
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)
    dt = time.time() - t0
    if not os.path.exists(out): print(r.stdout[-1500:], r.stderr[-1500:]); sys.exit("no result for " + name)
    d = json.load(open(out))
    note = [l for l in r.stdout.splitlines() if l.startswith("Live mode") or l.startswith("Reasoning")]
    return dt, d, note

t_norm, d_norm, n_norm = run([], "normal")
print("normal: %.1f s   %s" % (t_norm, " | ".join(n_norm)))
t_live, d_live, n_live = run(["--live"], "live")
print("live:   %.1f s   %s" % (t_live, " | ".join(n_live)))
same = d_norm["formulas"] == d_live["formulas"] and d_norm["states"] == d_live["states"]
res = d_norm["formulas"][0]["results"]
print("same answers: %s   (%d individuals checked; %d TRUE)" % ("YES" if same else "NO  <-- DIFFERENT", len(res), sum(1 for r in res if r["result"] == "TRUE")))
print("time per period: normal %.1f s, live %.1f s;  total speed-up %.1fx" % (t_norm / nrec, t_live / nrec, t_norm / t_live))
