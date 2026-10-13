#!/usr/bin/env python3
"""What does one period cost when the terminology is large (the real PMD Creep Testing Ontology, about 3,100 axioms)?
Three ways to reason about period k (the data of creep test k arrives in period k):
  fresh_component   a new Konclude process with the terminology and only the new record          (what tools/temporal/tlayer.py does per changed group today)
  fresh_whole       a new Konclude process with the terminology and all records so far
  revisions         ONE knowledge base: Tell the terminology and record 1, then Tell record 2, 3, ... (OWLlink revisions); the cost of period k is T_k - T_(k-1)
Each run asks for the instances of 'creep testing process' (this forces the reasoner to prepare the whole knowledge base).
usage: tbox_cost.py KONCLUDE [N_RECORDS]   data: the PMD workshop files (PMD_DATA environment variable: folder with cto_onlytbox.owx and cto_full.owx)"""
import os, re, subprocess, sys, tempfile, time
import xml.etree.ElementTree as ET

K = sys.argv[1]; N = int(sys.argv[2]) if len(sys.argv) > 2 else 12
DATA = os.environ.get("PMD_DATA", "/mnt/d/Github/PMDco-Reasoning-Workshop/part3-reasoners/data")
OWL = "http://www.w3.org/2002/07/owl#"
ET.register_namespace("owl", OWL)
KB = "http://pmd/kb"; TEST = "https://w3id.org/pmd/co/PMD_0000589"
INFERRED = "CTO_0000022"      # 'creep test piece chemical composition' (the terminology file writes its IRIs relative to xml:base, and they are sent as they are); its instances are found only through the definition of the class

def axioms(path, keep=lambda s: True):
    out = []
    for ev, el in ET.iterparse(path):
        if el.tag.startswith("{%s}" % OWL) and el.tag.split("}")[1] in ("SubClassOf", "EquivalentClasses", "DisjointClasses", "Declaration", "ObjectPropertyDomain", "ObjectPropertyRange", "SubObjectPropertyOf",
                "InverseObjectProperties", "TransitiveObjectProperty", "SymmetricObjectProperty", "FunctionalObjectProperty", "DisjointUnion", "SubDataPropertyOf", "DataPropertyDomain", "DataPropertyRange",
                "ClassAssertion", "ObjectPropertyAssertion", "DataPropertyAssertion", "DisjointObjectProperties", "AsymmetricObjectProperty", "ReflexiveObjectProperty", "IrreflexiveObjectProperty", "InverseFunctionalObjectProperty", "FunctionalDataProperty"):
            s = ET.tostring(el, encoding="unicode")
            if keep(s): out.append(s)
            el.clear()
    return out

print("reading the terminology and the data ...")
tbox = axioms(os.path.join(DATA, "cto_onlytbox.owx"))
full = axioms(os.path.join(DATA, "cto_full.owx"), keep=lambda s: "Vh5205_C-" in s)
recs = sorted({m.group() for s in full for m in re.finditer(r"Vh5205_C-\d+", s)})[:N]
byrec = {r: [s for s in full if (r + "-") in s or s.endswith(r)] for r in recs}
# (an axiom that mentions a record belongs to it; shared axioms do not exist in this data: every IRI of a record carries its name)
print("%d terminology axioms, %d records with %s axioms each" % (len(tbox), len(recs), sorted(len(v) for v in byrec.values())[len(recs) // 2]))

HEAD = '<?xml version="1.0" encoding="UTF-8"?>\n<RequestMessage xmlns="http://www.owllink.org/owllink-xml#" xmlns:owl="%s">\n<CreateKB kb="%s"/>\n' % (OWL, KB)
Q = "".join('<GetFlattenedInstances kb="%s"><owl:Class IRI="%s"/></GetFlattenedInstances>\n' % (KB, c) for c in (TEST, INFERRED))
def tell(ax): return '<Tell kb="%s">\n%s\n</Tell>\n' % (KB, "\n".join(ax))
CONFIG = os.environ.get("REVISION_CONFIG", "")        # an OWLlink Set file for the revision runs, for example experiments/incremental/incr_cfg_on.xml
SKIP_FRESH = bool(os.environ.get("SKIP_FRESH"))
def run(body, config=""):
    d = tempfile.mkdtemp(); rq = os.path.join(d, "q.xml")
    open(rq, "w", encoding="utf-8").write(HEAD + body + "</RequestMessage>\n")
    t0 = time.time()
    r = subprocess.run([K, "owllinkfile", "-w", "1"] + (["-c", config] if config else []) + ["-i", rq, "-o", os.path.join(d, "r.xml")], capture_output=True, text=True, env=dict(os.environ, KONCLUDE_SUPERVISED="1"), timeout=1800)
    resp = open(os.path.join(d, "r.xml")).read() if os.path.exists(os.path.join(d, "r.xml")) else ""
    sets = [el for el in ET.fromstring(resp).iter() if el.tag.endswith("}SetOfIndividuals")] if resp else []
    return time.time() - t0, [frozenset(i.get("IRI") or i.get("abbreviatedIRI") for i in s) for s in sets]       # one answer per query, in order

print("\n%3s %17s %15s %22s %15s" % ("k", "fresh_component s", "fresh_whole s", "revisions: total s", "increment s"))
prev = 0.0
for k in range(1, len(recs) + 1):
    tc, nc = (0, []) if SKIP_FRESH else run(tell(tbox + byrec[recs[k - 1]]) + Q)
    tw, nw = (0, []) if SKIP_FRESH else run(tell(tbox + sum((byrec[r] for r in recs[:k]), [])) + Q)
    body = tell(tbox + byrec[recs[0]]) + Q + "".join(tell(byrec[r]) + Q for r in recs[1:k])
    tr, nr = run(body, CONFIG)
    last = nr[-2:]                                        # the answers to the two questions after the last Tell
    same = "n/a" if SKIP_FRESH else ("YES" if nw[-2:] == last and len(last) == 2 else "NO  <-- DIFFERENT")
    print("%3d %17.2f %15.2f %22.2f %15.2f   instances: creep testing process %s, inferred composition %s; same names as a fresh load: %s"
          % (k, tc, tw, tr, tr - prev, len(last[0]) if last else "?", len(last[1]) if len(last) > 1 else "?", same))
    prev = tr
