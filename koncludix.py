# Runs Konclude as a set of SPARQL queries against an ontology (classes,
# object props, data props, subproperties, class assertions) and stitches
# the answers back into one reasoned TTL file.
#
# Two things this version handles that the original didn't:
#  - Konclude segfaults on a handful of specific queries in this ontology.
#    If a query still fails after retrying it on its own, we grab the
#    asserted (not reasoned) version of it straight from the source file
#    with rdflib, instead of just losing that data.
#  - Anonymous individuals (blank nodes) used to get silently dropped,
#    because the old parser only understood named IRIs in Konclude's
#    answers. Now blank nodes are read too, kept per-query so we don't
#    accidentally mix up two different unnamed things.
#
# Usage: python koncludix_fix.py <konclude_binary> <input.owl> <output.ttl>

import sys
import os
import re
import subprocess
import time
from decimal import Decimal
from rdflib import Graph, URIRef, BNode, Literal
from rdflib.namespace import RDF, RDFS, OWL
from collections import defaultdict, deque
import xml.etree.ElementTree as ET

# ---------------------------------------------------------
# URI extraction (kept for SPARQL XML fragments if needed)
# ---------------------------------------------------------
def extract_uri(text: str):
    if "<uri>" not in text:
        return None
    try:
        uri = text.split("<uri>")[1].split("</uri>")[0].strip()
        return uri if "://" in uri else None
    except Exception:
        return None


# ---------------------------------------------------------
# SAFE PARSER FOR MULTI-DOCUMENT KONCLUDE XML
# ---------------------------------------------------------
def load_konclude_multixml(path):
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()

    # Remove all XML declarations
    cleaned = raw.replace("<?xml version=\"1.0\"?>", "")
    cleaned = cleaned.replace("<?xml version=\"1.0\" encoding=\"UTF-8\"?>", "")

    # Wrap in a single root
    wrapped = f"<root>{cleaned}</root>"

    return ET.fromstring(wrapped)


# ---------------------------------------------------------
# BLANK NODE AWARE RESULT PARSING
# ---------------------------------------------------------
# Konclude labels blank nodes "b0", "b1", ... per query it answers, and
# those labels are only meaningful within that one query's own results --
# they get reused across different queries (different Konclude processes)
# without meaning the same node. So we keep a fresh label->BNode map per
# embedded result document instead of one shared map for the whole file,
# otherwise we'd risk stitching two unrelated anonymous individuals
# together just because they both got called "b0".
SPARQL_RESULTS_NS = {"sr": "http://www.w3.org/2005/sparql-results#"}


def iter_result_rows(root):
    docs = root.findall("sr:sparql", SPARQL_RESULTS_NS)
    if not docs:
        docs = [root]
    for doc in docs:
        bnode_map = {}
        for result in doc.findall(".//sr:result", SPARQL_RESULTS_NS):
            yield bnode_map, result


def extract_binding_node(binding, bnode_map):
    """Turn one sr:binding element into a URIRef, BNode, or Literal."""
    ns = SPARQL_RESULTS_NS

    uri_node = binding.find("sr:uri", ns)
    if uri_node is not None:
        text = uri_node.text.strip()
        # Konclude doesn't use the standard <bnode> element for blank
        # nodes -- it reports them as a <uri> whose text is a synthetic
        # "_:..." id instead (e.g. "_:http://konclude.com/test/kb:r1...").
        # Treat that the same as a real bnode rather than wrapping it as
        # a URIRef, which would leak Konclude's internal id as a fake IRI.
        if text.startswith("_:"):
            if text not in bnode_map:
                bnode_map[text] = BNode()
            return bnode_map[text]
        return URIRef(text)

    bnode_node = binding.find("sr:bnode", ns)
    if bnode_node is not None:
        label = bnode_node.text.strip()
        if label not in bnode_map:
            bnode_map[label] = BNode()
        return bnode_map[label]

    lit_node = binding.find("sr:literal", ns)
    if lit_node is not None:
        text = lit_node.text.strip() if lit_node.text else ""
        dtype = lit_node.attrib.get("datatype")
        if dtype and dtype.endswith("#decimal"):
            text = normalize_decimal_text(text)
        return Literal(text, datatype=URIRef(dtype)) if dtype else Literal(text)

    return None


# Konclude sometimes hands back a decimal value as a fraction ("1/2") or
# mixed number ("23 1/2") instead of a normal xsd:decimal lexical form
# ("0.5", "23.5") -- looks like its internal exact-arithmetic
# representation leaking into the answer. Convert it back if we see that
# shape, otherwise leave the text alone.
_MIXED_FRACTION_RE = re.compile(r'^(-?)(\d+)\s+(\d+)/(\d+)$')
_SIMPLE_FRACTION_RE = re.compile(r'^(-?)(\d+)/(\d+)$')


def normalize_decimal_text(text):
    m = _MIXED_FRACTION_RE.match(text)
    if m:
        sign, whole, num, den = m.groups()
        value = Decimal(whole) + Decimal(num) / Decimal(den)
        return str(-value if sign else value)

    m = _SIMPLE_FRACTION_RE.match(text)
    if m:
        sign, num, den = m.groups()
        value = Decimal(num) / Decimal(den)
        return str(-value if sign else value)

    return text


def extract_row(result, bnode_map, names):
    """Pull out the named bindings from one result row as a dict."""
    row = {}
    for b in result.findall("sr:binding", SPARQL_RESULTS_NS):
        name = b.attrib.get("name")
        if name in names:
            row[name] = extract_binding_node(b, bnode_map)
    return row


# ---------------------------------------------------------
# SILENT RUN
# ---------------------------------------------------------
def run_silent(cmd):
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)


# ---------------------------------------------------------
# PREPROCESS
# ---------------------------------------------------------
def preprocess(ontology, tmpfolder):
    print("[PRE] preprocessing...")

    g = Graph()
    g.parse(ontology)

    classes = {str(s) for s in g.subjects(RDF.type, OWL.Class) if isinstance(s, URIRef)}
    op_properties = {str(s) for s in g.subjects(RDF.type, OWL.ObjectProperty) if isinstance(s, URIRef)}
    dp_properties = {str(s) for s in g.subjects(RDF.type, OWL.DatatypeProperty) if isinstance(s, URIRef)}

    inverse_map = {}
    for p, _, q in g.triples((None, OWL.inverseOf, None)):
        if isinstance(p, URIRef) and isinstance(q, URIRef):
            inverse_map[str(p)] = str(q)
            inverse_map[str(q)] = str(p)

    # Asserted rdfs:subPropertyOf pairs for datatype properties, read directly
    # from the source ontology via rdflib. Konclude's SPARQL engine has known
    # issues handling subPropertyOf over datatype properties (crashes on this
    # ontology), and since these relationships are normally asserted rather
    # than something that needs deep reasoning to discover, we bypass
    # Konclude for this and compute transitive closure ourselves.
    dsub_pairs = set()
    for dp in dp_properties:
        for sup in g.objects(URIRef(dp), RDFS.subPropertyOf):
            if isinstance(sup, URIRef):
                dsub_pairs.add((dp, str(sup)))

    prefix = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
"""

    os.makedirs(tmpfolder, exist_ok=True)

    # CLASSES
    with open(os.path.join(tmpfolder, "classes.sparql"), "w") as f:
        f.write(
            prefix + "\n".join(
                f'SELECT (IRI("{c}") as ?class) ?superclass WHERE {{ <{c}> rdfs:subClassOf ?superclass . }}'
                for c in classes
            )
        )

    # OBJECT PROPERTIES
    with open(os.path.join(tmpfolder, "oprops.sparql"), "w") as f:
        f.write(
            prefix + "\n".join(
                f'SELECT ?s (IRI("{op}") as ?op) ?o WHERE {{ ?s <{op}> ?o . }}'
                for op in op_properties
            )
        )

    # DATA PROPERTIES
    with open(os.path.join(tmpfolder, "dprops.sparql"), "w") as f:
        f.write(
            prefix + "\n".join(
                f'SELECT ?s (IRI("{dp}") as ?dp) ?val WHERE {{ ?s <{dp}> ?val . }}'
                for dp in dp_properties
            )
        )

    # OBJECT SUBPROPERTIES
    with open(os.path.join(tmpfolder, "osubprops.sparql"), "w") as f:
        f.write(
            prefix + "\n".join(
                f'SELECT (IRI("{op}") as ?op) ?superop WHERE {{ <{op}> rdfs:subPropertyOf ?superop . }}'
                for op in op_properties
            )
        )

    # CLASS ASSERTIONS (SPARQL)
    with open(os.path.join(tmpfolder, "class_assertions.sparql"), "w") as f:
        f.write(
            prefix + """
SELECT ?s ?type WHERE {
    ?s rdf:type ?type .
    FILTER(isIRI(?type))
}
"""
        )

    print("[PRE] done")
    return classes, op_properties, dp_properties, inverse_map, dsub_pairs, g


# ---------------------------------------------------------
# RUN KONCLUDE
# ---------------------------------------------------------
from concurrent.futures import ThreadPoolExecutor, as_completed


def run_konclude(binary, input_file, sparql_file, output_file):
    """Run a single Konclude sparqlfile job. Returns (success, returncode)."""
    cmd = [
        binary, "sparqlfile",
        "-s", sparql_file,
        "-o", output_file,
        "-i", input_file
    ]
    result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    if (result.returncode != 0):
        print ("Crashed on args: sparqlfile -s ", sparql_file, " -o ", output_file, " -i ", input_file )
    return result.returncode == 0, result.returncode


def run_one_job(binary, input_file, tmpfolder, job):
    sparql_file = os.path.join(tmpfolder, f"{job}.sparql")
    output_file = os.path.join(tmpfolder, f"{job}.xml")

    ok, code = run_konclude(binary, input_file, sparql_file, output_file)

    if ok:
        return job, True, []

    # Batched query crashed/failed. Fall back to one-query-per-line so a
    # single bad IRI or pathological query doesn't lose the whole job.
    print(f"[WARN] {job} failed as a batch (exit {code}). Retrying line-by-line...")

    with open(sparql_file, encoding="utf-8") as f:
        lines = [l.rstrip("\n") for l in f]

    prefix_lines = [l for l in lines if l.strip().startswith("PREFIX")]
    query_lines = [l for l in lines if l.strip().startswith("SELECT")]

    prefix_block = "\n".join(prefix_lines) + "\n" if prefix_lines else ""

    good_results = []  # list of single-query output files that succeeded
    skipped = []

    retry_dir = os.path.join(tmpfolder, f"{job}_retry")
    os.makedirs(retry_dir, exist_ok=True)

    for i, q in enumerate(query_lines):
        single_sparql = os.path.join(retry_dir, f"{i}.sparql")
        single_xml = os.path.join(retry_dir, f"{i}.xml")

        with open(single_sparql, "w", encoding="utf-8") as f:
            f.write(prefix_block + q)

        ok_i, code_i = run_konclude(binary, input_file, single_sparql, single_xml)

        if ok_i:
            good_results.append(single_xml)
        else:
            skipped.append((q, code_i))

    if skipped:
        print(f"[WARN] {job}: skipped {len(skipped)} query line(s) that crashed Konclude individually:")
        for q, code_i in skipped:
            print(f"        exit {code_i}: {q[:160]}")

    # Merge surviving per-query XML fragments into the expected combined output file.
    merged = []
    for path in good_results:
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                merged.append(f.read())

    with open(output_file, "w", encoding="utf-8") as f:
        f.write("\n".join(merged))

    print(f"[INFO] {job}: recovered {len(good_results)}/{len(query_lines)} queries after retry.")
    return job, len(skipped) == 0, skipped


def run_jobs(binary, input_file, tmpfolder):
    print("[RUN] executing konclude (parallel)...")

    jobs = ["classes", "oprops", "dprops", "osubprops", "class_assertions"]

    start = time.time()
    skipped_by_job = {}

    with ThreadPoolExecutor(max_workers=len(jobs)) as executor:
        futures = [
            executor.submit(run_one_job, binary, input_file, tmpfolder, job)
            for job in jobs
        ]

        for f in as_completed(futures):
            job, clean, skipped = f.result()
            skipped_by_job[job] = skipped
            status = "DONE" if clean else "DONE (partial)"
            print(f"[{status}] {job}")

    print(f"[RUN] finished in {time.time() - start:.2f}s\n")
    return skipped_by_job


# ---------------------------------------------------------
# HIERARCHY
# ---------------------------------------------------------
def parse_hierarchy(file, sub_name, sup_name):
    pairs = set()
    if not os.path.exists(file):
        return pairs

    sub = sup = None

    with open(file) as f:
        for line in f:
            if f'binding name="{sub_name}"' in line:
                sub = extract_uri(line)
            elif f'binding name="{sup_name}"' in line:
                sup = extract_uri(line)
                if sub and sup and sub != sup:
                    pairs.add((sub, sup))

    return pairs


def compute_closure(pairs):
    g = defaultdict(set)
    nodes = set()

    for a, b in pairs:
        g[a].add(b)
        nodes.add(a)
        nodes.add(b)

    out = set()

    for n in nodes:
        vis = set()
        q = deque([n])

        while q:
            x = q.popleft()
            for y in g[x]:
                if y not in vis:
                    vis.add(y)
                    q.append(y)
                    out.add((n, y))

    return out


# ---------------------------------------------------------
# RECOVER QUERIES KONCLUDE COULD NEVER ANSWER
# ---------------------------------------------------------
# A query line that still fails after the per-line retry in run_one_job()
# means Konclude cannot answer it at all (e.g. it segfaults on it every
# time), not that it was just a transient batch failure. Rather than
# silently losing that data, read the equivalent triples directly from the
# source ontology via rdflib -- same trade-off already accepted for
# dsub_pairs: reasoner-only inferred values for that one item are lost,
# but the asserted ones survive instead of vanishing outright.

def _extract_iri_from_query(q):
    m = re.search(r'IRI\("([^"]+)"\)', q)
    return m.group(1) if m else None


def recover_skipped_predicate_triples(job, skipped, source_graph, literal_only):
    """For dprops/oprops queries shaped '?s (IRI(X) as ?p) ?val WHERE { ?s <X> ?val }',
    X is the predicate. Recover asserted (subject, X, value) triples for any
    X whose query never came back."""
    recovered = []
    for q, code_i in skipped:
        pred_iri = _extract_iri_from_query(q)
        if not pred_iri:
            continue
        count = 0
        for subj, val in source_graph.subject_objects(URIRef(pred_iri)):
            if not isinstance(subj, (URIRef, BNode)):
                continue
            if isinstance(val, Literal) != literal_only:
                continue
            recovered.append((subj, pred_iri, val))
            count += 1
        print(f"[INFO] {job}: recovered {count} asserted (non-reasoned) triple(s) for "
              f"{pred_iri} directly from source ontology (Konclude could not answer "
              f"this query, exit {code_i}).")
    return recovered


def recover_skipped_hierarchy_pairs(job, skipped, source_graph, fixed_predicate):
    """For classes/osubprops queries shaped '(IRI(X) as ?c) ?y WHERE { <X> fixed_predicate ?y }',
    X is the subject. Recover asserted (X, y) pairs for any X whose query
    never came back."""
    recovered = set()
    for q, code_i in skipped:
        subj_iri = _extract_iri_from_query(q)
        if not subj_iri:
            continue
        count = 0
        for obj in source_graph.objects(URIRef(subj_iri), fixed_predicate):
            if isinstance(obj, URIRef):
                recovered.add((subj_iri, str(obj)))
                count += 1
        print(f"[INFO] {job}: recovered {count} asserted {fixed_predicate.split('#')[-1]} "
              f"pair(s) for {subj_iri} directly from source ontology (Konclude could not "
              f"answer this query, exit {code_i}).")
    return recovered


# ---------------------------------------------------------
# REALISATION PARSER (OWL/XML)
# ---------------------------------------------------------
def parse_realisation_owlxml(file, graph):
    if not os.path.exists(file):
        return

    print("[POST] parsing realisation (OWL/XML)...")

    tree = ET.parse(file)
    root = tree.getroot()

    for ca in root.findall(".//{http://www.w3.org/2002/07/owl#}ClassAssertion"):

        cls = ca.find(".//{*}Class")
        ind = ca.find(".//{*}NamedIndividual")

        if cls is None or ind is None:
            continue

        cls_iri = cls.attrib.get("IRI")
        ind_iri = ind.attrib.get("IRI")

        if cls_iri and ind_iri:
            graph.add((URIRef(ind_iri), RDF.type, URIRef(cls_iri)))


# ---------------------------------------------------------
# POSTPROCESS (FIXED DATA PROPERTY PARSER)
# ---------------------------------------------------------
def postprocess(outfile, tmp, classes, op_properties, dp_properties, inverse_map, dsub_pairs,
                 source_graph=None, skipped_by_job=None):
    print("[POST] building graph...")

    skipped_by_job = skipped_by_job or {}

    g = Graph()

    for op in op_properties:
        g.add((URIRef(op), RDF.type, OWL.ObjectProperty))
    for dp in dp_properties:
        g.add((URIRef(dp), RDF.type, OWL.DatatypeProperty))
    for p, q in inverse_map.items():
        g.add((URIRef(p), OWL.inverseOf, URIRef(q)))

    # OBJECT PROPERTIES (blank-node aware: subject and object can be
    # anonymous individuals, not just named IRIs)
    oprops_file = os.path.join(tmp, "oprops.xml")
    if os.path.exists(oprops_file):
        print("[POST] parsing object properties...")
        root = load_konclude_multixml(oprops_file)
        for bnode_map, result in iter_result_rows(root):
            row = extract_row(result, bnode_map, {"s", "op", "o"})
            if row.get("s") is not None and row.get("op") is not None and row.get("o") is not None:
                g.add((row["s"], row["op"], row["o"]))

    if source_graph is not None:
        for subj, pred, val in recover_skipped_predicate_triples(
                "oprops", skipped_by_job.get("oprops", []), source_graph, literal_only=False
        ):
            g.add((subj, URIRef(pred), val))

    # DATA PROPERTIES (blank-node aware: subject can be an anonymous
    # individual, e.g. a value with a literal attached to it)
    dprops_file = os.path.join(tmp, "dprops.xml")
    data_assertions = []
    if os.path.exists(dprops_file):
        print("[POST] parsing data properties...")

        root = load_konclude_multixml(dprops_file)
        for bnode_map, result in iter_result_rows(root):
            row = extract_row(result, bnode_map, {"s", "dp", "val"})
            if row.get("s") is not None and row.get("dp") is not None and row.get("val") is not None:
                dp_iri = str(row["dp"])
                g.add((row["s"], row["dp"], row["val"]))
                data_assertions.append((row["s"], dp_iri, row["val"]))

    if source_graph is not None:
        for subj, pred, lit in recover_skipped_predicate_triples(
                "dprops", skipped_by_job.get("dprops", []), source_graph, literal_only=True
        ):
            g.add((subj, URIRef(pred), lit))
            data_assertions.append((subj, pred, lit))

    # CLASS ASSERTIONS (blank-node aware: an anonymous individual can be
    # classified too, e.g. one created to satisfy an existential restriction)
    ca_file = os.path.join(tmp, "class_assertions.xml")
    if os.path.exists(ca_file):
        print("[POST] parsing class assertions...")
        root = load_konclude_multixml(ca_file)
        for bnode_map, result in iter_result_rows(root):
            row = extract_row(result, bnode_map, {"s", "type"})
            if row.get("s") is not None and row.get("type") is not None:
                g.add((row["s"], RDF.type, row["type"]))

    # REALISATION
    real_file = os.path.join(tmp, "realisation.owl")
    parse_realisation_owlxml(real_file, g)

    # HIERARCHY
    subclass_pairs = parse_hierarchy(os.path.join(tmp, "classes.xml"), "class", "superclass")
    if source_graph is not None:
        subclass_pairs |= recover_skipped_hierarchy_pairs(
            "classes", skipped_by_job.get("classes", []), source_graph, RDFS.subClassOf
        )
    for s, t in compute_closure(subclass_pairs):
        g.add((URIRef(s), RDFS.subClassOf, URIRef(t)))

    osub_pairs = parse_hierarchy(os.path.join(tmp, "osubprops.xml"), "op", "superop")
    if source_graph is not None:
        osub_pairs |= recover_skipped_hierarchy_pairs(
            "osubprops", skipped_by_job.get("osubprops", []), source_graph, RDFS.subPropertyOf
        )
    for s, t in compute_closure(osub_pairs):
        g.add((URIRef(s), RDFS.subPropertyOf, URIRef(t)))

    # dsub_pairs comes from preprocess() (asserted in the source ontology via
    # rdflib), since Konclude's SPARQL engine crashes on subPropertyOf
    # queries over datatype properties for this ontology.
    for s, t in compute_closure(dsub_pairs):
        g.add((URIRef(s), RDFS.subPropertyOf, URIRef(t)))

    # Materialize inferred datatype property assertions via datatype property hierarchy
    dp_super = defaultdict(set)
    for sub, sup in compute_closure(dsub_pairs):
        dp_super[sub].add(sup)

    for subj, dp, lit in data_assertions:
        for super_dp in dp_super.get(dp, []):
            g.add((subj, URIRef(super_dp), lit))

    print("[POST] writing output...")
    g.serialize(outfile, format="turtle")


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
def koncludix(binary, input_file, output_file, work_dir):
    start = time.time()

    classes, op_properties, dp_properties, inverse_map, dsub_pairs, source_graph = preprocess(
        input_file, work_dir
    )

    skipped_by_job = run_jobs(binary, input_file, work_dir)

    postprocess(
        output_file,
        work_dir,
        classes,
        op_properties,
        dp_properties,
        inverse_map,
        dsub_pairs,
        source_graph=source_graph,
        skipped_by_job=skipped_by_job
    )

    print(f"\nTOTAL TIME: {time.time() - start:.2f}s")


# ---------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python koncludix.py <konclude_binary> <input.owl> <output.ttl>")
    else:
        koncludix(sys.argv[1], sys.argv[2], sys.argv[3], "tmp")
