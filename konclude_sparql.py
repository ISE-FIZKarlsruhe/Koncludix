import sys
import os
import subprocess
import time
import re
from rdflib import Graph, URIRef, Literal
from rdflib.namespace import RDF, RDFS, OWL
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
import xml.etree.ElementTree as ET


# ---------------------------------------------------------
# URI extraction (legacy fallback)
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

    prefix = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
"""

    os.makedirs(tmpfolder, exist_ok=True)

    queries = {
        "classes": "\n".join(
            f'SELECT (IRI("{c}") as ?class) ?superclass WHERE {{ <{c}> rdfs:subClassOf ?superclass . }}'
            for c in classes
        ),
        "oprops": "\n".join(
            f'SELECT ?s (IRI("{op}") as ?op) ?o WHERE {{ ?s <{op}> ?o . }}'
            for op in op_properties
        ),
        "dprops": "\n".join(
            f'SELECT ?s (IRI("{dp}") as ?dp) ?val WHERE {{ ?s <{dp}> ?val . }}'
            for dp in dp_properties
        ),
        "osubprops": "\n".join(
            f'SELECT (IRI("{op}") as ?op) ?superop WHERE {{ <{op}> rdfs:subPropertyOf ?superop . }}'
            for op in op_properties
        ),
        "class_assertions": """
SELECT ?s ?type WHERE {
    ?s rdf:type ?type .
    FILTER(isIRI(?type))
}
"""
    }

    for name, q in queries.items():
        with open(os.path.join(tmpfolder, f"{name}.sparql"), "w") as f:
            f.write(prefix + q)

    print("[PRE] done")
    return classes, op_properties, dp_properties, inverse_map


# ---------------------------------------------------------
# RUN KONCLUDE
# ---------------------------------------------------------
def run_one_job(binary, input_file, tmpfolder, job):
    cmd = [
        binary, "sparqlfile",
        "-s", os.path.join(tmpfolder, f"{job}.sparql"),
        "-o", os.path.join(tmpfolder, f"{job}.xml"),
        "-i", input_file
    ]
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
    return job


def run_jobs(binary, input_file, tmpfolder):
    print("[RUN] executing konclude...")

    jobs = ["classes", "oprops", "dprops", "osubprops", "class_assertions"]

    start = time.time()

    with ThreadPoolExecutor(max_workers=len(jobs)) as executor:
        futures = [
            executor.submit(run_one_job, binary, input_file, tmpfolder, j)
            for j in jobs
        ]

        for f in as_completed(futures):
            print(f"[DONE] {f.result()}")

    print(f"[RUN] finished in {time.time() - start:.2f}s\n")


# ---------------------------------------------------------
# SAFE RESULT BLOCK EXTRACTION
# ---------------------------------------------------------
def extract_result_blocks(content: str):
    return re.findall(r'<result>.*?</result>', content, re.DOTALL)


# ---------------------------------------------------------
# SAFE BINDING PARSER
# ---------------------------------------------------------
def parse_binding(block, name):
    try:
        root = ET.fromstring(f"<root>{block}</root>")

        for b in root.findall(".//binding"):
            if b.attrib.get("name") != name:
                continue

            uri = b.find(".//uri")
            if uri is not None and uri.text:
                return ("URI", uri.text.strip())

            lit = b.find(".//literal")
            if lit is not None:
                return ("LITERAL",
                        lit.text or "",
                        lit.attrib.get("datatype"))

    except Exception:
        return None

    return None


# ---------------------------------------------------------
# PARSE XML FILES (ROBUST)
# ---------------------------------------------------------
def parse_multi_xml_results(file_path):
    if not os.path.exists(file_path):
        return []

    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    blocks = extract_result_blocks(content)
    results = []

    for block in blocks:
        row = {}

        s = parse_binding(block, "s")
        p = (parse_binding(block, "dp") or
             parse_binding(block, "op") or
             parse_binding(block, "ap") or
             parse_binding(block, "type"))
        o = (parse_binding(block, "o") or
             parse_binding(block, "val"))

        if s: row["s"] = s
        if p: row["p"] = p
        if o: row["o"] = o

        if row:
            results.append(row)

    return results


# ---------------------------------------------------------
# CONVERT TO RDF
# ---------------------------------------------------------
def to_rdf(v):
    if not v:
        return None

    if v[0] == "URI":
        return URIRef(v[1])

    text = v[1]
    dtype = v[2] if len(v) > 2 else None

    if dtype and dtype.startswith("http"):
        return Literal(text, datatype=URIRef(dtype))

    return Literal(text)


# ---------------------------------------------------------
# POSTPROCESS
# ---------------------------------------------------------
def postprocess(outfile, tmp, classes, op_props, dp_props, inverse_map):
    print("[POST] building graph...")

    g = Graph()

    for p in op_props:
        g.add((URIRef(p), RDF.type, OWL.ObjectProperty))
    for p in dp_props:
        g.add((URIRef(p), RDF.type, OWL.DatatypeProperty))
    for p, q in inverse_map.items():
        g.add((URIRef(p), OWL.inverseOf, URIRef(q)))

    # ---------------------------
    # DATA PROPERTIES (FIXED)
    # ---------------------------
    for res in parse_multi_xml_results(os.path.join(tmp, "dprops.xml")):
        s, p, o = res.get("s"), res.get("p"), res.get("o")
        if s and p and o:
            g.add((to_rdf(s), to_rdf(p), to_rdf(o)))

    # ---------------------------
    # OBJECT PROPERTIES
    # ---------------------------
    for res in parse_multi_xml_results(os.path.join(tmp, "oprops.xml")):
        s, p, o = res.get("s"), res.get("p"), res.get("o")
        if s and p and o:
            g.add((to_rdf(s), to_rdf(p), to_rdf(o)))

    # ---------------------------
    # CLASS ASSERTIONS (UNCHANGED)
    # ---------------------------
    for res in parse_multi_xml_results(os.path.join(tmp, "class_assertions.xml")):
        s, t = res.get("s"), res.get("p")
        if s and t:
            g.add((to_rdf(s), RDF.type, to_rdf(t)))

    # ---------------------------
    # REALISATION
    # ---------------------------
    real_file = os.path.join(tmp, "realisation.owl")
    if os.path.exists(real_file):
        try:
            tree = ET.parse(real_file)
            for ca in tree.getroot().findall(".//{http://www.w3.org/2002/07/owl#}ClassAssertion"):
                cls = ca.find(".//{*}Class")
                ind = ca.find(".//{*}NamedIndividual")
                if cls is not None and ind is not None:
                    g.add((URIRef(ind.attrib.get("IRI")),
                           RDF.type,
                           URIRef(cls.attrib.get("IRI"))))
        except:
            pass

    # ---------------------------
    # HIERARCHY
    # ---------------------------
    for file, s_n, t_n, rel in [
        ("classes.xml", "class", "superclass", RDFS.subClassOf),
        ("osubprops.xml", "op", "superop", RDFS.subPropertyOf)
    ]:
        pairs = set()
        for res in parse_multi_xml_results(os.path.join(tmp, file)):
            s, t = res.get(s_n), res.get(t_n)
            if s and t:
                pairs.add((s[1], t[1]))

        graph = defaultdict(set)
        nodes = set()

        for a, b in pairs:
            graph[a].add(b)
            nodes.update([a, b])

        for n in nodes:
            vis = set()
            q = deque([n])

            while q:
                x = q.popleft()
                for y in graph[x]:
                    if y not in vis:
                        vis.add(y)
                        q.append(y)
                        g.add((URIRef(n), rel, URIRef(y)))

    print("[POST] total triples:", len(g))
    g.serialize(outfile, format="turtle")


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
def koncludix(binary, input_file, output_file, work_dir):
    start = time.time()

    classes, ops, dps, inv = preprocess(input_file, work_dir)

    run_jobs(binary, input_file, work_dir)

    postprocess(output_file, work_dir, classes, ops, dps, inv)

    print(f"\nTOTAL TIME: {time.time() - start:.2f}s")


# ---------------------------------------------------------
# ENTRY
# ---------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python script.py <konclude_bin> <input.owl> <output.ttl>")
    else:
        koncludix(sys.argv[1], sys.argv[2], sys.argv[3], "tmp")
