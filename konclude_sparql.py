import sys
import os
import subprocess
import time
import xml.etree.ElementTree as ET
from rdflib import Graph, URIRef
from rdflib.namespace import RDF, RDFS, OWL
from collections import defaultdict, deque


# ---------------------------------------------------------
# URI extraction
# ---------------------------------------------------------
def extract_uri(line: str):
    if "<uri>" not in line:
        return None
    try:
        uri = line.split("<uri>")[1].split("</uri>")[0].strip()
    except Exception:
        return None
    return uri if "://" in uri else None


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

    classes = {
        str(s) for s in g.subjects(RDF.type, OWL.Class)
        if isinstance(s, URIRef)
    }

    op_properties = {
        str(s) for s in g.subjects(RDF.type, OWL.ObjectProperty)
        if isinstance(s, URIRef)
    }

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

    # OBJECT SUBPROPERTIES
    with open(os.path.join(tmpfolder, "osubprops.sparql"), "w") as f:
        f.write(
            prefix + "\n".join(
                f'SELECT (IRI("{op}") as ?op) ?superop WHERE {{ <{op}> rdfs:subPropertyOf ?superop . }}'
                for op in op_properties
            )
        )

    print("[PRE] done")

    return classes, op_properties, inverse_map


# ---------------------------------------------------------
# RUN KONCLUDE
# ---------------------------------------------------------
from concurrent.futures import ThreadPoolExecutor, as_completed

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
    print("[RUN] executing konclude (parallel)...")

    jobs = ["classes", "oprops", "osubprops"]

    start = time.time()

    with ThreadPoolExecutor(max_workers=len(jobs)) as executor:
        futures = [
            executor.submit(run_one_job, binary, input_file, tmpfolder, job)
            for job in jobs
        ]

        for f in as_completed(futures):
            job = f.result()
            print(f"[DONE] {job}")

    print(f"[RUN] all jobs finished in {time.time() - start:.2f}s\n")

# ---------------------------------------------------------
# HIERARCHY
# ---------------------------------------------------------
def parse_hierarchy(file, sub_name, sup_name):
    pairs = set()

    if not os.path.exists(file):
        return pairs

    sub = None
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
# POSTPROCESS
# ---------------------------------------------------------
def postprocess(outfile, tmp, classes, op_properties, inverse_map):
    print("[POST] building graph...")

    g = Graph()

    for c in classes:
        g.add((URIRef(c), RDF.type, OWL.Class))

    for op in op_properties:
        g.add((URIRef(op), RDF.type, OWL.ObjectProperty))

    for p, q in inverse_map.items():
        g.add((URIRef(p), OWL.inverseOf, URIRef(q)))

    # OBJECT PROPERTIES
    with open(os.path.join(tmp, "oprops.xml")) as f:
        s = op = None
        for line in f:
            if 'name="s"' in line:
                s = extract_uri(line)
            elif 'name="op"' in line:
                op = extract_uri(line)
            elif 'name="o"' in line:
                o = extract_uri(line)

                if s and op and o:
                    g.add((URIRef(s), URIRef(op), URIRef(o)))

                    if op in inverse_map:
                        g.add((URIRef(o), URIRef(inverse_map[op]), URIRef(s)))

    subclass_pairs = parse_hierarchy(os.path.join(tmp, "classes.xml"), "class", "superclass")
    for s, t in compute_closure(subclass_pairs):
        g.add((URIRef(s), RDFS.subClassOf, URIRef(t)))

    osub_pairs = parse_hierarchy(os.path.join(tmp, "osubprops.xml"), "op", "superop")
    for s, t in compute_closure(osub_pairs):
        g.add((URIRef(s), RDFS.subPropertyOf, URIRef(t)))

    g.serialize(outfile, format="turtle")


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
def koncludix(binary, input_file, output_file, work_dir):
    start = time.time()

    classes, op_properties, inverse_map = preprocess(input_file, work_dir)

    run_jobs(binary, input_file, work_dir)

    postprocess(output_file, work_dir, classes, op_properties, inverse_map)

    print(f"\nTOTAL TIME: {time.time() - start:.2f}s")


# ---------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python koncludix.py <konclude_binary> <input.owl> <output.ttl>")
    else:
        koncludix(
            sys.argv[1],
            sys.argv[2],
            sys.argv[3],
            "tmp"
        )
