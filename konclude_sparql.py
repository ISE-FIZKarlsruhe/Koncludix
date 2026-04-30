import sys
import os
import subprocess
import time
from rdflib import Graph, URIRef, Literal
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

    print("[PRE] done")
    return classes, op_properties, dp_properties, inverse_map


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

    jobs = ["classes", "oprops", "dprops", "osubprops"]

    start = time.time()

    with ThreadPoolExecutor(max_workers=len(jobs)) as executor:
        futures = [
            executor.submit(run_one_job, binary, input_file, tmpfolder, job)
            for job in jobs
        ]

        for f in as_completed(futures):
            print(f"[DONE] {f.result()}")

    print(f"[RUN] finished in {time.time() - start:.2f}s\n")


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
# POSTPROCESS (FIXED DATA PROPERTY PARSER)
# ---------------------------------------------------------
def postprocess(outfile, tmp, classes, op_properties, dp_properties, inverse_map):
    print("[POST] building graph...")

    g = Graph()

    # Add Schema definitions
    for c in classes:
        g.add((URIRef(c), RDF.type, OWL.Class))
    for op in op_properties:
        g.add((URIRef(op), RDF.type, OWL.ObjectProperty))
    for dp in dp_properties:
        g.add((URIRef(dp), RDF.type, OWL.DatatypeProperty))
    for p, q in inverse_map.items():
        g.add((URIRef(p), OWL.inverseOf, URIRef(q)))

    # ---------------------------------------------------------
    # OBJECT PROPERTIES
    # ---------------------------------------------------------
    oprops_file = os.path.join(tmp, "oprops.xml")
    if os.path.exists(oprops_file):
        with open(oprops_file, encoding='utf-8') as f:
            s = op = None
            for line in f:
                if 'binding name="s"' in line:
                    s = extract_uri(line)
                elif 'binding name="op"' in line:
                    op = extract_uri(line)
                elif 'binding name="o"' in line:
                    o = extract_uri(line)
                    if s and op and o:
                        g.add((URIRef(s), URIRef(op), URIRef(o)))

    # ---------------------------------------------------------
    # DATA PROPERTIES (FIXED: Handling Multi-root and Datatypes)
    # ---------------------------------------------------------
    dprops_file = os.path.join(tmp, "dprops.xml")

    if os.path.exists(dprops_file):
        print("[POST] parsing data properties safely with datatypes...")

        # 1. Clean the file (Remove multiple XML headers)
        with open(dprops_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        clean_content = "".join([line for line in lines if "<?xml" not in line])
        wrapped_xml = f"<root>{clean_content}</root>"
        
        # 2. Parse the combined XML string
        root = ET.fromstring(wrapped_xml)

        # 3. Helper to capture both the value and the datatype attribute
        def get_binding_info(result_node, name):
            for b in result_node.findall(".//{*}binding"):
                if b.attrib.get("name") == name:
                    # Check for URIs
                    uri_node = b.find(".//{*}uri")
                    if uri_node is not None and uri_node.text:
                        return uri_node.text.strip(), "URI"
                    
                    # Check for Literals and their datatypes
                    lit_node = b.find(".//{*}literal")
                    if lit_node is not None:
                        val = lit_node.text.strip() if lit_node.text else ""
                        dtype = lit_node.attrib.get("datatype")
                        return val, dtype
            return None, None

        # 4. Iterate through results and add to graph
        for result in root.findall(".//{*}result"):
            s_val, _ = get_binding_info(result, "s")
            dp_val, _ = get_binding_info(result, "dp")
            val_text, val_dtype = get_binding_info(result, "val")

            if s_val and dp_val and val_text is not None:
                s_uri = URIRef(s_val)
                dp_uri = URIRef(dp_val)
                
                if val_dtype == "URI":
                    g.add((s_uri, dp_uri, URIRef(val_text)))
                elif val_dtype:
                    # This preserves things like xsd:anyURI or xsd:integer
                    g.add((s_uri, dp_uri, Literal(val_text, datatype=URIRef(val_dtype))))
                else:
                    g.add((s_uri, dp_uri, Literal(val_text)))

    # ---------------------------------------------------------
    # CLASSES & SUBPROPERTIES (Hierarchy)
    # ---------------------------------------------------------
    subclass_pairs = parse_hierarchy(os.path.join(tmp, "classes.xml"), "class", "superclass")
    for s, t in compute_closure(subclass_pairs):
        g.add((URIRef(s), RDFS.subClassOf, URIRef(t)))

    osub_pairs = parse_hierarchy(os.path.join(tmp, "osubprops.xml"), "op", "superop")
    for s, t in compute_closure(osub_pairs):
        g.add((URIRef(s), RDFS.subPropertyOf, URIRef(t)))

    print("[POST] writing output...")
    g.serialize(outfile, format="turtle")
# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
def koncludix(binary, input_file, output_file, work_dir):
    start = time.time()

    classes, op_properties, dp_properties, inverse_map = preprocess(
        input_file, work_dir
    )

    run_jobs(binary, input_file, work_dir)

    postprocess(
        output_file,
        work_dir,
        classes,
        op_properties,
        dp_properties,
        inverse_map
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
