# Konclude Reasoning Pipeline


./koncludix_pipeline.sh



The pipeline consists of four main stages:

### **Stage 1 — SPARQL Extraction (Konclude)**
Extracts explicit knowledge from the ontology:
- Object property assertions
- Subclass relations
- Subproperty relations
- Inverse Relations

---

### **Stage 2 — RDF Reconstruction (Python)**
The SPARQL XML results are parsed and reconstructed into an RDF graph using `rdflib`.

This produces a structured Turtle file containing all explicitly asserted triples.

---

### **Stage 3 — Realisation (Konclude Reasoning)**
Runs OWL reasoning using Konclude: (because this way it only takes a few seconds) 

```bash
Konclude realisation -i input.owl -o output_realisation.ttl
