# Konclude Reasoning Pipeline

```bash
./koncludix_pipeline.sh

Before running the pipeline, you must update the paths inside the script: Konclude binary path, Input ontology file path, Output file locations (if customized)

The pipeline consists of four main stages:

### **Stage 1 — SPARQL Extraction (Konclude)**
Extracts explicit knowledge from the ontology:

- Object property assertions
  
- Data property assertions
  
- Subclass relations
  
- Subproperty relations
  
- Inverse Relations

---

### **Stage 2 — RDF Reconstruction (Python)**
The SPARQL XML results are parsed and reconstructed into an RDF graph using `rdflib`.

This produces a structured Turtle file containing all explicitly asserted triples.

---

### **Stage 3 — Realisation (Konclude Reasoning)**
This step performs OWL reasoning and generates the inferred class assertions ABox. Realisation is intentionally executed as a separate stage because:

-Ensure Completeness

-Performance : Running realisation as a dedicated reasoning step is significantly faster and more stable than attempting to reproduce inference via multiple SPARQL queries.


```bash
Konclude realisation -i input.owl -o output_realisation.ttl
