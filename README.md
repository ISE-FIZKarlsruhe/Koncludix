# Koncludix

This script runs the Konclude reasoner on an OWL ontology and produces a fully materialized RDF graph in Turtle format.
-subclass axioms (+ closure)
-subproperty axioms (+ closure)
-inverse properties
-object property assertions
-class assertions
-sameAs normalization

---

## 🔧 Requirements

- Python 3.8+
- Konclude reasoner binary
- Python packages:
  - rdflib

Install dependencies:

```bash
pip install rdflib


How to run:
python koncludix_full.py input.owl output.ttl /path/to/Konclude
