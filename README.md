# Konclude Reasoning Pipeline


Install dependencies: pip install rdflib

Usage: python koncludix.py <konclude_binary> <input.owl> <output.ttl>

The code extracts 

- Object property assertions
  
- Data property assertions
  
- Subclass relations
  
- Subproperty relations
  
- Inverse Relations
  
- Class Assertions

## Konclude/

A modified, worked-upon build of [Konclude](https://github.com/konclude/Konclude), our OWL 2 DL reasoner. It adds a new `materialize` command that writes out the full materialized ABox — class hierarchy, object/data property hierarchies, class assertions, object/data property assertions, and same-individual equivalences — computed entirely by Konclude's own tableau reasoning, no SPARQL involved.

- `Konclude/Binaries/` — ready-to-run build (`Konclude.bat materialize -w AUTO -i <ontology> -o <output>`)
- `Konclude/Source/` — the modified source (LGPLv3, same license as upstream Konclude)

Checked against [InferTest](https://github.com/ISE-FIZKarlsruhe/InferTest): all core OWL 2 constructs pass (class/property hierarchies, class/property assertions, inverses, chains, cardinalities, functional/inverse-functional properties, etc.). One known gap remains: `owl:hasKey` with a datatype-property key is not enforced.

