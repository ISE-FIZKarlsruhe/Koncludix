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

Checked against [InferTest](https://github.com/ISE-FIZKarlsruhe/InferTest): all core OWL 2 constructs pass (class/property hierarchies, class/property assertions, inverses, chains, cardinalities, functional/inverse-functional properties, blank nodes, etc.). A few known gaps remain, all confirmed to be in Konclude's own reasoning/parsing rather than anything added here: `owl:hasKey` with a datatype-property key is not enforced, `owl:AsymmetricProperty` / `owl:IrreflexiveProperty` / `owl:propertyDisjointWith` violations aren't detected either, and numeric literals beyond 64-bit range (e.g. very large physical constants) are silently dropped.

### Quick start (Windows)

`Konclude/materialize.bat` wraps the ready-to-run Windows build (`Konclude/Binaries/`) with a simple interface:

```bat
cd Konclude
materialize.bat -selftest
materialize.bat -infertest
materialize.bat input.owl.xml output.owl.xml
materialize.bat input.owl.xml output.owl.xml -anon
```

- **Input/output format**: OWL2-XML or OWL2-Functional syntax only (`.owl.xml`, `.ofn`) — **not** Turtle/RDF-XML/N-Triples. The Windows build has no Redland integration, so it can't parse or write plain RDF directly. If your data is Turtle, either convert it first (see "Working with Turtle on Windows" below) or use the Docker image, which reads/writes Turtle natively.
- **`-anon`**: by default, facts touching a blank node (anonymous individual) are computed correctly internally but left out of the written output — only entailments between named individuals get written. `-anon` also writes the blank nodes' own facts (their types, property values, any assertion where one is the subject or object) to the file, tagged `<AnonymousIndividual nodeID="...">`. Leave it off unless you specifically need to see/use the blank nodes themselves.
- **`-selftest`**: a ~1-second sanity check — one hardcoded case (a property chain through a blank node). Run it first any time to confirm the build itself launches and reasons at all.
- **`-infertest`**: the thorough check — every construct [InferTest](https://github.com/ISE-FIZKarlsruhe/InferTest) defines (28 entailment + 9 inconsistency), not just one. Needs InferTest cloned as a sibling of this repo (`git clone https://github.com/ISE-FIZKarlsruhe/InferTest.git` next to `Koncludix/`) plus Python + `pip install rdflib`; the converters below are compiled automatically on first run. Reports **regressions only** — the small set of already-diagnosed Konclude kernel gaps (`has-key`, `asymmetric-property`, `disjoint-properties`, `irreflexive-property`) are expected to fail every time and don't count against you; anything else failing means something actually broke. Exit code 0 = no regressions.

#### Working with Turtle on Windows

`Konclude/tools/` has two small OWL API–based Java converters (`ConvertToOWLXML`, `ConvertToRDFXML`) plus a bundled, verified-working `openllet.jar` (Maven Central's own OWL API/openllet "distribution" jars turned out to be missing several of their own dependencies — this one is confirmed to actually work). `-infertest` above uses these automatically; to convert your own Turtle data by hand:

```bat
Konclude\tools\setup.bat

set CP=Konclude\tools\openllet.jar;Konclude\tools
java -cp %CP% ConvertToOWLXML  your-data.ttl        temp-input.owl.xml
Konclude\materialize.bat temp-input.owl.xml temp-output.owl.xml
java -cp %CP% ConvertToRDFXML  temp-output.owl.xml   your-result.rdf.xml
```

(run from the repo root; adjust paths if running from elsewhere)

### Running it anywhere: Docker (recommended)

`Konclude/Dockerfile` builds a small, fully self-contained image — verified locally to build and reason correctly. This is the actual answer to "make it not depend on the environment": the same image runs identically on Windows, Linux, and macOS, since Docker carries its own Qt runtime with it rather than relying on whatever's installed on the host.

```bash
docker build -t konclude -f Konclude/Dockerfile Konclude
docker run --rm -v "$(pwd):/data" konclude materialize -w AUTO -i /data/your-ontology.owl.xml -o /data/output.owl.xml
```

`.github/workflows/build-konclude.yml` also builds and publishes this image to `ghcr.io/<owner>/konclude` automatically on pushes to main (or trigger it manually from the Actions tab).

### Building natively for Linux / macOS

`Konclude/Binaries/Konclude.exe` only runs on Windows. The same workflow also builds native Windows/Linux/macOS binaries as downloadable artifacts. Unlike upstream Konclude's Linux/macOS releases (statically linked, self-contained), these are dynamically linked the same way the Windows build is — so the Linux/macOS binary also needs Qt 5's runtime libraries installed on the machine that runs it (e.g. `sudo apt install libqt5core5a libqt5network5 libqt5xml5 libqt5concurrent5` on Debian/Ubuntu, or `brew install qt@5` on macOS). Unlike the Docker path above, I haven't been able to verify these native Linux/macOS builds myself (no Linux/Mac machine in this environment) — only the Docker build has actually been built and run.

