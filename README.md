# Konclude

A modified build of [Konclude](https://github.com/konclude/Konclude), the OWL 2 DL tableau reasoner, adding a `materialize` command that writes out the **complete** materialized ABox — transitively-closed class/object-/data-property hierarchies, class assertions, object/data property assertions, and same-individual equivalences — computed entirely by Konclude's own tableau reasoning. No SPARQL involved anywhere in the pipeline.

## Downloads

Prebuilt binaries for every platform are attached to this repo's [Releases](../../releases):

| Platform | What to grab | Requirements |
|---|---|---|
| Windows | `Konclude-Windows-x64.zip` | none — self-contained |
| Linux | `Konclude-Linux-x64-portable.tar.gz` | none — bundles all its shared libraries |
| Linux (single file) | `Konclude-x86_64.AppImage` | `chmod +x` and run; no unzip |
| macOS | `Konclude-macOS-x64.zip` | none — self-contained |
| Any platform | `docker pull ghcr.io/<owner>/konclude` | Docker |

`.github/workflows/build-konclude.yml` builds all of these (native Windows/Linux/macOS binaries plus the Docker image) automatically on every push to `Source/`, and publishes the Docker image to `ghcr.io/<owner>/konclude` — or trigger it manually from the Actions tab.


## Quick start (Linux)

The portable build (`Konclude-Linux-x64-portable.tar.gz`) and the single-file AppImage both read/write Turtle, RDF/XML, N-Triples, OWL2-XML, and OWL2-Functional natively — format is auto-detected on both input and output, no conversion step needed.

```bash
# portable tarball
tar xzf Konclude-Linux-x64-portable.tar.gz
cd Konclude-Linux-x64-portable
./materialize.sh input.ttl output.ttl
./materialize.sh -selftest

# or the single-file AppImage
chmod +x Konclude-x86_64.AppImage
./Konclude-x86_64.AppImage input.ttl output.ttl
# if your machine has no FUSE (common on servers/containers):
./Konclude-x86_64.AppImage --appimage-extract-and-run input.ttl output.ttl
```


## Quick start (Windows)

`materialize.bat` wraps the Windows build (`Binaries/`) with a simple interface:

```bat
materialize.bat -infertest
materialize.bat input.owl.xml output.owl.xml
materialize.bat input.owl.xml output.owl.xml -anon
```

- **Input/output format**: OWL2-XML or OWL2-Functional syntax only (`.owl.xml`, `.ofn`) — **not** Turtle/RDF-XML/N-Triples. The Windows build has no Redland integration, so it can't parse or write plain RDF directly. If your data is Turtle, either convert it first (see "Working with Turtle on Windows" below) or use the Linux/Docker build, which reads/writes Turtle natively.
- **`-anon`**: by default, facts touching a blank node (anonymous individual) are computed correctly internally but left out of the written output — only entailments between named individuals get written. `-anon` also writes the blank nodes' own facts (their types, property values, any assertion where one is the subject or object) to the file, tagged `<AnonymousIndividual nodeID="...">`. Leave it off unless you specifically need to see/use the blank nodes themselves.
- **`-infertest`**: the thorough check — every construct [InferTest](https://github.com/ISE-FIZKarlsruhe/InferTest) defines (28 entailment + 9 inconsistency), not just one. Needs InferTest cloned as a sibling of this repo (`git clone https://github.com/ISE-FIZKarlsruhe/InferTest.git` next to this repo) plus Python + `pip install rdflib`; the converters below are compiled automatically on first run. Reports **regressions only** — the small set of already-diagnosed Konclude limitations (see below) are expected to fail every time; anything else failing means something actually broke. Exit code 0 = no regressions.

### Working with Turtle on Windows

`tools/` has two small OWL API–based Java converters (`ConvertToOWLXML`, `ConvertToRDFXML`) plus a bundled, verified-working `openllet.jar` (Maven Central's own OWL API/openllet "distribution" jars turned out to be missing several of their own dependencies — this one is confirmed to actually work). `-infertest` above uses these automatically; to convert your own Turtle data by hand:

```bat
tools\setup.bat

set CP=tools\openllet.jar;tools
java -cp %CP% ConvertToOWLXML  your-data.ttl        temp-input.owl.xml
materialize.bat temp-input.owl.xml temp-output.owl.xml
java -cp %CP% ConvertToRDFXML   temp-output.owl.xml   your-result.rdf.xml
```

(run from the repo root; adjust paths if running from elsewhere)


## Running it anywhere: Docker

`Dockerfile` builds a small, fully self-contained image — the same image runs identically on Windows, Linux, and macOS, since Docker carries its own Qt/Redland runtime with it rather than relying on whatever's installed on the host. Reads/writes Turtle, RDF/XML, N-Triples, OWL2-XML, and OWL2-Functional natively.

```bash
docker pull ghcr.io/<owner>/konclude
docker run --rm -v "$(pwd):/data" ghcr.io/<owner>/konclude materialize -w AUTO -i /data/your-ontology.ttl -o /data/output.ttl

# or build locally instead of pulling
docker build -t konclude .
docker run --rm -v "$(pwd):/data" konclude materialize -w AUTO -i /data/your-ontology.ttl -o /data/output.ttl
```

## Building from source

- `KoncludeWithoutRedland.pro` — Windows/macOS build, no Redland (OWL2-XML/Functional only)
- `KoncludeRedlandLinux.pro` — Linux build with Redland linked (native Turtle/RDF-XML/N-Triples), what the Docker image and Linux binaries are built from
- `Source/` — the modified source (LGPLv3, same license as upstream Konclude)

```bash
qmake KoncludeWithoutRedland.pro   # or KoncludeRedlandLinux.pro on Linux
make        # or nmake on Windows
```

See `.github/workflows/build-konclude.yml` for the exact toolchain/flags each platform needs (e.g. Windows needs an older MSVC toolset — Qt 5.15.2's headers use an MSVC STL helper Microsoft has since removed from newer toolsets).

## Known limitations

Checked against [InferTest](https://github.com/ISE-FIZKarlsruhe/InferTest)'s full suite (28 entailment + 9 inconsistency constructs). Results differ by platform, because Windows and Linux/Docker take input through two entirely different parsers:

**Windows / OWL2-XML path** (input converted via the OWL API–based `openllet.jar` first): only one gap — `owl:hasKey` with a datatype-property key is not enforced.

**Linux / Docker native-Turtle path** (input parsed directly from RDF via Redland): the same `owl:hasKey` gap, plus five more entailments/violations not detected — `owl:InverseFunctionalProperty`, qualified cardinality restrictions, and `owl:sameAs` merging from `same-individual`-style constructs aren't entailed; `owl:differentFrom` and negative property assertion violations aren't detected. All five share something in common: each has an unusually indirect RDF/Turtle encoding (RDF-list-encoded keys, blank-node restriction structures, reification-style assertions) — the Redland-based Turtle→OWL-axiom mapping doesn't appear to fully reconstruct them from raw triples before reasoning starts. This is a parser-level gap, separate from the reasoning/materialization logic itself (everything else — 23/28 entailment constructs and 3/9 inconsistency constructs on this path — is correct).

Both paths also share: `owl:AsymmetricProperty` / `owl:IrreflexiveProperty` / `owl:propertyDisjointWith` violations aren't detected (a kernel-level gap, not parser-specific), and numeric literals beyond 64-bit range (e.g. very large physical constants) are silently dropped.

## Licensing

Konclude is free software: you can redistribute it and/or modify it under the terms of version 3 of the GNU Lesser General Public License (LGPLv3) as published by the Free Software Foundation. See `GPL.txt` / `LGPL-3.0.txt`.

Konclude uses the following libraries:
- Qt 5.15 (https://www.qt.io/download), released under LGPLv3.
- Redland RDF Libraries (http://librdf.org/), released under LGPL 2.1 (or newer) / GPL 2 / Apache 2 — used only for parsing/writing RDF serializations (Turtle/RDF-XML/N-Triples) on Linux, never for its SPARQL/Rasqal query engine.
