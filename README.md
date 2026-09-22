# Konclude

A modified build of [Konclude](https://github.com/konclude/Konclude), the OWL 2 DL tableau reasoner, adding a `materialize` command that writes out the **complete** materialized ABox — transitively-closed class/object-/data-property hierarchies, class assertions, object/data property assertions, and same-individual equivalences — computed entirely by Konclude's own tableau reasoning. No SPARQL involved anywhere in the pipeline.

## Downloads

| Platform | Where to get it |
|---|---|
| Linux (single file) | `Konclude-x86_64.AppImage`, attached to this repo's [Releases](../../releases) |
| Windows | Already in this repo — `Binaries/` + `materialize.bat` (`git clone` and go) |
| Docker | `docker build -t konclude .` (build locally from this repo) |
| Docker (prebuilt pull) | coming soon |
| macOS | coming soon |


## Quick start (Linux)

The portable build (`Konclude-Linux-x64-portable.tar.gz`) and the single-file AppImage both read/write Turtle, RDF/XML, N-Triples, OWL2-XML, and OWL2-Functional natively — format is auto-detected on both input and output, no conversion step needed.

```bash
# portable tarball
tar xzf Konclude-Linux-x64-portable.tar.gz
cd Konclude-Linux-x64-portable
./materialize.sh input.ttl output.ttl

# or the single-file AppImage
chmod +x Konclude-x86_64.AppImage
./Konclude-x86_64.AppImage input.ttl output.ttl
# if your machine has no FUSE (common on servers/containers):
./Konclude-x86_64.AppImage --appimage-extract-and-run input.ttl output.ttl
```

Every wrapper already runs with `-w AUTO` (see "Thread count" below) — this is enough for most uses. **Two optional** arguments, only if you need them:

- **`-anon`** — by default, facts touching a blank node (anonymous individual) are computed correctly internally but left out of the written output; only entailments between named individuals get written. Adding `-anon` also writes the blank nodes' own facts to the file. Leave it off unless you specifically need to see/use the blank nodes themselves:
  ```bash
  ./materialize.sh input.ttl output.ttl -anon
  ./Konclude-x86_64.AppImage input.ttl output.ttl -anon
  ```
- **Thread count** — both wrappers hardcode `-w AUTO` (scale to every CPU core available) for convenience. To control this yourself, call the underlying binary directly instead of the wrapper, e.g. `-w 1` for single-threaded, or `-w 4` for a fixed count:
  ```bash
  ./Konclude materialize -w 1 -i input.ttl -o output.ttl
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

**Linux / Docker native-Turtle path** (input parsed directly from RDF via Redland): the same `owl:hasKey` gap, plus two more entailments not detected — `owl:InverseFunctionalProperty` and qualified cardinality restrictions don't trigger the expected `owl:sameAs` merge. Unlike the parser-level gaps below, these two look like a reasoning-kernel issue rather than a triples→axiom mapping one: the mapper builds the `InverseFunctionalObjectProperty`/`ObjectMaxCardinality`(`Qualified`) axioms correctly and unconditionally (verified by direct inspection and instrumentation of `CConcreteOntologyRedlandTriplesDataExpressionMapper`), but the resulting same-individual merge is flaky under `-w AUTO` (passes on some runs, fails on others) and the isolated `inverse-functional-property` construct alone reliably *hangs* the reasoner during precomputation (`-w 1` and `-w AUTO` both) rather than producing a wrong answer — a non-termination bug in the SROIQ kernel's precomputation/realization step, not something fixable in the RDF parser.
- **Fixed**: `owl:sameAs`/`owl:differentFrom` merging for direct (non-`owl:AllDifferent`) triples — e.g. the `same-individual` construct and the `differentFrom`-vs-`sameAs` inconsistency construct — and `owl:NegativePropertyAssertion` reification (the `negative-property-assertion` inconsistency construct). Root cause: `CConcreteOntologyRedlandTriplesDataExpressionMapper::buildSimpleABoxAxioms()` — the only code path that turned direct `owl:sameAs`/`owl:differentFrom` triples into `SameIndividual`/`DifferentIndividuals` axioms — is gated by `mConfExtractSimpleABoxAssertions`, which defaults `false` and is never set `true` for the CLI `materialize`/`consistency` commands (confirmed at runtime), so it silently never ran; ordinary ABox assertions (class/object-property/data-property) still worked because they're populated through a separate, always-on mechanism. Separately, `owl:NegativePropertyAssertion` reification (`owl:sourceIndividual`/`owl:assertionProperty`/`owl:targetIndividual`/`owl:targetValue`) had its four successor-predicate filters initialized in the wrong order (`owl:assertionProperty`/`owl:targetIndividual`/`owl:targetValue` rotated by one) *and* had the object-property/data-property branches swapped (the literal-`targetValue` branch built an object-property axiom using the object-property hash, and the individual-`targetIndividual` branch built a data-property axiom using the data-property hash) — so the reification was never matched. Fixed by moving the `owl:sameAs`/`owl:differentFrom` collection into the always-on `buildSeparateNodeBasedAxioms()` (alongside the structurally similar `owl:AllDifferent`/`owl:NegativePropertyAssertion` handling) and correcting the predicate-filter/property-type-hash mismatches. 25-27/28 entailment + 5/9 inconsistency constructs now pass on this path depending on `-w` (was 23/28 + 3/9); no regressions among the previously-passing constructs.

Also specific to the native-Turtle path: certain `rdfs:subPropertyOf` axioms between data properties fail to parse (`Couldn't extract minimal required 2 DataProperty-Expressions... Couldn't match parameters for 'SubDataPropertyOf'-Expression`), confirmed on real-world data — the same ontology parses and reasons cleanly when given as OWL2-XML instead of Turtle. If you hit this, converting to OWL2-XML first (see "Working with Turtle on Windows" above) is the current workaround.

Numeric literals beyond 64-bit range (e.g. very large physical constants) are silently dropped, on both paths.

- **Fixed**: `owl:AsymmetricProperty` / `owl:IrreflexiveProperty` / `owl:propertyDisjointWith` violations weren't detected on either path (a kernel-level gap, not parser-specific) — `CCalculationTableauApproximationSaturationTaskHandleAlgorithm::initializeRoleAssertions()` only ran these clash checks inside the branch handling an already-initialized neighbour saturation node; for two named individuals that reference each other (the exact shape of `r(a,b)` + `r(b,a)`, or a role's own self-assertion `r(a,a)`), each one's neighbour node can still be unresolved — neither initialized nor equal to the current node — when the other's assertion is processed, so the check sat in a branch that was silently skipped for precisely the inputs it needed to catch. Fixed by moving the checks out of that branch entirely, to run unconditionally on the individuals' raw ABox assertions (`nominalIndi`/`othIndi`), matching the pattern already used successfully by the neighbouring equivalent/disjoint-role checks in the same function — no dependency on saturation-node state or processing order. All three constructs now correctly report inconsistent (confirmed via InferTest, `-w AUTO`, no regressions in the other 6 inconsistency constructs or the 28 entailment constructs).

Separately (found while verifying the fix above): the `consistency`/`classification`/`materialize` commands can hang indefinitely during preprocessing when run with `-w 1` on small, standalone ontologies — reproduced on a fully unmodified build, so it predates and is unrelated to the fix above. `-w AUTO` (or any thread count above 1) does not hit it. Root cause not yet identified (looks like a startup synchronization issue specific to single-threaded mode); until it's fixed, avoid `-w 1` for `consistency`/`classification` on small inputs.

## Licensing

Konclude is free software: you can redistribute it and/or modify it under the terms of version 3 of the GNU Lesser General Public License (LGPLv3) as published by the Free Software Foundation. See `GPL.txt` / `LGPL-3.0.txt`.

Konclude uses the following libraries:
- Qt 5.15 (https://www.qt.io/download), released under LGPLv3.
- Redland RDF Libraries (http://librdf.org/), released under LGPL 2.1 (or newer) / GPL 2 / Apache 2 — used only for parsing/writing RDF serializations (Turtle/RDF-XML/N-Triples) on Linux, never for its SPARQL/Rasqal query engine.
