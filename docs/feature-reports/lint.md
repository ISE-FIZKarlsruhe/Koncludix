# `Konclude lint` - the lint report built into the binary

## What was added

A new command: `Konclude lint -i ontology.owl [-o report.txt] [-w 1]`.
It parses the ontology with Konclude's own parsers (no reasoning), then prints the same kind of report as the earlier Java prototype
(feature summary, warnings, ABox hints). It needs no Java, OWL API or extra binary. The report goes to standard output; with `-o` it is also written to that file.
Konclude's usual `{info}` log lines are printed too (e.g. `grep -v '^{'` removes them).
The command is also listed in the usage text (`Konclude` without arguments, or `-h`).

Tree: `~/kon_feat_lint` (built with `make -j4`, binary `~/kon_feat_lint/Release/Konclude`). Source of truth for the changes: `~/kon_clean` (unchanged).

## Files

New files (10), all registered in `Konclude.pri`:

| File | Purpose |
|---|---|
| `Source/Control/Interface/CommandLine/CCLILintComandLinePreparationTranslator.{h,cpp}` | recognises the word `lint` (copy of the materialize translator) |
| `Source/Control/Loader/CCLILintBatchProcessingLoader.{h,cpp}` | creates KB, loads the ontology, fetches the loaded revision, runs the analysis, prints the report |
| `Source/Control/Loader/CLintOntologyAnalyser.{h,cpp}` | all checks, the ABox model, the ABox-reduction estimate (port of the colour refinement in `AboxReduce.java`) |
| `Source/Parser/CLintExpressionRenderer.{h,cpp}` | writes expressions/axioms as OWL functional syntax text (for the `e.g.` lines) and collects entity names |
| `Source/Parser/CLintParseRecorder.{h,cpp}` | small global recorder, only switched on by `lint` (see "hasKey" below) |

Changed existing files (small hunks):

* `CCommandLinePreparationTranslatorSelector.{h,cpp}` - one include, one `append`
* `CDefaultLoaderFactory.{h,cpp}` - one include, two `else if` (create / can-create)
* `CCommandLineTranlatorHelpLoader.cpp` - help text lines
* `Parser/FunctionalJAVACC/OWL2FunctionalStyleParser.cpp` (+ the `.jj` grammar, same change) - `hasKey()` now hands the key to the recorder if recording is on; it still returns `nullptr` as before
* `Parser/CXMLOWL2StreamHandler.{h,cpp}` - OWL/XML `HasKey` uses a new jump function that records the key and then calls the old "unsupported" function
* `Konclude.pri` - the 10 new files

Without `lint` the recorder is off (one atomic flag test per HasKey axiom), so other commands behave as before.

## How to apply

```
cd <Konclude source tree>          # the one that has Konclude.pri and Source/
patch -p1 < D:\scratch_logs\feat_lint.patch      (WSL: /mnt/d/scratch_logs/feat_lint.patch)
qmake -o Makefile KoncludeRedlandLinux.pro       # new files are in Konclude.pri, so the Makefile must be regenerated
make -j4
```
`patch -p1 --dry-run` on `~/kon_clean` is clean. Existing files keep their CRLF line endings in the patch.
`Konclude.pro` / `KoncludeWithoutRedland.pro` / `KoncludeLIB.pro` include the same `Konclude.pri`, so they pick the files up too (only the Redland Linux build was built and tested).

## How it works (and what could not be seen)

* Axioms are read from the ontology's told-axiom set (`getBuildData()->getTellAxiomSet()`), sorted by creation serial (= roughly file order), and walked with the expression visitor. Class expressions, cardinalities, `oneOf`, `hasValue`, `hasSelf`, unions, DisjointUnion, inverse/transitive/symmetric axioms are all still visible there.
* **hasKey is not in the ontology at all**: the functional and OWL/XML parsers drop `HasKey`, and the RDF mapper ignores `owl:hasKey`. Therefore: (a) functional syntax and OWL/XML: the parser hooks above record the keys while `lint` runs; (b) RDF (RDF/XML, Turtle, N-Triples): `lint` reads `owl:hasKey` and the `rdf:first/rest` key list directly from the stored triples. For an RDF key whose class is a blank-node class expression only `_:id` is printed.
* Side finding (not changed): the functional-syntax grammar parses the *data property* list of `HasKey` with `objectPropertyExpression()`, i.e. it creates object-property entities for the data key properties. Lint records them as data properties.
* **Simple ABox assertions**: with functional syntax and OWL/XML they are normal axioms. With RDF input Konclude keeps them only as triples, so `lint` reads class/object/data assertions from the triples (object/data property told from the declarations that were mapped; predicates that are not declared as properties are not counted). Complex ABox axioms (negative assertions, sameAs, differentFrom, complex class assertions) are always expressions.
* Large files: DL-20 (138 MB RDF/XML) takes 20 s and 1.7 GB peak; DL-10 11 s / 0.9 GB; DL-5 4 s / 0.4 GB. `-w 1` is not needed (lint does no reasoning).

## What matches the Java tool and what does not

Compared by running both tools on the same files (header counts, feature line, every warning title/detail text, all counts in details, ABox-reduction numbers). Only the order of the `e.g.` lines was ignored (Java prints them in OWL API set order; this tool in file order, first 3 each).

| File | Result |
|---|---|
| my `lint_all` test (all constructs), as functional, OWL/XML, RDF/XML, Turtle (4 files) | identical in all four formats, incl. axiom count 70 |
| OWL2Bench DL-1 (functional) | identical (54441 axioms, 14 report blocks) |
| generated `big.ofn`, `big2.ofn` (transitive+symmetric chain, cycles, 7520 individuals, large-ABox warning, reduction 5973 of 7520 = 79 %) | identical |
| generated `red.ofn` (hasKey with shared/unique keys, functional data property, data restriction, subproperty of functional, inverse assertions, anonymous individuals, sameAs, nominals; reduction 989 of 1506) in functional, OWL/XML, RDF/XML, Turtle | identical, reduction number identical in all 4 |
| OWL2Bench DL-2, DL-5, DL-10 (RDF/XML) | identical except: axioms 3 lower, data properties 12 instead of 13 (see below) |
| OWL2Bench EL-1, QL-1, RL-1 (RDF/XML) | identical except: axioms 1 lower |
| DL-20 | run with Konclude only (counts consistent with DL-10; no Java run: memory) |

Known differences (honest list):

1. **Dropped trivial axioms in RDF input.** Konclude's RDF mapper drops `DataPropertyRange(p rdfs:Literal)` and `SubDataPropertyOf(p owl:topDataProperty)`. So "Axioms" is lower by 1 (EL/QL/RL) or 3 (DL-2/5/10) and a data property that only occurs there is missing from the signature count. Nothing else changes.
2. **Annotation assertions** are not counted (Konclude does not keep them); none in the test files.
3. **`e.g.` examples**: first three in file order instead of OWL API order; operand order inside `ObjectOneOf`/`ObjectUnionOf` text is file order (OWL API sorts it); blank/anonymous rendering and datatype names may differ slightly. The truncation (170 characters + `...`) is the same.
4. **Combination warning (nominals/hasValue with hasKey/Self)**: same condition as Java (`Self` present, or hasValue and hasKey). Java shows only the hasValue axioms as examples (none if only `Self` is present); this tool shows hasValue axioms and then Self axioms. Deliberate.
5. **Feature line "inverse roles"** is true if there is an `InverseObjectProperties` axiom *or* any `ObjectInverseOf` expression. Java only looks at the axiom. Deliberate (looks like a gap in the Java tool).
6. **Order of warnings** for several transitive roles is sorted by IRI (Java: hash-set order).
7. **RDF input approximations**: individuals/assertions are recognised through the declared properties and `rdf:type` with non-OWL/RDF/RDFS objects; undeclared properties are ignored; literal datatypes are not distinguished in the reduction estimate (only their count/lexical value). On the OWL2Bench and generated RDF files the numbers equal Java's.
8. If the ontology cannot be loaded (missing file, parse error) the loader prints the parser errors and "could not be loaded, no lint report is available". The process exit code is 0 (as for the other commands).

### Statements about "this build" (please decide)

The README lists hasKey, inverse-functional and qualified cardinality as **not enforced**. I checked with `materialize` on this tree:

* `hasKey`: two instances with the same key value are **not** merged -> the Java text is kept unchanged.
* inverse-functional (`a p c`, `b p c`, `InverseFunctional(p)`): `materialize` **does** write `a sameAs b`.
* qualified `ObjectMaxCardinality(1 r B)` with two `B` fillers of the same individual: `materialize` **does** write `x sameAs y`.

So the Java texts "are not merged" / "are not used to merge/clash" would be false for these two in this build. I therefore reworded only those two details so that they stay true: they say that the README lists them as not enforced, that a small materialize test did merge, and that one should not rely on it for all cases. The titles and the warning itself (which are what drives users to check) are unchanged. If you prefer the original Java wording, replace the two strings in `CLintOntologyAnalyser.cpp` (`createReport`, search for "listed as not enforced"). I did not test clashes (inconsistency detection) for these two.

## Tests run (all in WSL, Release build)

```
Konclude lint -i /mnt/d/owl2gen/diagnostics/owl2bench/OWL2DL-1.owl      # 0.6 s, 110 MB
Konclude lint -i .../OWL2DL-5.owl    # 4.1 s, 420 MB     (DL-10: 10.9 s, 880 MB; DL-20: 19.6 s, 1.67 GB)
Konclude lint -i D:\scratch_logs\lint_work\t1.ofn                         # all constructs; t1_x.owx, t1_r.owl, t1_t.ttl = same ontology in other formats
Konclude lint -w 1 -i reg1.ofn -o /tmp/lintout/report.txt                 # stdout and file identical
Konclude lint                                                              # "Lint request file not specified." + usage
Konclude lint -i /nonexistent/file.owl, garbage.owl                        # error messages, no report

```
Scripts and test files: `D:\scratch_logs\lint_work\` (`runlint.sh`, `cmp_lint.py`, `gen_large.py`, `gen_red.py`, `*.ofn`, `java_*.txt` / `kon_*.txt` outputs).

Sample output (`t1.ofn`, same as Java except example order):
```
Ontology: t1.ofn
Axioms 70 | classes 15, object properties 6, data properties 1 | individuals 13, class assertions 2, object property assertions 4, data property assertions 1
[INFO] Features: nominals true, inverse roles true, number restrictions true (largest constant 15), transitive roles 2, hasKey axioms 2
[WARN] hasKey is not enforced
       Konclude (this build) does not derive sameAs or inconsistencies from owl:hasKey, ...
       e.g. HasKey(<http://example.org/lint#A> () (<http://example.org/lint#d> ))
...
10 warning(s).
```

No regression: `consistency`, `classification`, `realization`, `materialize` run with the old (`kon_clean`) and the new binary on four small ontologies (functional with hasKey, OWL/XML, Turtle, generated): same verdicts; output files identical after sorting lines. (The *unsorted* output order of realization/classification/materialize differs from run to run in the **old** binary as well - three runs of `kon_clean` gave three different md5 sums - so I compared sorted lines.)

## Known limits

* Only the Redland Linux build (`KoncludeRedlandLinux.pro`) was compiled and tested. The RDF code is under no `#ifdef`; builds without Redland would still compile it (it only uses the generic `CTriplesData` interface) but were not tried.
* The old non-stream OWL/XML handlers (`CXMLOWL2Handler`, SAX parser) are not used by the CLI auto-loader and were not hooked for hasKey.
* `owl:imports` were not tested; the lint loader uses the same load command as `consistency`, so imports are handled the way that command handles them.
* DisjointUnion/EquivalentClasses are analysed as in Java (only `SubClassOf` counts for the complex-GCI warning).
* Java-vs-Konclude comparison for DL-20 and the quadratic transitive-closure numbers on RDF input with blank-node ABox were not done.
