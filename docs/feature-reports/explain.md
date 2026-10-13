# Konclude `explain` command (justifications inside the Konclude binary)

Work tree: `~/kon_feat_explain` (WSL, copy of `~/kon_clean`, built Release, `make -j4`, full link OK).
Nothing under `D:\Github\Koncludix` was touched, nothing was committed or pushed.

## What was added

A new CLI command `Konclude explain`. It computes a 1-minimal justification (a minimal set of axioms) for

1. the ontology being inconsistent (default),
2. `--subclass SUB SUPER`: SUB is a subclass of SUPER (test axioms `ClassAssertion(SUB x)`, `ClassAssertion(ObjectComplementOf(SUPER) x)`, fresh `x = <urn:konclude:explain#x>`),
3. `--type INDIVIDUAL CLASS`: INDIVIDUAL is an instance of CLASS (test axiom `ClassAssertion(ObjectComplementOf(CLASS) INDIVIDUAL)`).

Only the Konclude binary is needed (no Python, Java, OWL API, second binary).

### Strategy chosen: B (parse once, rewrite as functional syntax, re-invoke the same executable)

* The ontology is loaded once, in process, with the normal Konclude loading commands (`CreateKB`, `LoadKnowledgeBaseOWLAutoOntology`), so every input format Konclude reads works (functional syntax, OWL/XML, RDF/XML, Turtle). The loaded axiom expressions are written as OWL functional syntax by a new writer, one axiom per entry, full IRIs (no prefixes). Then the knowledge base is released.
* Every test runs `<this executable> consistency -w 1 -i <temp file>` through `QProcess` with `QCoreApplication::applicationFilePath()`. The answer is read from the "is inconsistent" / "is consistent" line, as the earlier Python prototype does.
* Why B and not in-process (A): a per-call timeout is the main requirement for noisy data. An in-process reasoning call cannot be cancelled safely; a child process can be killed. A separate process also cannot leak state from one test into the next, and building ontologies for subsets in process would have needed the (large) ontology build machinery to be re-run for every subset. The cost is process start and re-parsing for each call (about 0.1 s for tiny files, about 0.5 s for the 4 MB OWL2Bench DL-1 file).
* The existing renderers in `Source/Renderer` were **not** used. They are not faithful: `COntologyAxiomExpressionsRenderVisitor` renders `ObjectMinCardinality`/`ObjectMaxCardinality` as `ObjectExactCardinality`, `EquivalentObjectProperties` as `InverseObjectProperties`, never dispatches `InverseFunctionalObjectProperty`, and silently drops unknown axioms; `COntologyOWL2QtXMLRenderer` writes the cardinality attribute with `QString(int)`. I did not change them (out of scope); the new writer is separate, uses `dynamic_cast` per expression class, and **fails with an error** on any axiom or expression it cannot write (no silent drop).
* Triple-encoded assertions (RDF/XML, Turtle input keeps class/property assertions as Redland triples, not as axioms) are written by the same writer, using the same recognition rule Konclude's own indexer uses (rdf:type with a known class, known object property with a resource as object, known data property with a literal as object).

### Safety check built in (on by default, `--no-verify` to skip)

Before explaining, Konclude is run on the original file and on the rewritten ontology; if the two answers differ (e.g. the writer lost an axiom), the command stops with an error instead of explaining something else. This check really fired during development: before the triple support existed, all RDF/XML inputs were refused with "inconsistent vs consistent".

### Search algorithm

Delta debugging with the same loop as the earlier Python prototype (complement testing, granularity doubling), plus:

* verdict cache (the same subset is never sent to the reasoner twice; only real calls are counted),
* stage 1 (optional, `--no-slicing` disables): units are the connected components of the ABox axioms (individuals connected by an axiom), TBox/RBox fixed. This is only a **candidate generator**: every step is accepted only if the reasoner confirms it, so it cannot make the result unsound,
* stage 2: every remaining axiom is its own unit and nothing is fixed; this gives the 1-minimality: at the end the removal of each single remaining axiom was tested by a real reasoner call. Declarations are kept in every test file and are not part of the result (as in explain.py).
* timeouts (`--timeout`, default 60 s per call; 0 = none) and failed calls count as "not entailed" (axiom stays). If any happened the result carries a warning: it is then a verified justification, but minimality is only guaranteed for the calls that answered.
* In entailment mode one extra call checks whether the justification is already inconsistent without the test axioms (then a note is printed).

(I first had separate stages "ABox axioms with fixed TBox", "TBox with fixed ABox"; on DL-1 they were 3 to 4 times slower than the mixed stage, because a consistent test with the full TBox needs the full reasoning. They were removed.)

## Files

New (all under `Source/`):

| file | content |
|---|---|
| `Renderer/COntologyFunctionalSyntaxWriter.{h,cpp}` | faithful functional-syntax writer for all axiom/expression kinds, triples assertions |
| `Control/Loader/CExplainJustificationSearch.{h,cpp}` | search (delta debugging, cache, stages), independent of Konclude classes |
| `Control/Loader/CCLIExplainBatchProcessingLoader.{h,cpp}` | the command: loading, flattening, child-process oracle, output |
| `Control/Interface/CommandLine/CCLIExplainComandLinePreparationTranslator.{h,cpp}` | command line options |

Changed (small hunks): `Control/Loader/CDefaultLoaderFactory.{h,cpp}` (register loader), `Control/Interface/CommandLine/CCommandLinePreparationTranslatorSelector.{h,cpp}` (register translator), `Control/Loader/CCommandLineTranlatorHelpLoader.cpp` (usage text), `mainLoader.cpp` (`return` the exit code of explain instead of 0; other commands still return 0), `Konclude.pri` (8 lines, source/header lists). The generated `Makefile` is not part of the patch: regenerate with `qmake -o Makefile KoncludeRedlandLinux.pro` (the same command as in the head of the old Makefile) and run `make`.

## How to apply

```
cd <Konclude tree>            # the directory that contains Source/ and Konclude.pri
patch -p1 < feat_explain.patch
qmake -o Makefile KoncludeRedlandLinux.pro && make -j4
```
Checked: the patch applies cleanly to a copy of `~/kon_clean` and gives exactly the files of `~/kon_feat_explain`.
Patch: `D:\scratch_logs\feat_explain.patch` (15 files, `diff -uN`).

## Usage

```
Konclude explain -i ontology.owl [-o justification.txt]
Konclude explain -i ontology.owl --subclass "<http://ex#A>" "ObjectIntersectionOf(<http://ex#B> <http://ex#C>)"
Konclude explain -i ontology.owl --type "<http://ex#a>" "ObjectSomeValuesFrom(<http://ex#p> <http://ex#C>)"
  --timeout SECONDS     per reasoner call, default 60, 0 = none
  --call-workers N      threads of every reasoner call, default 1
  --no-slicing          skip the ABox component stage
  --no-verify           skip the original-versus-rewritten check
  --flatten FILE        only write the ontology as functional syntax, one axiom per line, and stop
```
Class expressions are in functional syntax with full IRIs; a bare IRI (`http://ex#A`) is accepted and put in `<>`. `-o` writes only the axioms of the justification. Help: `Konclude explain -h` / `Konclude -h` (new lines added). Exit code: 0 explanation found, 1 nothing to explain (consistent / not entailed), 2 error (could not load, no answer, writer failure, check failed).

Output style is the same as the earlier Python prototype; Konclude's usual `{info}` log lines are also on stdout (grep `-v '^{info}'` to get only the result).

Real output (OWL2Bench DL-1 with `ClassAssertion(:Woman :U0C0D0AP0)` injected; `Man`/`Woman` are disjoint, `U0C0D0AP0` is a `Man`):
```
The ontology is inconsistent because of these 3 axiom(s) (minimal; 84 Konclude calls):
  DisjointClasses(<http://benchmark/OWL2Bench#Man> <http://benchmark/OWL2Bench#Woman>)
  ClassAssertion(<http://benchmark/OWL2Bench#Man> <http://benchmark/OWL2Bench#U0C0D0AP0>)
  ClassAssertion(<http://benchmark/OWL2Bench#Woman> <http://benchmark/OWL2Bench#U0C0D0AP0>)
```
Entailment:
```
$ Konclude explain -i ent03_transitive.ofn --type "<http://ex#a>" "ObjectSomeValuesFrom(<http://ex#p> <http://ex#C>)"
The ontology entails the statement because of these 4 axiom(s) (minimal; 14 Konclude calls):
  TransitiveObjectProperty(<http://ex#p>)
  ObjectPropertyAssertion(<http://ex#p> <http://ex#a> <http://ex#b>)
  ObjectPropertyAssertion(<http://ex#p> <http://ex#b> <http://ex#c>)
  ClassAssertion(<http://ex#C> <http://ex#c>)
(statement: ClassAssertion(ObjectSomeValuesFrom(<http://ex#p> <http://ex#C>) <http://ex#a>); test axioms added for the check: ClassAssertion(ObjectComplementOf(ObjectSomeValuesFrom(<http://ex#p> <http://ex#C>)) <http://ex#a>))
```

## Tests and results

Test files were generated into `~/ex/tests` (WSL; scripts `mktests.py`, `runtests.sh`, `runent.sh`, `runfmt.sh`, `regress.sh` in `~/ex`). Each small ontology contains 12 harmless distractor axioms.

**Inconsistency cases** (all give the expected set; the Python tool, run on the same files, gave the same axiom sets for the cases 01 to 09, 11 and 12; cases 10 and 13 to 19 were not run with the Python tool, their justifications were checked by hand): disjoint classes + instance (3 axioms), functional property with two different values (4), irreflexive + self loop (2), asymmetric (3), max-cardinality clash (5), nominal clash (2), `ObjectMaxCardinality(2)` with 3 distinct fillers (5), datatype clash (2), `Thing SubClassOf Nothing` with no ABox (1), property chain (5), anonymous individual (3), literal with escaped quotes and backslash (3), inverse + HasValue (3), HasSelf + DisjointUnion + irreflexive (3), datatype restriction (2), data max cardinality (4), SameIndividual (4), negative property assertion (2), inverse-functional (4). Calls: 6 to 13 for the new tool, 20 to 33 for explain.py.
Two cases (`DisjointDataProperties`, `DisjointObjectProperties` with an equivalence) are *consistent* according to Konclude itself on the original file, so "Nothing to explain" is correct for them (rc 1); the check confirmed original and rewritten agree.
**Entailment cases** (all correct and minimal): subclass via two EquivalentClasses (2), a third axiom pulled in for a different super class (3), role domain (2), transitive + class expression `ObjectSomeValuesFrom` (4, same as the earlier Python prototype), complex SubClass with a class expression on the left (1), inverse properties + range (3, same as the earlier Python prototype), not-entailed statement (rc 1), inconsistent ontology (note printed), bad class expression syntax (rc 2, parser messages shown), a missing option value (usage text).
**Formats**: all inconsistency tests were also converted with the OWL API to RDF/XML and OWL/XML (`tools/Convert*.java`): the justifications are identical to the functional-syntax ones, except for names of anonymous individuals and OWL/XML plain literals, which Konclude reads as `rdf:PlainLiteral` instead of `xsd:string`. A Turtle test: a copy of `~/mse_kg_0804.ttl` (5.5 MB real knowledge graph, 23053 axioms) with the triple `<orcid ...> a BFO_0000003` appended: justification of 8 axioms (one `DisjointClasses`, a chain of five `SubClassOf`, two assertions), 232 calls, 13 s.
**Fixpoint check** of the writer: a kitchen-sink ontology with every construct (58 axioms; all class, property and data range constructors, chains, inverse, HasSelf, nominals, data restrictions, literals with escapes, anonymous individuals) written with `--flatten`, read back and written again gives the identical file.
**Independent minimality check** (`verifymin.py`, runs plain `Konclude consistency` on the justification and on all subsets with one axiom less): holds for the DL-1 justification below (5 axioms: inconsistent, every 4-subset consistent).
**OWL2Bench DL-1** (`OWL2DL-1.owl`, 50,128 axioms + 3,906 declarations, plain consistency check about 13 s):
* one injected `ClassAssertion(:Woman :U0C0D0AP0)`: exactly the 3 expected axioms (the injected one, the Man assertion, `DisjointClasses(Man Woman)`), 84 calls, 17 s on a loaded machine (13 s on an idle one). The Python tool on the same file: same 3 axioms, 75 calls, about 11 s. So the in-binary version is not faster here (the Python tool starts from a differently ordered axiom list), but it is in the same range.
* injection that needs TBox reasoning (`WomenCollege` with `hasStudent` to a Man): 5 axioms (the two injected ones, the Man assertion, `EquivalentClasses(WomenCollege ...)`, `DisjointClasses`), 155 calls, 42 s on a loaded machine. Without the component stage (`--no-slicing`): same result with 111 instead of 84 calls on the first test.
* with `--timeout 1`: the warning about timed-out calls is printed and the (still verified) result is the same.
**Existing commands** unchanged: `consistency` (consistent and inconsistent files), `classification` (output file written), `satisfiability`, `realization`, `-h`, no argument: all still run as before. The only change that can touch them is the `return` value in `mainLoader.cpp` (still 0 for every command except explain).

## Comparison with the earlier Python prototype

* Same algorithm and output format, same results on all comparable tests.
* No Java/OWL API/Python; reads RDF/XML, OWL/XML, Turtle and functional syntax through Konclude's own parsers.
* Per-call timeout and a distinguishing of "consistent" from "no answer" (the old tool treats a parse error as consistent).
* Fewer calls on the tests (verdict cache, no repeated tests), sound ABox component stage.
* Output contains full IRIs (no prefixes) and anonymous individuals as `_:label`.
* The old tool uses the OWL API's view of the ontology, this one uses Konclude's: what Konclude's parser does not keep cannot appear in the justification.

## Limits (honest list)

* Annotations and axioms that Konclude's parsers ignore (`HasKey`, `DatatypeDefinition`, annotation axioms, SWRL rules; also single axioms Konclude cannot build and logs as "Couldn't extract/match ...", e.g. a malformed `SubDataPropertyOf` in the MSE graph) are not in the explanation. This is consistent with what Konclude reasons over, but an explanation never mentions them.
* One justification is returned (the first minimal one the search finds), not all justifications, and it is minimal (no proper subset works), not the smallest one.
* Minimality assumes that the reasoner answers deterministically and monotonically; Konclude answers were stable in all tests. Timeouts weaken the minimality claim, as printed.
* Each call re-reads the file, so very large ontologies with many consistent (slow) tests take long; the number of calls is about 2 to 3 times the number of justification axioms times the logarithm of the ontology size. The DL-1 ontology (50k axioms) needed 17 to 42 s in the tests above, on a machine shared with other jobs. No parallel testing yet (`--call-workers` only sets the threads of one call).
* The explain process keeps the parsed axioms of the whole ontology as text in memory next to the knowledge base that is released after loading; for an ontology with millions of axioms this is several GB.
* Statements are passed through Konclude's command line translator: double quotes inside an expression (literals) are not supported on the command line, and the command line is split at spaces, so each expression must be one shell argument (quote it). `=`, `+`, `?`, `%` in IRIs work (tested).
* The ABox component stage is only a speed-up. With one connected ABox (like DL-1) it does nothing (3 components, one large).
* The reading of RDF triples (which triple is an assertion) mirrors Konclude's indexer; ABox triples with a class or property that Konclude does not know as such are ignored by Konclude as well. The built-in check against the original file would report a difference.
* Exit code 2 is not set for the "inadequate arguments, usage printed" path (that path is shared with all commands and returns 0).
* Literals: Konclude's functional syntax parser keeps backslash escape sequences inside quoted strings as they are; the writer keeps existing escapes and escapes everything else, so Konclude reads back the same value. A lexical value coming from RDF that itself contains a backslash followed by a quote would be read as an escape sequence (not observed).
* Language-tagged literals are handled the way Konclude stores them (`"text@lang"^^rdf:PlainLiteral`).
