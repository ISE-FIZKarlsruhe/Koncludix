# ABox reduction inside Konclude (`materialize` and `realization`)

Tree: `~/kon_feat_reduce` (copy of `~/kon_clean`, builds with `make -j4`, links). Patch: `D:\scratch_logs\feat_reduce.patch`
(`diff -ruN` against `~/kon_clean/Source` plus the `Konclude.pri` hunk; verified: applied with `patch -p1` to a fresh copy of
`~/kon_clean` it reproduces `~/kon_feat_reduce/Source` exactly). Nothing under `D:\Github\Koncludix` was touched.

## What was added

The external workflow (`AboxReduce.java` + `materialize-reduced.sh`) now runs inside the binary:

1. After parsing (OWL functional syntax or OWL/XML) and before the reasoner's ontology is built, the told axioms are analysed.
   Individuals with the same asserted types, the same data shape and the same neighbour classes (colour refinement, as in the Java tool)
   are represented by one individual; the axioms of the others are removed from the parsing session (class, data, object property
   assertions, declarations, and the individual expressions).
2. The reduced ontology is classified/realized as usual.
3. The writer copies the results to the removed individuals while writing (streaming, no second file).
4. A log line is printed, e.g.
   `ABox reduction applied: 19950 interchangeable individuals were merged into 50 representatives (3727 of 23677 individuals left, 84.3 percent removed; 3512 individuals are never merged: ...)`
   and `ABox reduction not applied: ...` with the reason otherwise.
5. Safety net: if after reasoning the reduction turns out to be unusable (the reduced ontology is inconsistent, or the reasoner derived
   that some individual is the same as others, other than through told `SameIndividual` axioms), nothing is written, the
   program re-executes itself with the same command line and `ReduceABox=off`, so the result is then exactly the unreduced result.

### Options (registered in `CReasonerConfigurationGroup.cpp`, pass with `+=Key=value`)

| key | default | meaning |
|---|---|---|
| `Konclude.CLI.Materialize.ReduceABox` | `off` | `off` / `on` (reduce whenever individuals can be removed) / `auto` |
| `Konclude.CLI.Realization.ReduceABox` | `off` | same switch for the `realization` command (types are copied) |
| `Konclude.CLI.Materialize.ReduceABoxMinIndividuals` | 100000 | auto: reduce if the ontology has at least this many individuals (0 = ignore) |
| `Konclude.CLI.Materialize.ReduceABoxMemoryPercent` | 50 | auto: reduce if estimated memory demand > this % of available RAM (`/proc/meminfo` MemAvailable, `GlobalMemoryStatusEx` on Windows) |
| `Konclude.CLI.Materialize.ReduceABoxMinReductionPercent` | 10 | auto: only if at least this % of the individuals can be removed |
| `Konclude.CLI.Materialize.ReduceABoxBytesPerIndividual` / `...BytesPerAssertion` | 20000 / 2000 | the (rough, uncalibrated) memory estimate: individuals*B1 + assertions*B2 |

Default is `off`: `auto` is a heuristic (only the memory estimate is rough; the reduction itself is the same as for `on`), so it is not the default.
Example: `Konclude materialize -w 1 -i in.ofn -o out.nt +=Konclude.CLI.Materialize.ReduceABox=on`.
Not applied (with a log line): ontologies loaded as RDF triples (RDF/XML, Turtle, N-Triples: the ABox lives in the Redland store there), ontologies with imports /
several load steps, `Konclude.CLI.IRIName` (single individual), abbreviated IRI output.

### Files (in the patch)
New: `Reasoner/Generator/CABoxReducer.{h,cpp}` (analysis + reduction), `Reasoner/Ontology/CABoxReductionData.{h,cpp}` (data for the writer, fallback switch).
Changed: `COntologyLoadData.{h,cpp}` (holds the data), `Parser/COntologyMultiAutoParsingLoader.{h,cpp}` (hook), `COWLlinkProcessor.{h,cpp}` (allows it for single-file loads),
`CReasonerConfigurationGroup.cpp` (options), `CWriteMaterializedIndividualAssertionsQuery.{h,cpp}` (expansion), `CWriteIndividualFlattenedTypesQuery.{h,cpp}` (realization),
`CCLIBatchProcessingLoader.{h,cpp}` (re-exec helper), `CCLIMaterializeBatchProcessingLoader.{h,cpp}`, `CCLIRealizationBatchProcessingLoader.{h,cpp}` (fallback, forced same-individuals),
and `Konclude.pri` (source lists; regenerate the Makefile with `qmake -o Makefile KoncludeRedlandLinux.pro`). A full rebuild is needed (`COntologyLoadData.h` is included widely).
In the two files the coordinator also changed (`CWriteMaterializedIndividualAssertionsQuery.*`, `CCLIMaterializeBatchProcessingLoader.cpp`) the hunks are self-contained:
writer: new members/methods (`mReduction`, `checkABoxReductionApplicable`, `writeCollectedDataAssertions` [the existing data block moved into a helper], `writeRemovedMemberDataAssertions`, `getReductionClosure`, `writeReducedAssertionEdges`) and small `if (mReduction)` additions in the constructor, `addQueryError`, `visitConcept`, `visitIndividual`, `visitRoleInstance`, and `writeMaterializedAssertionsResult` (before writing, IRI->role hashes, per-individual members, role-fact copy, edges, sameAs names);
loader: `createMaterializeTestingCommands` split into `addMaterializeCommands` + `processNextCommand`, new `concludeOWLlinkContent` override, and the forced same-individuals command is also added when reduction is on.

## How the expansion is done (and where it is stricter than the Java tool)

* Never merged ("pinned"): individuals in TBox/RBox axioms (nominals), in sameAs/differentFrom/negative assertions, subject of complex class assertions, neighbours of anonymous
  individuals, individuals with an asserted self-loop, and individuals at an asserted edge of a role with cardinality / functional / inverse functional / reflexive / irreflexive / asymmetric /
  disjoint / transitive / chain axioms or has-self, including all roles related to them by sub-property / inverse / equivalence axioms (inverse roles in cardinalities are stripped to the named role).
* Types and same-individuals of a representative are copied; data values are written from the removed individual's own asserted values (exact lexical form and datatype, plus the
  super-property closure); object property facts between a representative and a pinned individual (or itself) are copied; **facts between two non-pinned individuals are rebuilt from the asserted edges**
  (closure computed with the writer's own sub-property/inverse propagation), not copied from the reduced ontology.
* Konclude ignores `HasKey` axioms (the parser drops them), so no key rule is needed for equality with Konclude's own result (the Java tool's key rule would matter for other reasoners).

Cases where I found the Java rules unsound and made the C++ version stricter:
1. **Mixed pair roles**: two groups U={u1,u2}, V={v1,v2} with edges u1-R->v1, u1-S->v2, u2-R->v2, u2-S->v1 have one colour class each. The Java expansion copies
   the roles of the representative pair to every real edge, so it writes `u1 S v1` and `u1 R v2`, which are not entailed. Here the pair facts come from the real asserted edges (test `mixroles`, equal).
2. **Self-loops created by merging**: an edge between two members of one class becomes a fake self-loop on the representative. Here such classes keep two representatives (edges go between the two) and individuals with a real self-loop are pinned.
3. **Data ranges / disjoint data properties / sub data properties** were not treated as exact data in Java (values 3 and 7 under a `DataPropertyRange` facet could be merged); here these properties (and everything related by sub/equivalence) are compared by exact value.
4. **Top role as sub-property / in other axioms** (universal role) makes all pairs related: reduction is switched off.
5. **Nominal + functional/cardinality roles**: a restriction with a nominal (has-value, some-values with nominal) over a merge-capable role can force merges of individuals without asserted edge: reduction is switched off for such ontologies.
6. **Merges derived by the reasoner** (functional roles, nominal classes, ...) are not reproducible by copying; any derived same-individual outside told `SameIndividual` axioms, and an inconsistent reduced ontology, trigger the fall back to the unreduced computation (above). This replaces the Java rule "individuals mentioned in complex class assertions" only partly: those individuals (and the subjects) are pinned too.
7. Reflexive roles / has-self facts (`R(a,a)` for every individual) are copied from the representative (test `selfrole`, equal).

## Tests (all with the final binary unless stated; comparison = `sort -u` of the N-Triples files, byte equal unless stated)

Scripts: `~/rt/cmp_one.sh`, `~/rt/fuzz_all.sh`, `~/rt/inflate.py`, `~/rt/normcmp.py`, `~/rt/mkhand.py` (WSL home; `/tmp` was wiped).

* **Fuzz corpus `~/fz` (231 random ontologies), original ABoxes**: 230 byte-equal, 0 different, 1 skipped (the unreduced run exceeded the 120 s limit). Reduction was applied in 97 files; in 75 of them the safety net fell back (inconsistent ontology, or derived merges) and the result is the unreduced one; in 22 the reduced result was used, 21 verified equal (the 22nd is the skipped file: its reduced run finished, the full run did not).
* **Same corpus with the ABox replicated twice (disjoint replicas; 60 s limit, 3.5 GB address-space limit)**: 231 files, 224 byte-equal, 1 equal after mapping sameAs representatives, 0 different, 6 skipped (limits). Applied 104, fallback 80, 24 reduced results used (21 verified, rest skipped).
* **Corpus replicated twice with chained replicas**: run was interrupted by the session restart at 150 of 231 files (before the final fix): 145 equal, 1 equal modulo representatives, 0 different, 4 skipped; 27 reduced results used (24 verified, 3 skipped). Not re-run to the end.
* 11 hand-written ontologies (`~/rt/hand`: mixed roles, internal edges + symmetric role + sub data property, pinned/has-value, singleton nominal class (fallback), reflexive/has-self, functional role, data range, inverse roles, inconsistent, anonymous/negative assertions, top role): all equal. The same 5 reducible ones converted to OWL/XML: all equal. `realization` with `Konclude.CLI.Realization.ReduceABox=on` on 4 of them: class assertions equal.
* An earlier run of the corpus replicated twice showed 3 "different" files; each was a sameAs-class naming or a derived-merge case that Konclude's unreduced output itself depends on (checked: the unreduced run is stable under permuting the axioms, but removing the interchangeable individuals by hand from the unreduced ontology and running the unreduced Konclude gave exactly the reduced result; Openllet shows the merge is entailed, i.e. Konclude's same-individual output is incomplete for the smaller ABox). This is why derived merges now trigger the fall back; after that change all differences were gone.
* **OWL2Bench DL-1** (functional syntax): not reducible, my analysis agrees with the Java report (3677 individuals, 3512 pinned, 0 removable). Unreduced vs `ReduceABox=on` (identical computation): equal after mapping equivalent class/property names (Konclude chooses the name of an equivalent property class differently from run to run, e.g. `hasPart`/`hasSubOrganization`). 4:16 min, 2.0 GB RSS (on) vs 3:58 min, 2.2 GB (off).
* **OWL2Bench DL-2** is RDF/XML in the files given, so reduction is not applicable (log line "loaded as RDF triples"); the full-vs-on comparison for DL-2 was **not finished** (it was started and stopped on request; I did not obtain a result). DL-5 not tried.
* **Reduction case on OWL2Bench**: DL-1 plus 2000 synthetic publications (`~/rt/dl/big1.owl`): 1950 individuals merged into 50, `on`: 4:09 min / 2.15 GB RSS; unreduced: 3:51 min / 2.41 GB RSS (the extra individuals are cheap here: -11 % memory). Output: identical except ~15-20 `dislikes` facts of ~15 pinned persons that differ in both directions (asserted facts that either run drops for different persons). Konclude's own output is unstable in exactly this way: two *unreduced* runs on DL-1 plus 50 versus plus 2000 extra bare individuals show the same kind and size of difference (18/20 facts), so this is baseline instability of Konclude on this ontology (also seen: the unmodified binary produces two different results on fuzz file c0314, 2 of 6 runs), not caused by the reduction.
* **Larger case** `big2.owl` (DL-1 + 20000 publications, 50 authors): `auto` (with `MinIndividuals=10000`) applied: 19950 of 23677 individuals removed (84.3 %), reduced run 4:31 min, 2.12 GB RSS (about the same as DL-1 alone). The unreduced run for comparison and the equality check were **not finished** (stopped at the coordinator's request because of the memory limit of the machine), so I cannot give measured full-run numbers for it. From the 2000-publication case the unreduced run would be about 2.4 GB + ~65 KB per extra individual, i.e. roughly 3.7 GB (an extrapolation, not a measurement).

## Limits (honest)

* No gain when most individuals are pinned (OWL2Bench DL-1/DL-2: 0 removable, because of transitive/functional/cardinality roles, keys-free), or when individuals are densely and randomly linked. The `auto` memory estimate constants are not calibrated.
* The parsed axioms of the removed individuals stay in memory during the run (only the reasoner's structures and the individuals are saved); saving is small when individuals are cheap for the reasoner (-11 % in the 2000 case).
* Only OWL functional syntax and OWL/XML; RDF/XML/Turtle/N-Triples inputs are not reduced. Not for imports, single-individual materialization, abbreviated IRI output.
* The fall back doubles the work when it triggers (the reduced computation is wasted, and the first process stays alive while the second runs, holding the reduced ontology). In the `auto` out-of-memory scenario this means a failing reduced result still ends in the unreduced computation.
* Konclude's output contains non-deterministic parts of its own (equivalent class/property naming, some `dislikes` facts on OWL2Bench, a flaky fact on fuzz file c0314); equality claims are therefore "equal up to this baseline noise", and byte-equality is only expected when the unreduced run is itself stable.
* The soundness argument (colour classes are bisimulation classes, merges only through pinned structure) is informal; the tests find mistakes but do not prove the absence of them. Not covered by tests: rules (SWRL, switched off), datatype definitions (dropped by the parser), `HasKey` (ignored by Konclude).
* Windows memory query (`GlobalMemoryStatusEx`) and the `QProcess` re-exec were not compiled/tested on Windows.
