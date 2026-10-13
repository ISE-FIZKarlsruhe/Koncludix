# Updates to a knowledge base (OWLlink Tell and Retract): the lost-instances bug

## What was wrong

When facts were added to a knowledge base with `Tell` (OWLlink), some individuals **disappeared from the instance lists** afterwards. Example: 2 persons, add a third, ask for the instances of `Person`: only 2 of 3 came back. The persons that disappeared were different from run to run. Their types were still correct (`GetFlattenedTypes` was right), only the class-to-instances answer lost them. This happened with every ontology, even with no axioms at all, and in the original build too.

## The cause (found with the trace described below)

Every individual has an ID number. When a revision is rebuilt (the default, `IncrementalRebuild` off):

1. the builder reads all told facts again, finds the old individuals by name (they keep the IDs they had) and puts them into the new, still empty individual list;
2. **before** that, it computes where the IDs for the new individuals should start, by counting the individuals in the list. The list is still empty, so the count is **0**;
3. the new individuals get IDs 0, 1, ..., which the old individuals already have, and **overwrite** them in the list.

So after a `Tell` of 2 new persons, the 2 old persons with IDs 0 and 1 were gone from the list (for example `p3` and `p4`). That matches every observation: the number lost equals about the number added, and which ones are lost depends on the order in which the individuals were first numbered (a hash order, different per run).

## The fix

`Source/Reasoner/Generator/CConcreteOntologyUpdateBuilder.cpp`, function `buildIndividualIDs`: the new IDs now start after the highest ID of the individuals that are already waiting to be installed (6 lines). Nothing else changed.

## How it was found (for the record)

Minimal reproduction (`experiments/incremental/incr_min.py`): no axioms, 2 persons, one `Tell` gives 2 instead of 3. Then: a `Tell` of an unrelated axiom loses nothing; a second query on the same revision loses nothing; only new individuals trigger it. Forcing the normal realizer did not help (the incremental realizer was not involved: both revisions used the normal one). A trace of the label-to-individual lists showed that the second revision's cache had entries for IDs 0 to 7 only, and that the names behind those IDs had changed; a trace of the builder showed the ID assignment starting at 0.

## Tests

`python3 tests/incremental/run_revision_tests.py PATH_TO_KONCLUDE` (18 checks): for three ontology variants (no axioms; an enumerated class; the enumerated class with `Person ⊑ ≥3 hasHobby.Hobby`) and five sizes (1+1 up to 500+5 persons), the instances after the `Tell` and after the following `Retract` must equal those of a fresh load of the same data; plus three revisions in a row. The original release build passes 2 of 16, the fixed build 16 of 16. Regression: interval tests 24/24, temporal 20/20, diagnose 7/7, the 231 small ontologies give the same `consistency` verdicts (only the known `c0266`).

## The second problem: `IncrementalRebuild=on`

This option makes a revision reuse the data of the previous revision (the individual list, the axioms) and build only the new axioms, instead of rebuilding everything from the beginning. It crashed or stopped answering. Two separate causes were found with a crash call stack (a temporary handler):

1. **A crash (null pointer).** The incremental consistency test wants the *completion graph of the previous revision* as tasks. When the previous revision kept its consistency only in the representative cache (the normal case for ABox-heavy ontologies), those tasks do not exist, and two functions used them without checking (`CSatisfiableCalculationTaskFromCalculationJobGenerator::createSatisfiableCalculationTaskExtension`, then `initializeIncrementalIndividualExpansion`). Fix: a previous revision counts as "usable for the incremental test" only if it has the model tasks (`CRequirementConfigPreprocessingThread::searchPreviousConsistentOntologyVersion` and `CIncrementalPrecomputationThread::getLastConsistentlyTestedOntologyVersion`), and the call is guarded. Otherwise the revision is reasoned over completely.
2. **Wrong answers when the model *is* reused.** For ontologies with nominals the previous model exists, and the incremental reasoning then returns an **empty** instance list after a `Tell`. That is a real bug (or an unfinished feature) in the incremental completion-graph reuse, not investigated further. It is switched off by a new option, `Konclude.Ontology.Revision.ReuseConsistencyModel` (default **false**, marked EXPERIMENTAL). With it off, `IncrementalRebuild=on` means: build the revision incrementally, reason completely.

Result: `IncrementalRebuild=on` is now safe and correct (the revision tests give 18 of 18 in both modes, after the fixes below). On small ABox-heavy ontologies it is **not faster** than the default (10,000 persons, +20, retract: 10.7 s against 1.6 s with it off), because the work there is the ABox reasoning, which is repeated. It pays when the **terminology is large**: see the section "When it pays" below (real PMD ontology: about 15 times cheaper per period).

## Timing of updates (fixed build, WSL, one worker; hobby model with `Person ⊑ ≥3 hasHobby.Hobby`)

| Persons | Load, +20, retract, 3 answers (default) | `IncrementalRebuild=on` | One fresh load of the base |
|---|---|---|---|
| 5,000 | 0.9 s | 5.3 s | 0.7 s |
| 10,000 | 1.6 s | 10.7 s | 1.2 s |
| 20,000 | 18.2 s | 26.1 s | 11.9 s |

An update is therefore roughly as expensive as loading again, plus a small amount, which is acceptable for the temporal layer (a revision per period) but is not yet "the cost of the change". The old release build needed **1,276 s and 11.7 GB** for the same 10,000-person run (it includes all our earlier memory fixes, not only this one). A reading of 8 s for the fixed build in an earlier run was taken while another heavy job was running and is wrong.

## When it pays: a large terminology (the real PMD creep-test ontology)

Experiment `experiments/temporal/tbox_cost.py` (data: the PMD workshop files). The terminology has 3,358 axioms (`cto_onlytbox.owx`). Period k brings the data of creep test k (729 axioms per test, `cto_full.owx`). After each period we ask for (a) the instances of "creep testing process" and (b) the instances of the *defined* class `CTO_0000022` "creep test piece chemical composition", whose instances can only be found by reasoning (47 per test record).

| Way of reasoning in period k | Cost per period |
|---|---|
| New process, terminology + only the new record (what `tlayer.py` does per changed group today) | 10.7 to 11.6 s every time (almost all of it is classifying the terminology) |
| New process, terminology + all records so far | 10.9 s (k=1) growing to 21.3 s (k=12) |
| **One knowledge base, `Tell` per period, `IncrementalRebuild=on`** | **10.9 s for k=1, then 0.04 to 2.3 s per added record (average 0.7 s)** |

All 12 periods together: 18.5 s with revisions against 133 s with a new process per period (7 times) and 190 s with all data each time (10 times). **The answers are identical to a fresh load in all 12 periods**: the same instance names for both classes (up to 12 processes and 564 inferred composition items).

Why: with the option on, the revision keeps the work already done for the terminology (its classification). With the default, every revision does it again (about 11 s here).

Honest limits of this measurement:
- One machine (WSL, one worker), one run per setting; the single increments vary (0.04 to 2.3 s), the trend does not.
- Only **added** records were measured at this size. Retraction with the option on is covered by the 16 small revision tests, not yet by a large one.
- The `Tell` answers with "Annotation is not supported" messages in every mode (annotations are skipped; all logical axioms are loaded; the same for fresh loads).
- The reuse of the previous *model* (`ReuseConsistencyModel`) is still off; what is saved here is the terminology work, not the ABox work. With a very large ABox the per-period cost still grows with the data.

## The third problem: a fact that was retracted and told again was ignored (`IncrementalRebuild=on`)

Found by the live mode of the temporal layer (a fact that ends and begins again is normal in a timeline). With the option on, `Tell A`, `Retract A`, `Tell A` left `A` out: after the last Tell the instance list was incomplete (the regression test shows 9, 6, 6 instead of 9, 6, 9 instances). The default mode was correct.

Cause: the update builder keeps the set of (individual, assertion) pairs it already knows (`mIndiTermAssertionAxiomSet`). When an assertion is retracted it was dropped from the individual but its pair stayed in the set, so the later Tell found the pair "already known" and did not take the assertion up again (the code for class axioms already removed its pair in the same situation; the code for assertions did not). Fix: `CConcreteOntologyUpdateBuilder::buildIndividualIndi` forgets the pair of a retracted assertion. Test: the new check "Tell, Retract, then Tell the same facts again" fails on the old binary and passes on the fixed one (17 of 17 in both modes). A second change that I tried first (also removing the fact from the set of retracted axioms in the update collector) turned out to be unnecessary and was removed.

## The fourth and fifth problem: a Retract and a Tell that are built together (`IncrementalRebuild=on`)

Found while checking the live mode: `Retract Student(dan)` followed by `Tell Employee(dan)` without a question in between (one request, or two requests) wrongly reported the knowledge base as inconsistent, and a Retract of persons followed by a Tell of new persons **crashed** the process (exit code 139). The default mode was correct. Two causes, both in the incremental build:

1. **Wrong answer.** The check "is this assertion retracted?" in `CConcreteOntologyUpdateBuilder::buildIndividualIndi` used only the set of axioms retracted in the *last* revision. When the Retract was an earlier revision that had not been built yet (nothing asked in between), the later revision did not know about it and kept the retracted assertion. Fix: an assertion also counts as retracted when it is no longer in the current set of told axioms (`mTellAxiomSet`).
2. **Crash.** `CReverseRoleAssertionGeneratorPreProcess::preprocess` dereferenced `prevIndividual`, which is null for an individual that is new in the revision, whenever the revision also deleted ABox assertions. Fix: a null check.

Found with a temporary trace of what the builder keeps and drops, and a temporary crash handler that prints the call stack. Test: "Retract and then Tell without a question in between" fails (crash) on the old binary and passes now; 18 of 18 revision tests in both modes.

## Open findings (not fixed)

- **DL-safe rules in a loaded ontology, then a Tell.** Loading the real PMD OWL/XML file (it contains 4 DL-safe rules, so nominal schemas are used) and then telling facts makes the process recurse for ever and crash (exit code 139) in `CNominalSchemaTemplateExtractionPreProcess` (`hasNominalSchemaConcept`, `collectNominalSchemaConcepts`, `hasNominalSchemaConceptFillHash`). I tried a visited-set guard in two of the three functions; the crash only moved to the third, so the concept graph of that revision is cyclic. I removed the partial patches. Ontologies without DL-safe rules are not affected; the layer's terminology (functional syntax, no rules) works.
- **Incremental classification and realization are not reliable.** `previousClassesClassifiedOntology` is never found in a Tell chain (`prevClassified=0`) because it is searched from the previous revision *with a reusable consistency model*, which my earlier guard (`ReuseConsistencyModel` off) never gives. When I let the search start from the previous revision itself, the incremental classifier and realizer were used and many revision tests failed (wrong instance lists). Reverted. What makes the revisions fast is something else: the **saturation-based classification** ("sufficiently saturated, extracting data for classification": 3 ms instead of 10 s), which applies when the saturation of the revision has no insufficient node.
- **A class defined as the complement of another one (`TLneg_C ≡ ¬C`) makes that fast path unavailable**: the saturation is then insufficient and every revision classifies the whole terminology again (10 to 13 s instead of 0.1 to 0.7 s; measured with the PMD terminology). The live mode of the layer therefore leaves the helper classes out in validation mode, where a negative type changes no answer.
- **Instance query for a complex class expression crashed the server on the PMD ontology** (`GetFlattenedInstances` of `ObjectComplementOf(C)`): **fixed**. `CConcreteOntologyQueryBasicBuilder` asked every query concept for its class name, which a class expression does not have (null pointer); it now uses the existing `getStringFromConcept`. The answer equals the helper-class route (90 of 188 individuals, identical sets), so the live mode uses it for entailed negations and needs no helper class in any mode.
- **A hang in the incremental mode.** After a certain sequence (a fresh knowledge base after a restart, then Tell and Retract revisions) every query that needs realization (types, complement instances) never answers with `IncrementalRebuild=on`; the default mode answers. Found by a random timeline (replay of the logged requests). Not analysed. The layer gives up live mode after `--live-timeout` seconds (default 300) and continues with one process per group, with the same answers.
- **Default mode, scenario: a role assertion retracted and told again** (`Tell r(i2,i3)`, `Retract r(i2,i3)`, `Tell` it again with other facts): the default mode misses a clash that a fresh load finds, also in the original build. The builder does apply all assertions in the last revision (traced), so the loss is later. Not found. The layer therefore never re-tells a retracted fact into the same knowledge base: it starts a fresh one (`knowledge base restarts` in the live line).
- **`GetFlattenedInstances` of a named class defined as a complement returns nothing**, although `GetFlattenedTypes` of an individual lists it (and `materialize` does). Not investigated; the complement query above gives the right answer.
- **The first ABox is expensive.** Loading the terminology and then telling the first facts costs the full 10 s (the first saturation with individuals has insufficient nodes); every later Tell costs 0.1 to 0.7 s. This is the "first period 10.9 s" of the measurements above. An earlier version of this page said that a Tell after `LoadOntologies` is never cheap: that was wrong, the comparison was between the first ABox and a later one. Loading the terminology plus the first record from a file and then telling the second record: 0.4 s.

## What is not fixed (honest list)

- **Reuse of the previous reasoning** (`ReuseConsistencyModel=true`) returns empty instance answers for ontologies with nominals. This is the part that would make an update cost "the size of the change". Fixing it means working inside the incremental completion-graph code (`CIncrementalCompletionGraphCompatibleExpansionHandler`, the incremental precomputation and classification threads); not started.
- `owllinkfile` crashes when the knowledge base named in a request does not exist (found earlier, not fixed).
