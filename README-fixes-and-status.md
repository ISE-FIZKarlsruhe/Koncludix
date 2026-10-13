# Koncludix: what I fixed, what works now, what is still open

Koncludix is my version of the Konclude reasoner. 

Goal is to improve memory requirements, add additional feaures that aren't yet supported such as explnation, lint, materialize dependign on required inference type such as inverse, transitivity, hierarchy etc etc.

Additionally, adding support for approximate reasoning.

## Test data

- OWL2Bench DL files (DL-1 to DL-20): my shared folder with the OWL2Bench <https://github.com/kracr/owl2bench> data sets: <https://drive.google.com/drive/folders/1HYURRLaQkLK8cQwV-UBNKK4_Zur2nU68?usp=sharing>
- 231 small random ontologies made for testing (hangs, crashes and output changes) using OWL2Gen <https://github.com/kracr/owl2gen> -- more to be added
- InferTest (28 entailment tests and 9 inconsistency tests).
- A second reasoner (Openllet) as an independent check: I compared its facts with Konclude's output on the random files.

## Why Konclude is fast

| Idea | In simple words |
|---|---|
| Quick first pass | The easy part of the ontology is finished first, like ELK does |
| Memory of results | It does not repeat work it has already done |
| Smart back-jumping | After a failed guess it goes back to the guess that caused it |
| Cached descriptions | Similar individuals share what is known about them |
| Many cores | Several workers share the work |

## Results

"Before" means before my changes. "Now" is the final build. Timings on my laptop vary by about 10 to 20 % from run to run.

### Consistency check (1 worker)

| File | Individuals | Before | Now |
|---|---|---|---|
| DL-1 | 3.7K, 50K | 34 s, 3.5 GB | 7 s, 1.8 GB |
| DL-2 | 7K, 99K | no answer in 900 s | 26 s, 3.7 GB |
| DL-5 | 23K, 325K | no answer in 12 GB | 31 s, 2.1 GB |
| DL-10 | 50K, 711K | no answer in 12 GB | 80 s, 3.3 GB |
| DL-20 | 98K, 1.4M | no answer in 12 GB | 169 s, 5.4 GB |

### `materialize` (writes all facts), 1 worker

| File | Before | Now | Facts written | Instances of `PeopleWithManyHobbies` (check) |
|---|---|---|---|---|
| DL-1 | 820 s, 3.7 GB (4 workers) | about 3 to 4 minutes, 2.2 GB (1 worker); about 2 minutes, 3.8 GB (4 workers) | 1.44 million | 61 before, **536 now** (correct) |
| DL-2 | 773 s, 5.1 GB | about 19 minutes, 5.4 GB | 5.0 million | 119 before, **1,142 now** |
| DL-5 | not tried | **still fails on a 12 GB machine.** With the memory limit option (`-m`) the classification finishes in about 5 s after the 40 s precompute (first attempt, hash seed 2), the realization needs about 9 to 10 minutes and 4.8 GB, and then the step that computes the types of the individuals grows from 4.8 GB to more than 10 GB within 30 seconds (also when object property assertions and `sameAs` are switched off), so the run was stopped at the 7.5, 9 and 11 GB limits | none | |
| DL-10, DL-20 | not tried | **fail: out of memory** in the classification step (stopped at 10.5 GB after 161 s and 207 s) in the first test. Later: DL-10 classification with hash seed 2 finishes in 128 s with 3.2 GB; `materialize` on DL-10 and DL-20 was not tried again | none | |

The old runs were faster because they skipped tests they should have done, so their answers were incomplete (see the last column). The new runs do the tests, so they take longer but give the full answer.

### Choosing what `materialize` computes (DL-1, 1 worker)

| Selection | Time | Facts written |
|---|---|---|
| Class assertions only (no object properties, data properties, sameAs or hierarchies) | 115 s | 30,749 |
| Everything except object property assertions | 138 s | 49,997 |
| Everything | about 195 s | 1,444,364 |
| Object property assertions only (types are computed but not written) | about 200 s | 1,398,129 |

The object property assertions are the expensive part (about 1.4 million of the 1.44 million facts).

### Other tests

| Test | Before | Now |
|---|---|---|
| 231 random small ontologies, `materialize` | about 1 in 4 hung or crashed (37 hangs, 17 crashes) | 229 give the same output every time (5 runs each), 1 (`c0314`) still changes now and then, 1 (`c0162`) runs a very long search. Compared with my previous build no file lost a fact except one wrong `sameAs` pair in `c0302` (removed on purpose); 13 files gained facts (sampled: every added fact is entailed) |
| Verdict (consistent or not) on the 231 files | | 1 changed (`c0266` is now correctly inconsistent, as Openllet says) |
| InferTest, each test run alone | 25 of 28 and 8 of 9 | 27 of 28 and 8 of 9 (only the `hasKey` inconsistency test fails) |
| The small repro files, run 24 times | output changed | same facts every time |
| Cardinality change: 185 files and 1,800 small tests | not applicable | same answers with the option on and off |
| Comparison with Openllet on the 225 random files (named individuals: types and object property facts) | 1,657 facts that Openllet derives were missing in the output | 501 missing. For the files that are valid OWL 2 DL (`c0104`, `c0136`, `c0267`, `c0334`) I tested a sample of the remaining "missing" facts with Konclude's own consistency check (negated fact added): none of them is entailed, so these are facts Openllet adds by an arbitrary merge. The other files are not valid OWL 2 DL (their behaviour is undefined) |
| Are the written `sameAs` pairs really entailed? (adding `differentFrom` for the pair must give "inconsistent") | `c0302`: 1 of 76 pairs was not entailed | 75 of 75 pairs are entailed |

None of this proves the results are sound and complete. It only finds mistakes. The comparison with a second reasoner is the strongest check so far.

## What was wrong and what I changed

| Problem | Why it happened | What I did | Result |
|---|---|---|---|
| Large files ran out of memory | The rule "at least 3 hobbies" made new empty nodes for every person, although the hobbies were already in the data. Each empty node needed guesses | The rule now counts the neighbours that already exist and creates only the missing ones. New option `AtLeastBackendNeighbourSatisfaction` (on by default) | See the consistency table |
| `-w 1` (one worker) hung forever | The thread pool had no free thread | The pool is one thread bigger | `-w 1` works and is the best setting for big files |
| Crash with several workers | A worker wrote to the wrong place in the cache | The cache entry is completed before writing | 12 of 12 runs of DL-5 with 4 workers survive |
| `materialize` hung or crashed on small files | A counter was never released, and missing data was not checked | Counter released, checks added | 55 of 56 failing files now finish |
| Classification hung when individuals were merged | A shortcut for merged individuals could get stuck | Guard added | Fixed |
| Results changed from run to run | Things were ordered by memory address; one loop could run forever; the scheduler scanned too much | Fixed order, fixed loop, removed the scan; statistics only when asked (27 % less memory) | Stable |
| A true fact was sometimes missing | A later cache update threw away an asserted link | Earlier links are kept unless the update restates them | Fixed |
| A fact that is not true was sometimes written | A test treated one possible merge as certain | Results that depend on such a merge are confirmed by their own tests | Fixed |
| `sameAs` pairs in a different order | Members were written in the order they were found | Members are sorted before writing | Fixed |
| `materialize` slow when many individuals are linked to each other | The walk over the group was repeated for every individual | Each group is worked out once | DL-1: 820 s to about 2 minutes |
| **Many class assertions missing** (DL-1: 61 of 536) | Two shortcuts decided "not an instance" for many individuals without testing them, because they did not count neighbours that are stored only in the cache | Both shortcuts count those neighbours now | DL-1: 536, with 1 and with 4 workers |
| **No object property facts at all** for small ontologies that use nominals (a class like `{a, b}`) | The cache was never published, so realization found no data | If the cache still has unfinished individuals at that point, it is completed with the data it has | The five small repro files now write their facts |
| **Missed inconsistency** (`Irreflexive(t)`, `u` is the inverse of `t`, `u(a,a)`) | The check looked at the role but not at its inverse | The check looks at the inverse role too (also for asymmetric roles) | Found inconsistent |
| Asymmetric, irreflexive and disjoint-property errors not found | A check was skipped for individuals that point at each other | The check always runs now | Fixed |
| Facts of `sameAs` individuals were missing, and the output of the same file changed from run to run (`c0194`: 266 or 300 lines) | When individuals are the same, the realization sometimes had the facts of only one of them (it depended on timing which ones) | The writer now copies the types, object property facts and data values of every `sameAs` group to all its members. New option `Konclude.CLI.Materialize.CopyFactsToSameIndividuals` (on by default). `c0194` now gives the same 481 lines in every run and equals the Openllet facts | Missing facts against Openllet: 1,657 to 501 |
| A `sameAs` pair that is not entailed was written (`c0302`: `g0_9 = g1_0`) | Individuals that stay "not completely handled" in the cache get a forced completion. Their stored same-individual sets come from one possible choice of the search, not from a certain one, and the realization trusted them | Same-individual entries of such individuals (and entries that point to them) are not used. The group is then smaller but every written pair is entailed | 75 of 75 written pairs entailed. `c0314` still differs by one fact in some runs (`nomA u nomA`, which is entailed): when the nominal itself is unfinished its sameness is lost, depending on timing |
| Crash (stack overflow) in about 1 of 3 runs of a DL-5 subset | When a nominal's own node had been merged into another node, `expandIndividualNeighbourNodeFromBackendCache` called itself with the same arguments again and again (found with a depth counter: the same neighbour, representative and nominal repeated 200,000 times). Which run hit the case depended on the hash seed | The recursive call is only made when it continues with a different individual. A larger thread stack only delayed the crash, so I did not keep it | 20 of 20 runs of the same subset (seed 0, default stack) finish; before, 1 of about 10 crashed. One more place where the search could stop forever is gone |
| **A true subclass fact was missing in some runs of the class hierarchy of DL-5** (`T20CricketFan` is a subclass of `PeopleWithHobby`; the hierarchy depended on the hash seed: seeds 2 and 3 missed it). The same wrong answer is also in the old Konclude build from before my changes (a test for the class `T20CricketFan and not PeopleWithHobby` said "satisfiable") | `PeopleWithHobby` is defined as `Person and some likes Interest`. The nominal `T20Cricket` (a hobby) is taken from the cache of the individuals and then blocked (its successors are not expanded). Its universal restrictions (here the ones that come from the absorbed `some likes Interest`) stayed unprocessed in its queue. When the test linked a new node to the nominal, the restrictions were never applied to this node, so `PeopleWithHobby` was never derived and the clash was lost. Found with a trace of the one failing test and by switching the option `AllowBackendSuccessorExpansionBlocking` off (answer correct) | The retest of the synchronisation looks for this case: when a new node of the test (not an individual of the ontology) is linked to a blocked individual that has a universal restriction for that link, the individual is released from the blocking and processed. The cache neighbours are not affected, so the memory saving of the blocking stays | The test now answers "not satisfiable" for seeds 0, 2 and 3. All six seeds of the DL-5 classification give the same hierarchy (before: 2 different) |
| **Asserted facts were missing from the output** (DL-1: about 20 of the 731 asserted `dislikes` facts, a different set for every hash seed: 23 for seed 0, 17 for seed 3; they are plain input data). Also in the build from before my changes; with `AtLeastBackendNeighbourSatisfaction` off it was 160 facts (`dislikes`, `isCrazyAbout`, `loves`) | `likes` and `dislikes` are disjoint. For a person the rule "at least 3 hobbies" makes a successor and, through the 28-way `Interest` choice, merges it into a hobby individual. If the choice is the hobby that the person dislikes (an asserted link that is only in the cache), the new `likes` link must clash with the cached `dislikes` link. When an anonymous node is merged into a cached individual, the function that expands the cached links between the two (`expandBackendCacheIndividualNodesNominalMergingNeighbouringConnections`) looked up the neighbour labels of the cache with node IDs (negative for nominals) instead of individual IDs, so in some cases it found nothing and did not expand the link. The clash was missed, and the cache then stored the `likes` link instead of the asserted `dislikes` link. Found with traces of the cache update and of this merge for one lost fact (the lookup with the negated key found the label) | The lookups use the individual IDs | DL-1, seeds 0, 3 and 5: 0 asserted facts missing, and all three seeds write the same 1,426,298 facts (before 1,426,275 to 1,426,281). Time and memory unchanged (about 2.0 GB) |
| Statistics bookkeeping slowed deep searches (about 10 %) | After every finished test the whole decision tree up to the root was updated | Only the nearest 256 decisions are updated. New option `BranchingStatisticsAnalysingMaximumDepth` | About 10 % faster on DL-1, same answer |

## Features

| Feature | Status |
|---|---|
| `materialize` command | Works. Complete on DL-1 and DL-2, slow on DL-2, out of memory on DL-5 |
| Choose what `materialize` computes | Done. Six on/off options, all on by default (see below) |
| `AtLeastBackendNeighbourSatisfaction` option | Done, on by default, can be switched off |
| Validation of that option on generated data | OWL2Gen files (16): same answers, no cost difference (the case is never triggered); enumerated-hobby family: 1.6 to 1.9 times less memory, up to 7 times faster than the original, but the build without the option is slower than the original. Found: `CopyFactsToSameIndividuals` output grows with the square of a `sameAs` group (8 GB for a 200-individual file), not fixed, needs a decision |
| **Asserted types missing from `materialize` and `realization` on large ABoxes** | Found and fixed on 12 October. Above roughly 5,000 individuals (for the enumerated-hobby family) the output left out the asserted `type` of individuals whose item had only role data (new build about 860 of 5,000 persons, original build all but about 90). Fixed in the realizer (a flag on the individual item; concept queries fall back to the cache label). 5,000 and 10,000 persons are complete now at the same time and memory. Regression on the 231 small ontologies and OWL2Bench DL-1: see that page |
| Lint v2 | Done in the source: new finding "minimum cardinality over an enumerated class" with an estimate from the data (OWL2Bench DL-1: 1,983 of 2,488 individuals need new successors, about 4,990 nodes, 28 nominals), a check for a minimum larger than its enumeration, a `Suggestion:` line for every warning, JSON output (`-o report.json` or `KONCLUDE_LINT_JSON=1`). Not flagged: the generated OWL2Gen files and the plain hobby family without enumeration. Not yet calibrated against the measured costs |
| **Diagnose: what takes the time and memory** (`tools/konclude_diagnose.py`, profiler `KONCLUDE_PROFILE=1` inside the reasoner) | Runs a task with limits and reports the stages with memory, the concept expressions ranked by their share of the new nodes and branches (so a run that does not finish still shows what it was busy with), the matching lint findings and what to try; `docs/lint-and-diagnose.md`. DL-1: 89 % of the branches come from the 28-way hobby enumeration and the discipline disjunctions; DL-5 classification stopped at 70 s: 72 % / 21 % / 6 %. Off by default, no measurable cost, no crash over 231 files; 7 checks in `tests/diagnose/`. Lint's last line points to it |
| **Updates (OWLlink Tell / Retract)** | Bug found and fixed: after a `Tell`, existing individuals vanished from instance lists (new individuals got IDs that old ones already had). `IncrementalRebuild=on` no longer crashes (safe, correct; on the real PMD terminology it makes a period about 15 times cheaper, on small ontologies it is not faster; reuse of the previous model is behind the experimental option `ReuseConsistencyModel`, off, wrong for nominals). 18 checks in `tests/incremental/` in both modes (also fixed since: a retracted fact told again was ignored, a Retract followed by a Tell wrongly gave 'inconsistent', and a Retract of persons followed by a Tell of new persons crashed, all with `IncrementalRebuild=on`); 10,000-person update sequence 1.6 s (old release: 1,276 s, 11.7 GB). `docs/incremental-revisions.md` |
| **Examples of reasoning** (`tools/konclude_examples.py`) | Finds multi-step reasoning examples in an ontology: conclusions that are not written down, the smallest set of axioms behind each (`explain`), ranked by number and variety of axioms (definitions, restrictions, inverse or transitive properties, cardinalities, ...), written in plain English with the ontology's labels; `--classes` limits it to conclusions about classes you name, `--involving` also to examples where the class appears in the reasoning; similar examples are merged. Real run on the PMD creep-test ontology; 12 checks in `tests/examples/`; `docs/reasoning-examples.md` |
| **Live mode of the temporal layer** (`tlayer.py --live`) | One running Konclude with one knowledge base; per period only the changed facts are told/retracted. Same answers as the normal mode (20 checks + random timelines, also with inconsistent periods). Real PMD creep-test ontology, validation mode: 12 periods 22.8 s instead of 200 s (8.8 times), with retractions 5 times; open-world mode works but keeps the classification cost per period (helper classes). `docs/temporal-layer.md` |
| **Interval module** (`Konclude interval`) | New command, `docs/interval-module.md`: Allen relations and time stamps (PROV-O, OWL-Time) of the individuals, conflicts found by path consistency (also through composition, which OWL axioms cannot do) with a minimal set of facts, derived relations as N-Triples. Connected to `consistency` and `explain`: a conflict makes the ontology inconsistent with the minimal set in the report and the justification, and relations derived from time stamps are added (with the ontology's own property IRIs) so process-order rules see them (`Konclude.CLI.Interval.Consistency`, `AddDerivedRelations`). 20 checks in `tests/interval/` (10 command, 9 consistency/explain, random check against an exact ground truth: 200 of 200 verdicts agree); `consistency` verdicts unchanged on the 231 test ontologies (only `c0266`, an earlier fix), same total time; with the DL rules over process classes it found 18 of 18 violating timelines of 200 samples, whole-file `consistency` 0.1 s. Synthetic data only |
| **Temporal layer** (`tools/temporal/tlayer.py`) | Tool around Konclude, `docs/temporal-layer.md`: facts with validity times, snapshots at the change points, Konclude per snapshot, time formulas (`F G X U` and past), answers TRUE / FALSE / UNKNOWN / PENDING with witness periods and `explain` justifications, a component cache (3,000 people: 10.8 s against 42.0 s when reasoning is expensive, no gain when cheap). Deadline operators `F[a,b]`, `G[a,b]`, `O[a,b]`, `H[a,b]` (a missed deadline turns FALSE on its last day). **Temporal definitions** (`--define`): a class derived from a formula about the past goes back into the reasoner (class hierarchy, disjointness), with justifications that mark derived facts. **Explanation over time** (`--why`): the smallest set of atom values and periods that decides an answer. 20 checks in `tests/temporal/`; demo kit in `demo/`. Does not use Konclude's broken Tell/Retract. Synthetic data only |
| Konclude's revision mechanism (`Tell`/`Retract`, `IncrementalRebuild`) | Fixed (see the Updates row above). Real PMD terminology: a period costs 0.7 s on average with `IncrementalRebuild=on` instead of about 11 s per fresh process; answers identical in 12 of 12 periods. Open: reuse of the previous model (wrong for nominals), `owllinkfile` crashes if the knowledge base does not exist. See `docs/incremental-revisions.md` |
| GenACT benchmark with Konclude (cold windows) | First run: the generated data is inconsistent with the DL ontology (datatype ranges, exact cardinality of `hasWebsiteURL`); a DL reasoner needs seconds and 1 to 2.5 GB per window of a few thousand axioms |
| `BranchingStatisticsAnalysingMaximumDepth` option | Done, default 256 (a negative value means no limit) |
| `NominalSaturationMaxIndividualCount` option | Done, default 5000 (a negative value means no limit). The saturation of nominals is only used if the ontology has at most this many individuals. With more individuals it copied the concept sets of the hub nominals into every test: the DL-5 classification then needed either about 2 GB or more than 10 GB, depending on the hash seed. With the limit, six seeds of DL-5 finish with 1.9 to 3.4 GB (44 to 278 s) and give the same hierarchy. The classification time still differs a lot between seeds |
| Same output on every run | Done for the cases I found |
| Lint report (warns about expensive axioms and data) | Done, inside Konclude: `Konclude lint -i ontology.owl [-o report.txt]`. Same report as the old Java tool (compared on OWL2Bench DL-1/2/5/10 and EL/QL/RL-1 and on test files in four syntaxes: identical except that Konclude's RDF reader drops two axiom kinds, so the axiom count differs by 1 to 3). Needs no Java. DL-20: 20 s, 1.7 GB |
| Explanations (why inconsistent, why something follows) | Done, inside Konclude: `Konclude explain -i ontology.owl` (why inconsistent), `--subclass SUB SUPER`, `--type INDIVIDUAL CLASS`. It finds a minimal set of axioms by removing axioms while the result stays, and calls Konclude once per test. 19 test cases gave the expected sets; it found the 3 injected axioms in OWL2Bench DL-1 in 17 s (84 calls). It returns one minimal set, not all. Axioms the parsers ignore (`hasKey`, annotations) can never appear. I want to use it later for noisy data |
| Number of workers | Done: without `-w` Konclude decides (`SMART`): up to 4 workers for inputs up to 1 MB, 2 up to 8 MB, 1 above, and fewer if the available memory is small. `-w N` and `-w AUTO` work as before. On the 231 small random files the output with the default equals the output with `-w 1` in 229 of 230 files (the other one is `c0194`, which varied from run to run before the `sameAs` fix above; `c0162` is left out because it runs very long) |
| Memory limit with restart (`-m MB`, `AUTO` or `OFF`; Linux) | Done. For inputs larger than 4 MB (or with `-m`), the command runs in a child process that is stopped when it passes the limit and started again with another hash seed (up to 6 times). The result of a finished run does not depend on the seed. Tested on DL-5 classification |
| Reason on a small ABox and copy the results to similar individuals | Done, inside Konclude, for `materialize` and `realization` (OWL functional syntax and OWL/XML input; not for RDF/XML, Turtle, imports). Option `+=Konclude.CLI.Materialize.ReduceABox=off|on|auto` (default `off`; `auto` reduces when the ABox has at least 100,000 individuals or its estimated memory is above 50 % of the free memory, and at least 10 % of the individuals can be removed). A log line says how many individuals were merged. It is stricter than the old Java tool in six cases where that tool would write facts that do not follow (for example mixed roles between two groups). If the reduced ontology turns out to be inconsistent or the reasoner derives merges of its own, Konclude restarts itself without the reduction. Tested: 230 of 231 random files give the same output, 11 hand-made files all equal; not finished: DL-2 (it is RDF/XML, so it is not reduced) and the unreduced comparison for a 20,000-individual test file. Measured memory saving so far is small (11 % on DL-1 plus 2,000 added individuals, 2.15 GB against 2.41 GB) because the parsed axioms of the removed individuals stay in memory |
| Single-file Linux image | Script `tools/make-appimage.sh`. An image built on Ubuntu 24.04 needs Ubuntu 24.04 or newer. For older systems it must be built on Ubuntu 22.04 |

About the reduce-and-copy option (before it was inside Konclude): on a test file with 9,215 individuals only 22 were needed. Time went from 3.3 s to 0.19 s and memory from 312 MB to 38 MB, with the same output. On OWL2Bench it saves nothing, because the links are random.

### The options for choosing what to compute

Put the keys you want to change in a config file and pass it with `-c`. Every key is `true` by default.

| Key (all start with `Konclude.CLI.Materialize.`) | What it switches |
|---|---|
| `ClassAssertions` | the types of the individuals |
| `ObjectPropertyAssertions` | object property facts (the expensive part) |
| `DataPropertyAssertions` | data property facts |
| `SameIndividuals` | `owl:sameAs` groups |
| `ClassHierarchy` | subclass facts |
| `PropertyHierarchy` | subproperty facts |

```xml
<?xml version="1.0" encoding="UTF-8"?>
<RequestMessage xmlns="http://www.owllink.org/owllink-xml#">
  <Set key="Konclude.CLI.Materialize.ObjectPropertyAssertions"><Literal>false</Literal></Set>
</RequestMessage>
```
```bash
./Konclude materialize -w 1 -c types-only.xml -i ontology.owl -o inferred.nt
```

Object property facts need the class assertions to be computed first, so when you ask for object properties the types are computed too (but only written if you ask for them).

## Still open

| Item | What I know |
|---|---|
| `materialize` on DL-5, DL-10 and DL-20 runs out of memory | Two separate problems. (1) **Class classification** needs either about 2 GB or more than 10 GB, depending only on the order in which the ontology is loaded (the hash seed `KONCLUDE_QHASH_SEED`). DL-5 classification: seeds 2, 3 and 6 finish in 5 to 7 s, seeds 0, 1, 4, 5, 7 and 8 pass 4.5 GB within 100 s. The colleague's Konclude binary from September (before my changes) runs out of memory in about 1 of 4 runs, so my changes did not cause it. In the bad runs one test (for example `Interest` is a subclass of `AeronauticalEngineering`) pulls the neighbours of the hub individuals and then, through at-most restrictions on `enrollFor`, `enrollIn` and `takesCourse`, the whole connected ABox into one test graph: 20,000 and more neighbour expansions, about 4 MB each (many non-deterministic choices are kept as tasks). Things I tried that did not help: lowering `BackendCriticalNeighbourExpansionLimit` (it is only used while the individuals are precomputed and not in the class tests, so even a limit of 20 does not stop this blow-up; in the precomputation it earlier gave a wrong "inconsistent"), switching off the cache-writing options, expansion reuse and blocking options, trying the disjunct that the cached model used first, trying disjuncts that would force merges last, not forcing the processing of the expanded neighbours, ignoring the non-deterministic same-individual entries. The new `-m` option restarts with another seed when the limit is passed (about 1 to 2 minutes lost per bad attempt). What did help: the saturation of nominals was the part that made the tests so large; with the new default of `NominalSaturationMaxIndividualCount` (5000) the memory of the classification is 1.9 to 3.4 GB for all six seeds I tried (before: 6 of 9 seeds passed 4.5 GB). The time is still heavy-tailed (44 to 278 s for the six seeds, `KONCLUDE_QHASH_SEED` 0 to 5). The bigger files are not done: the classification of DL-10 does not finish in 600 s (6.3 GB for seeds 0 and 1), and DL-20 passes 9 GB after 226 s and 397 s (seeds 0 and 1). (2) **After the realization step** the memory grows from 4.8 GB to more than 11 GB within 2 minutes. I first thought this was the writing of the object property facts, but it happens in the same way with object property assertions, `sameAs` and its copying switched off (types and data only, run stopped at the 11 GB limit). So it is the computation of the types of the individuals (instance tests). With the fixes above I looked at it again on the DL-5 subsets (types and data only, `-w 1`): the smallest subset (about a quarter of the individuals) finishes in 77 s and 2.6 GB, the next one does not finish in 400 s (seeds 0, 1, 4 pass 4.8 to 6.3 GB; seeds 2 and 3 stay at 2.2 GB but are slow), the largest passes 8.5 GB in two minutes. A trace of the tests shows why: about 20,000 nodes in one batch and 3 to 4 non-deterministic decisions per node, all about the same disjunction. `explain` shows the reason: `NonScience` is equivalent to "not `Science`" and below `CollegeDiscipline`, which is a disjoint union of five classes, so every individual and every anonymous node of a test must be a college discipline (and an engineering node one of ten engineering kinds). Each decision is a branching task of about 270 KB. A smaller batch (`IndividualsPrecompuationSize` 100 or 300 instead of 1500) and a smaller default memory pool (8 KB instead of 50 KB, about 20 % less) did not change this; the memory grows with the number of decisions in a row. On DL-1 the same stage adds only about 0.2 GB. Separately, the output of the object property facts of DL-5 is very large: the lint command estimates about 52 million role assertions for the transitive and symmetric role `hasSameHomeTownWith` alone (DL-1: 1.3 million), so even without the memory problem the result file would be many GB. A name pool in the writer saves 27 % of the memory of the fact-writing stage on DL-1 (peak 2.21 GB to 2.03 GB). Not done yet |
| `materialize` blows up on small random ontologies when the ABox grows, if the TBox has an enumerated class | I took the 58 consistent ones of the 230 small random ontologies and repeated their ABox 2, 3, 5 and 10 times with renamed individuals (the TBox is unchanged; `D:\scratch_logs\scale_fuzz.py`). Files that need more than 30 s or 2 GB: 6 of 58 at 2 times the ABox, 14 at 3 times, 26 at 5 times, and (limit 120 s, 3 GB) 34 at 10 times (20 to 420 individuals). Of the files that finish, the time grows about 20 times for 10 times the data. All 34 files that blow up contain an enumerated class (`ObjectOneOf`, for example `E ≡ {nomA, nomB}` with many individuals asserted in `E`); the 4 files without one never blow up. Konclude's consistency check and classification need at most 0.5 s on these files, the cost is in the realization (the role facts take most of it: `c0003` at 10 times needs 40 s and 2.6 GB for 20 individuals). Openllet realizes 24 of these 34 files in at most 5 s (it ignores some transitivity and chain axioms, so this is a rough comparison). The ABox reduction option helps only partly: at 5 times the ABox, 5 of the 26 blow-ups finish (for example `c0134`: 45 s to 1 s), 14 still run into the 60 s limit, and in all 19 files that finish both ways the output is identical (10 of the reduced runs were killed when the machine ran out of memory, so the reduced numbers are not complete). The cost grows exponentially with the number of interchangeable individuals that must be one of a few nominals (`c0003`: 1.0 s, 1.8 s, 3 s, 4.7 s for 5, 6, 7 and 8 copies). Not solved |
| `materialize` is slow on big files | DL-2 takes about 19 minutes. The complete answer needs tests that the old shortcuts skipped. Other speed-ups are not done yet |
| One random file misses facts, and how many depends on the seed | `c0314` (287 or 288 lines). It does not depend on the hash seed or on ASLR: with the same seed, `-w 1` and ASLR off, 30 runs on several cores gave 21 and 9 runs of the two outputs, and with seed 2 the same split; pinned to one CPU core, 80 of 80 runs gave the same output (287 lines). So it is the timing of Konclude's internal threads. I checked with Konclude's own consistency test which facts follow: `nomA u nomA`, `nomB u nomB` and `sameAs` between `g1_0`, `g1_1` and `g1_2` are entailed (three individuals must differ from `g0_0` but the class `E` has only two nominals, so two of them are the same: a counting argument). The output has none of the `sameAs` pairs and `nomB u nomB` in every run, and `nomA u nomA` in some runs. So the output is not complete for merges that are forced only by such an argument over several possibilities; `materialize` finds the merges that it derives without a choice. Nothing in the output is wrong. A complete answer would need an entailment test for every pair of candidates. (`c0194` is fixed.) |
| One random file runs a very long search | File `c0162`: not a deadlock, one test searches for a very long time |
| Not enforced | `hasKey` (the parsers drop the axiom, so two individuals with equal keys are not merged and a violation is not reported). I checked on small ontologies that inverse-functional properties and qualified cardinality restrictions are enforced (merges and clashes are found), InferTest fails only for `hasKey` |
| Order of axioms | For a few ontologies the answer depends on the order of the axioms |
| Several workers on big files | Memory grows (idle workers try alternatives early). Use `-w 1` |
| Generated ontologies | One classification hang, and some searches that do a very large amount of work |
| Not checked yet | The other OWL2Bench profiles (EL, QL, RL) |

## How to try it

```bash
# materialize (one worker is the safest choice for big files)
./Konclude materialize -w 1 -i ontology.owl -o inferred.nt

# time and peak memory
/usr/bin/time -f "%e s, %M KB" ./Konclude consistency -w 1 -i OWL2DL-20.owl

# warnings about expensive parts of an ontology (no Java needed)
./Konclude lint -i ontology.owl
./Konclude lint -i ontology.owl -o report.json     # machine readable (level, title, detail, suggestion, examples)

# interval relations and time stamps: conflicts with a minimal explanation, derived relations
./Konclude interval -i timeline.ttl -o derived-relations.nt

# why is it inconsistent? why does A follow from B?
./Konclude explain -i ontology.owl
./Konclude explain -i ontology.owl --subclass "<http://ex.org/A>" "<http://ex.org/B>"

# large file: stop and restart with another hash seed if more than 9 GB are used (Linux)
./Konclude classification -m 9000 -i OWL2DL-5.owl -o hierarchy.xml

# optional: reduce interchangeable individuals, materialize, copy results back
./Konclude materialize -w 1 -i input.ofn -o output.nt +=Konclude.CLI.Materialize.ReduceABox=on
```
