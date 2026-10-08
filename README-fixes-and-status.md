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
| DL-5 | not tried | **fails: out of memory** (the system stopped it at 10.9 GB while classifying) | none | |
| DL-10, DL-20 | not tried | **fail: out of memory** in the classification step (stopped at 10.5 GB after 161 s and 207 s) | none | |

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
| 231 random small ontologies, `materialize` | about 1 in 4 hung or crashed (37 hangs, 17 crashes) | 229 give the same output every time (5 runs each), 2 still change now and then, 1 runs a very long search. No file lost any fact compared with the old build, 9 files gained facts |
| Verdict (consistent or not) on the 231 files | | 1 changed (`c0266` is now correctly inconsistent, as Openllet says) |
| InferTest, each test run alone | 25 of 28 and 8 of 9 | 27 of 28 and 8 of 9 (only the `hasKey` inconsistency test fails) |
| The small repro files, run 24 times | output changed | same facts every time |
| Cardinality change: 185 files and 1,800 small tests | not applicable | same answers with the option on and off |
| Comparison with Openllet on 51 consistent random files | not done | found the two problems that are fixed now (see below) and one that is still open |

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
| Statistics bookkeeping slowed deep searches (about 10 %) | After every finished test the whole decision tree up to the root was updated | Only the nearest 256 decisions are updated. New option `BranchingStatisticsAnalysingMaximumDepth` | About 10 % faster on DL-1, same answer |

## Features

| Feature | Status |
|---|---|
| `materialize` command | Works. Complete on DL-1 and DL-2, slow on DL-2, out of memory on DL-5 |
| Choose what `materialize` computes | Done. Six on/off options, all on by default (see below) |
| `AtLeastBackendNeighbourSatisfaction` option | Done, on by default, can be switched off |
| `BranchingStatisticsAnalysingMaximumDepth` option | Done, default 256 (a negative value means no limit) |
| Same output on every run | Done for the cases I found |
| Lint report (warns about expensive axioms and data) | Separate tool (`tools/KoncludeLint.java`). Plan: build it into Konclude |
| Explanations (why inconsistent, why something follows) | Separate tool (`tools/explain.py`). Plan: build it into Konclude. I want to use it later for noisy data |
| Reason on a small ABox and copy the results to similar individuals | Separate tool (`tools/AboxReduce.java`, `tools/materialize-reduced.sh`). Plan: make it an option inside Konclude |
| Single-file Linux image | Script `tools/make-appimage.sh`. An image built on Ubuntu 24.04 needs Ubuntu 24.04 or newer. For older systems it must be built on Ubuntu 22.04 |

About the reduce-and-copy tool: on a test file with 9,215 individuals only 22 were needed. Time went from 3.3 s to 0.19 s and memory from 312 MB to 38 MB, with the same output. On OWL2Bench it saves nothing, because the links are random.

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
| `materialize` on DL-5, DL-10 and DL-20 runs out of memory | Not `materialize` itself: the plain class classification step already uses more than 10 GB on DL-5 (DL-1: 0.3 s, DL-2: 0.9 s). The tableau tests of the classification load the neighbours of the nominal hub individuals (the 28 interests, linked to thousands of persons) into every test graph, up to the limit `BackendCriticalNeighbourExpansionLimit` (default 15000). With the limit lowered to 2000 the memory stays small (1.5 GB), but then the reasoner wrongly says DL-5 is **inconsistent** (with `AtLeastBackendNeighbourSatisfaction` switched off it says consistent). So I do not use a lower limit. The wrong clash is on a student who is asserted `Man` and gets `Woman` from the definition `WomenCollege = College and (all hasStudent are Woman)`, i.e. a choice that is only one possibility is applied as if it were certain. The `Woman` comes from the cached concept set of that student: it contains a stored (certain) "automaton" concept, the kind used for restrictions over role chains, and processing it on the student yields `Woman`. Leads: when the limit cuts the propagation, the cache installs the propagated concept directly into the cached description of the neighbours as a certain concept (`updatePropagationCutIndividualIncompletelyHandled`); with the lowered limit this happens 44 times on DL-5, with the default never. The concept is an automaton concept of a role chain. Also, in `expandDirectlyInfluencedNeighboursWithPropagation` a callback parameter hides the flag that says whether the concept itself is non-deterministic. Making that flag reach the cut data did not change the verdict, so the cause is not found yet. Switching the related backend options one by one does not help either, and neither does switching off any of the cache-writing options (completion graph, expansion, saturation: memory still passes 10.5 GB within 100 s). Switching off the direct installation makes the reasoner crash. Not fixed yet |
| `materialize` is slow on big files | DL-2 takes about 19 minutes. The complete answer needs tests that the old shortcuts skipped. Other speed-ups are not done yet |
| Two random files miss facts in some runs | Files `c0194` and `c0314`: a few entailed facts are written in some runs and not in others. They are ontologies with nominals, where some individuals stay unfinished in the cache |
| One random file runs a very long search | File `c0162`: not a deadlock, one test searches for a very long time |
| Not enforced | `hasKey`, inverse-functional properties, qualified cardinality |
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

# warnings about expensive parts of an ontology
java -cp tools/openllet.jar:tools KoncludeLint ontology.owl

# why is it inconsistent?
python tools/explain.py ontology.owl --konclude ./Konclude

# optional: reduce identical individuals, materialize, copy results back
tools/materialize-reduced.sh input.owl output.nt ./Konclude
```
