# Koncludix: what I fixed, what works now, what is still open

Koncludix is my version of the Konclude reasoner. 

Goal is to improve memory requirements, add additional feaures that aren't yet supported such as explnation, lint, materialize dependign on required inference type such as inverse, transitivity, hierarchy etc etc.

Additionally, adding support for approximate reasoning.

## Test data

- OWL2Bench DL files (DL-1 to DL-20): my shared folder with the OWL2Bench <https://github.com/kracr/owl2bench> data sets: <https://drive.google.com/drive/folders/1HYURRLaQkLK8cQwV-UBNKK4_Zur2nU68?usp=sharing>
- 231 small random ontologies made for testing (hangs, crashes and output changes) using OWL2Gen <https://github.com/kracr/owl2gen> -- more to be added
- InferTest (28 entailment tests and 9 inconsistency tests).

## Why Konclude is fast

| Idea | In simple words |
|---|---|
| Quick first pass | The easy part of the ontology is finished first, like ELK does |
| Memory of results | It does not repeat work it has already done |
| Smart back-jumping | After a failed guess it goes back to the guess that caused it |
| Cached descriptions | Similar individuals share what is known about them |
| Many cores | Several workers share the work |

## Results

"Before" means before my changes. "Now" is the final build.

### Consistency check (1 worker)

| File | Individuals | Before | Now |
|---|---|---|---|
| DL-1 | 3.7K, 50K | 34 s, 3.5 GB | 7 s, 1.8 GB |
| DL-2 | 7K, 99K | no answer in 900 s | 26 s, 3.7 GB |
| DL-5 | 23K, 325K | no answer in 12 GB | 31 s, 2.1 GB |
| DL-10 | 50K, 711K | no answer in 12 GB | 80 s, 3.3 GB |
| DL-20 | 98K, 1.4M | no answer in 12 GB | 169 s, 5.4 GB |

### `materialize` (writes all facts)

| File | Before | Now | Facts written |
|---|---|---|---|
| DL-1 | 820 s, 3.7 GB (4 workers) | 126 s, 3.7 GB (4 workers); 140 s, 2.2 GB (1 worker) | 1.44 million |
| DL-2 | not tried | 773 s, 5.1 GB (1 worker) | 5.0 million |
| DL-5 | not tried | **fails: out of memory** (stopped by the system at 10.9 GB while classifying) | none |
| DL-10, DL-20 | not tried | not tried yet | |

On DL-1 the output with 4 workers has exactly the same facts as before. Only the order in which equivalent class and property names are written can differ.

### Other tests

| Test | Before | Now |
|---|---|---|
| 231 random small ontologies, `materialize` | about 1 in 4 hung or crashed (37 hangs, 17 crashes) | 1 still hangs, 1 changes output now and then, 229 give the same output every time (5 runs each) |
| InferTest, each test run alone | 25 of 28 and 8 of 9 | 27 of 28 and 8 of 9 (only the `hasKey` inconsistency test fails) |
| The two small repro files, run 24 times | output changed | same facts every time |
| Cardinality change: 185 files and 1,800 small tests | not applicable | same answers with the option on and off |

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
| `materialize` slow when many individuals are linked to each other | The walk over the group was repeated for every individual | Each group is worked out once | DL-1: 820 s to 126 s |
| Asymmetric, irreflexive and disjoint-property errors not found | A check was skipped for individuals that point at each other | The check always runs now | Fixed |

## Features

| Feature | Status |
|---|---|
| `materialize` command | Works. Fast on DL-1, slow on DL-2, out of memory on DL-5 |
| `AtLeastBackendNeighbourSatisfaction` option | Done, on by default, can be switched off |
| Same output on every run | Done for the cases I found |
| Lint report (warns about expensive axioms and data) | Separate tool (`tools/KoncludeLint.java`). Plan: build it into Konclude |
| Explanations (why inconsistent, why something follows) | Separate tool (`tools/explain.py`). Plan: build it into Konclude. I want to use it later for noisy data |
| Reason on a small ABox and copy the results to similar individuals | Separate tool (`tools/AboxReduce.java`, `tools/materialize-reduced.sh`). Plan: make it an option inside Konclude |

About the reduce-and-copy tool: on a test file with 9,215 individuals only 22 were needed. Time went from 3.3 s to 0.19 s and memory from 312 MB to 38 MB, with the same output. On OWL2Bench it saves nothing, because the links are random.

## Still open

| Item | What I know |
|---|---|
| `materialize` on DL-5 runs out of memory | The consistency check needs only 2.1 GB, but `materialize` passes 10.9 GB while classifying. Not looked into yet |
| One fact missing with 1 worker on DL-1 | One person is not listed as `PeopleWithManyHobbies` (60 instead of 61). With 4 workers it is found. Being checked |
| DL-2 `materialize` is slow | 773 s. Not looked into yet |
| One random file hangs | `materialize` stops at "Realizing ontology" (file `c0162`). It also happens when I undo my latest fixes |
| One random file misses a fact now and then | File `c0314`. The same test gives different answers on different runs |
| Not enforced | `hasKey`, inverse-functional properties, qualified cardinality |
| Order of axioms | For a few ontologies the answer depends on the order of the axioms |
| Several workers on big files | Memory grows (idle workers try alternatives early). Use `-w 1` |
| Generated ontologies | One classification hang, and some searches that do a very large amount of work |

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
