# Koncludix: what I fixed, what works now, what is still open

Koncludix is my version of the Konclude reasoner. I use it mostly for the `materialize` command: it reads an ontology with lots of individuals (an ABox) and writes out every fact that follows from it.

Konclude is very fast, but on large or unusual data it hung, crashed, ran out of memory, or gave a different answer each time I ran it. This page explains why Konclude is fast, what went wrong, what I changed, how I tested it, and what is still not fixed.

## Why Konclude is fast

- **It does the cheap work first.** A quick pre-pass settles most of the simple parts of an ontology. The expensive search only runs for the hard parts.
- **It remembers things.** If it has already shown that some combination of conditions is impossible, it never works that out again.
- **It reuses the description of an individual.** Individuals that look alike share one cached description instead of being searched again.
- **It jumps back smartly.** When a guess fails, it goes straight back to the guess that caused the failure, not to the one just before it.
- **It uses several CPU cores.**

That is also why running it a second time on a file that already contains the inferred facts is quicker: the facts are simply asserted, so there is less to work out.

## What went wrong, and what I did about it

| What I saw | Why it happened | What I did |
|---|---|---|
| `-w 1` (one worker) hung forever, even on tiny files | The thread pool had exactly as many threads as workers, and one thread was always blocked | The pool is now one thread bigger. `-w 1` works and is the best setting for big ABoxes |
| Big OWL2Bench files ran out of memory | A rule like "at least 3 hobbies" made new, empty neighbours for every person, although the hobbies were already in the data. Each of them started a huge search | The rule now counts the neighbours that already exist and only creates the missing ones. New option `AtLeastBackendNeighbourSatisfaction`, on by default |
| Crash with several workers on big files | A worker wrote into the cache with an out-of-date view of an individual, and wrote outside its array | The cache entry is now extended first, so the write is valid |
| `materialize` hung or crashed on small random ontologies | A counter was never released, and two places did not check for missing data | Release the counter, add the checks |
| Classification hung when individuals were merged | A shortcut for merged individuals could get stuck | Added a guard |
| Output order and results changed from run to run | Some data structures were ordered by memory address; one search loop could run forever; the scheduler wasted time scanning | Fixed ordering, fixed the loop, removed the scan. Per-task statistics are only kept on request (about 27 % less memory) |
| **Output differed between runs** (see the next section) | Details below | Three fixes |

## The output that changed from run to run

This was the last problem I fixed. It had three different causes.

1. **An asserted fact was sometimes missing.** Example: `g r g` (an individual linked to itself by a transitive role) was in the input, but sometimes not in the output. The cache first stored it. A later update, built from the reasoner's search graph, did not contain that link, and the update then threw the stored link away. Whether this happened depended on the order of the updates. Now the cache keeps an earlier link unless the new update really re-states it.
2. **Facts that do not follow were sometimes written.** When a cardinality limit forces two individuals to be merged, there are several ways to do it. A test for one individual used one of these merges as if it were certain, so everything reachable from it looked certain too. Now results that depend on such a merge are not trusted. Each one is confirmed by its own test.
3. **The same `sameAs` group was written in different orders.** The group was always right, but the pairs came out in a different order. The members are now sorted before writing.

After the fixes, my two small repro files give the same output in 24 out of 24 runs (the lines of the file can still be in a different order, the content is the same).

## How I tested

- **OWL2Bench DL-1 to DL-20** (3,700 to 98,000 individuals), one worker, consistency check: DL-1 went from 34 s / 3.5 GB to 7 s / 1.8 GB. DL-5 went from "no answer in 12 GB" to 31 s / 2.1 GB. DL-10 takes 80 s / 3.3 GB and DL-20 takes 169 s / 5.4 GB.
- **231 random small ontologies** (a fuzz test). At the start about 1 in 4 hung or crashed on `materialize`. Now one still hangs (see below). I also run each file 5 times and compare the output. Result: 229 of 231 files give the same output every time, one still changes (`c0314`) and one still hangs (`c0162`).
- **Soundness checks for the cardinality change:** 185 files and 1,800 small targeted tests gave no different answers. A deliberately broken version was caught, so the tests do detect mistakes.
- **InferTest** (28 entailment and 9 inconsistency tests, each run alone): 27 of 28 and 8 of 9 pass. Only the `hasKey` inconsistency test fails.
- **Generated ontologies from my generator** (different TBoxes and ABox sizes): old and new Konclude give the same answers. Most of the generated ones are inconsistent or very hard, so they are not good speed tests.
- **Repeat runs** of the repro files, 24 times each, to check the output is stable.

## What this version supports

| | Before my changes | Koncludix now |
|---|---|---|
| `materialize` command | hung or crashed on about 1 in 4 small random files | no longer does on the cases I found |
| `-w 1` | hangs | works |
| Big ABoxes with "at least n" restrictions | huge memory | much smaller memory (option can be switched off) |
| Same output on every run | no | yes for the cases I found (open issues below) |
| Lint (warns about axioms and data that are too expensive) | not available | `tools/KoncludeLint.java` |
| Explanations (why is it inconsistent / why does this follow) | not available | `tools/explain.py` |
| Keep one of many identical individuals and copy its results to the others | not available | `tools/AboxReduce.java` and `tools/materialize-reduced.sh`, optional |

Lint, explanations and the reducer are separate tools around Konclude, not Konclude commands. The reducer only pays off when there are thousands of interchangeable individuals. On a test file with 9,215 individuals it cut them to 22 and the output was identical. On OWL2Bench it saves nothing, because the links there are random.

## What I am still working on

- **One hang remains** in the random test (file `c0162`): `materialize` stops at "Realizing ontology". It still happens when I undo my latest fixes, so they did not cause it.
- **One output still changes** (file `c0314`): in about 4 runs out of 10 one entailed fact is missing. The same test on the same data answers differently between runs. I have not found the cause.
- **Generated ontologies:** a classification hang on one of them, and the search sometimes does a very large amount of work on others.
- **Not enforced yet:** `hasKey`, inverse-functional properties and qualified cardinality. Konclude does not apply them, so some merges are missed.
- **Order of axioms:** for a few ontologies the verdict depends on the order of the axioms.
- **Several workers on big files** still use a lot of memory, because idle workers explore alternatives early. Use `-w 1` for big ABoxes.
- **Transitive and symmetric roles** make a huge output and are slow (DL-1 `materialize` takes about 800 s, nearly all of it for that closure).

## How to try it

```bash
# materialize (one worker is the safest choice for big files)
./Konclude materialize -w 1 -i ontology.owl -o inferred.nt

# warnings about expensive parts of an ontology
java -cp tools/openllet.jar:tools KoncludeLint ontology.owl

# why is it inconsistent?
python tools/explain.py ontology.owl --konclude ./Konclude

# optional: reduce identical individuals, materialize, copy results back
tools/materialize-reduced.sh input.owl output.nt ./Konclude
```
