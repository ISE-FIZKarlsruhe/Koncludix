# Problems, ideas and changes: the full story

This is the long version of `README-fixes-and-status.md`, for explaining the work to someone else. Each item has the same four parts: the problem, what I found, the idea, and the change.

## How I looked for problems

I did not guess. I used four ways of finding problems:

1. **Big real data.** OWL2Bench DL-1 to DL-20 (3,700 to 98,000 individuals). I watched time and memory.
2. **Random small ontologies.** I generated 231 of them and ran `materialize` on each. About 1 in 4 hung or crashed. A small file is easy to study.
3. **Shrinking a failing file.** When a file failed, I removed axioms one by one until only the few that matter were left. A 9-axiom file is much easier to understand than a 5,000-line one.
4. **Running the same file many times.** If the output changes between runs, something is depending on timing or memory addresses.

## The problems, one by one

### 1. One worker (`-w 1`) hung forever
- **Found:** even tiny files hung with `-w 1`, which is also the default when `-w` is not given.
- **Cause:** the program makes a pool of threads exactly as big as the number of workers, then permanently blocks one thread of it. With one worker nothing was left to do real work.
- **Idea:** make the pool one bigger.
- **Change:** pool size is now workers plus the blocked thread. `-w 1` works.

### 2. Big ABoxes ran out of memory
- **Found:** DL-5 needed more than 12 GB. I counted where the memory went. 231,000 decisions ("this person is one of 28 interests") were made on about 3,900 new nodes. Each decision copies memory.
- **Cause:** a rule like "a person with at least 3 hobbies" created 3 brand-new empty hobbies for every person, even though the hobbies were already in the data. Those new nodes then had to guess everything.
- **Ideas I tried:** delaying the rule (did not help); merging identical individuals (does not help, the links are random). What worked: look at the existing neighbours first.
- **Change:** the rule counts the neighbours that already exist, checks they are all different, and only creates the missing ones. New option `AtLeastBackendNeighbourSatisfaction` (on by default).
- **Result:** DL-1 34 s / 3.5 GB to 7 s / 1.8 GB. DL-5 no answer to 31 s / 2.1 GB. DL-20 169 s / 5.4 GB.
- **Check:** 185 files and 1,800 small tests gave the same answers. A deliberately broken version was caught.

### 3. Crash with several workers on big files
- **Found:** a random segfault with `-w 4`.
- **Cause:** a worker built a cache update from an old view of an individual. The update contained a link whose label was not in the individual's list of labels. The code looked up its position, got -1, and wrote before the start of an array.
- **Idea:** make the list of labels complete before writing.
- **Change:** extend the label with every missing link label first.
- **Result:** 12 of 12 runs of DL-5 with 4 workers survive.

### 4. `materialize` hung or crashed on small random files
- **Found:** 37 hangs and 17 crashes out of 231 files.
- **Causes:** (a) a counter of "work still to do" for roles was never released, so the program waited forever; (b) two places used data that does not exist for some individuals (for example a nominal that only appears in the TBox).
- **Change:** release the counter when role work is done; add the missing checks.
- **Result:** 55 of the 56 failing files now finish.

### 5. Classification hung when individuals were merged
- **Change:** a guard in the precomputation step for merged individuals.

### 6. Results and memory depended on memory addresses
- **Found:** the same file gave different results or timings between runs.
- **Causes:** hash tables ordered by pointer; a search loop that could run forever; a scheduler that rescanned the same tasks; per-task statistics allocated for every task.
- **Changes:** fixed ordering; the loop now remembers what it has seen; no rescanning; statistics only on request (about 27 % less memory).

### 7. Output that changed from run to run (the last fix)
Three different causes:
- **A missing fact.** An asserted self link was stored, then a later cache update thrown away. Fix: keep an earlier link unless the new update restates it.
- **Facts that do not follow.** After a non-deterministic merge, a test treated one possible merge as certain. Fix: results that depend on such a merge are confirmed by their own tests.
- **`sameAs` order.** The group was right but the pairs came in a different order. Fix: sort before writing.

### 8. Earlier fixes (before this round)
- Asymmetric, irreflexive and disjoint-property violations were not detected (a check was skipped for individuals that point at each other).
- `sameAs`, `differentFrom` and negative property assertions were not read from Turtle files.

## Things I added around Konclude
- **Lint** (`tools/KoncludeLint.java`): warns about axioms and data that are expensive, for example big enumerations, wide disjunctions, big cardinalities, cycles on transitive roles.
- **Explanations** (`tools/explain.py`): finds a small set of axioms that causes an inconsistency or an entailment, by removing axioms while the result stays the same.
- **ABox reducer** (`tools/AboxReduce.java`): keeps one of many identical individuals and copies the results back. It only pays off when thousands of individuals are really interchangeable. It saves nothing on OWL2Bench.

## What is still open
See "What I am still working on" in `README-fixes-and-status.md`. The biggest item is that `materialize` is slow when a transitive and symmetric role links large groups of individuals.
