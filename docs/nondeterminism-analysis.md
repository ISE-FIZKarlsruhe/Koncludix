# Where the non-determinism is in Konclude, and how it differs from Openllet

This is what I measured and found. Every number comes from a run on this machine (WSL, one reasoner run at a time unless it says otherwise). I say clearly what I did not verify.

## 1. Four different things are called "non-determinism"

| Kind | What changes between two runs of the same file | Does it matter? |
|---|---|---|
| A. Output order | The order of the lines or of the items in a printed set | No, sort before comparing |
| B. Work | How many decisions, tasks or seconds the search needs | Yes for speed and memory |
| C. Answer | Which facts are in the result | Yes, this is a correctness problem |
| D. Hang or crash | The run does not finish or dies | Yes |

Openllet shows A and B. Konclude shows B, and also C and D (which I found and partly fixed).

## 2. Konclude: the layers, from the bottom up

### Layer 1. Per-process randomness (found and fixed)
- Qt gives every process a random seed for its hash tables, and Konclude derived the hash of many expressions from **memory addresses**. The numbering of the concepts after the ontology was built therefore differed in every run. The search order depends on this numbering (the operand lists are sorted by concept tag and the disjuncts are tried in that order), so the same file needed between **1.6 thousand and 1.76 million** OR-decisions and 0.07 to 13 s (before the fix, same file, same flags). On some ontologies about 1 run in 3 hung (a loop in the branch trigger preprocessing, a separate upstream bug that I also fixed).
- Fix (already in the build): hashes use a creation serial instead of the address; `qSetGlobalQHashSeed(0)` in `mainLoader.cpp` by default (`KONCLUDE_QHASH_SEED=n`, or `-1` for Qt's random seed); the cache tag permutation (`CCacheTaggingPool.cpp`) and the skip lists use a local xorshift generator. Result: concept tables identical in 6 of 6 runs, and 18 of 18 (ontology, file order) combinations repeat exactly.

### Layer 2. Sensitivity to the input order (deterministic, but chaotic)
After layer 1, one seed and one file order give one search. A different seed or order is a **different search**, and the cost can differ enormously:
- DL-5 classification, seeds 0 to 5: 41 to 517 s and 1.9 to 3.4 GB (before my change, 6 of 9 seeds passed 4.5 GB). The same seed can differ a little between builds, so I give the range.
- On nominal-heavy generated files, 99 % of the executed tasks are in failed subtrees, and 1 to 8 early wrong picks cause most of it; the same file needs up to about 500 times more tasks in another order.
- The seed also decides **whether a bug shows up**. With nominal saturation off, the hierarchy of DL-5 lacked `T20CricketFan ⊑ PeopleWithHobby` for seeds 2 and 3 only (5 of 5 repeats per seed, so this part is deterministic). The lost `dislikes` facts of DL-1 appeared only when the search chose the disliked hobby (23 lost for seed 0, the same 23 in a second run; 17 for seed 3). Both were real incompleteness bugs that the seed only uncovered.

### Layer 3. Thread timing (still there)
- **Evidence (today):** `c0314`, `materialize`, same file and flags, 30 runs each:

| Setting | Outputs (runs) |
|---|---|
| seed 0, `-w 1`, ASLR on | 17 / 13 |
| seed 0, `-w 1`, ASLR off | 21 / 9 |
| seed 0, `-w 4` | 15 / 15 |
| seed 2, `-w 1`, ASLR on | 15 / 15 |
| seed 2, `-w 1`, ASLR off | 21 / 9 |
| seed 0, `-w 1`, ASLR off, **pinned to one core** | **40 / 0 and 40 / 0** |

- So the remaining variation does **not** come from the hash seed or from ASLR. It disappears when the threads cannot run at the same time. Konclude has several threads (from the class names: reasoning manager, scheduler, processing, completion, realization, query); even with `-w 1` the process has three threads in the profiler, although one does almost all the work during the tableau phases. The others hand work and results over by messages, and the order of these messages depends on timing (I did not trace this).
- What the timing changes: in which order individuals are completed and written to the cache, and which possible-instance tests run before others. In `c0314` the two outputs differ by an entailed fact (`nomA u nomA`), because the nominal is sometimes still unfinished in the cache when the realizer asks for it. This is a different **answer**, not a different order.
- Other timing effects I found earlier: with `-w 4` the DL times varied from 28 to 120 s, and a scheduler loop (a dead O(n) scan) stalled about 5 % of the runs (fixed).
- Not verified: which exact pair of threads and which shared structure decide it. Pinning to one core only shows that interleaving is the cause.

### Layer 4. History dependence of the cache
The backend cache is updated while the reasoning goes on (individuals are completed in batches, labels are replaced, "forced completion" fills in unfinished individuals). The cache content after a step depends on what was computed before. This is why a wrong update (for example the ID sign bug) only damages the runs in which the search happens to walk through it.

### Output order
I sort the `sameAs` members before writing. The other lines are written in hash order, so **always sort before comparing two outputs** (my comparisons do).

## 3. Openllet: what I measured today and before

- **Source:** Openllet runs in a JVM on OWLAPI. OWLAPI hands the same axiom set to the reasoner in a **different iteration order in every JVM run** (checked causally on GALEN with OWLAPI 5.5.1 and 5.1.19: same hash codes, only the order changes), and Java's hash sets iterate in an order that depends on identity hash codes. The reasoner itself is single-threaded (parallel streams are off). I ruled out garbage collection and the loader (Jena or OWLAPI, RDF/XML or OWL/XML).
- **What varies:** the work. GALEN: 302 thousand to 500 thousand branch attempts, coefficient of variation about 20 %, every run different. On blow-up ontologies the order changes the work by a median of 25.8 times, and 10 of 72 generated ontologies flip between solved and timeout by order alone.
- **What does not vary:** the answer. The class hierarchy was identical in every run I checked.
- **Today:** `realize` on `c0314`: 8 runs gave 2 outputs; `c0025` (8 copies): 6 runs gave 2 outputs and times from 11.8 to 17.0 s. I compared the two outputs of each file: they are **identical if the order of the items inside a printed set is ignored**. `c0003` (8 copies): 1 output.
- **Control:** with the axioms fed in a fixed order the spread falls to 0 to 1.6 %. Which order is chosen still changes the work (293 to 498 thousand), so the order, not the randomness, is the real lever.
- **HermiT** (earlier): the work varies by only about 1.3 % on GALEN, much less order-sensitive than Openllet.

## 4. The comparison

| | Konclude | Openllet |
|---|---|---|
| Source | Thread timing (still), seed and order of the search (controllable), address-based hashes (fixed) | Axiom iteration order of OWLAPI and identity hash codes |
| Threads involved | Yes: several internal threads, even with `-w 1` | No (single-threaded reasoning) |
| Can the **answer** change? | Yes: facts missing in some runs (`c0314`, earlier the lost asserted facts and the missing subclass fact). Every fact I checked was entailed, so runs are incomplete in different ways, not wrong | Not in what I measured (hierarchy identical; only the order of printed items differs). It does ignore some axioms (transitivity and chains), which is a separate, deterministic gap |
| Can the **time/memory** change? | Yes, a lot (up to 1000 times before the fix, 44 to 517 s for DL-5 seeds today) | Yes, 20 % typical, much more on blow-ups |
| Cause is controllable? | Seed: yes. Order: yes. Thread timing: only by pinning to one core | Order: yes, with a fixed-order driver |
| Reproducible run | Same seed, same file order, **one core** (`taskset -c N`), then identical (80 of 80) | Same fixed axiom order (and same identity hash sequence) |
| Crash or hang | Hangs and a segfault in backtracking existed (the loop is fixed; I reproduced the backtracking segfault on a pristine build and have not fixed it) | A `ConcurrentModificationException` and an `IllegalStateException` seen in 6 runs of generated files |

The main difference: **Openllet's randomness changes how long it takes; Konclude's also changes what it finds.** That is because Konclude's result depends on a pipeline of threads and a cache that is filled while the search runs, and some of its paths are deliberately incomplete (forced completion of unfinished individuals, facts that need a choice). Openllet's search is exact but its order is random.

## 5. What this means in practice

- Compare outputs **after sorting**, and run several seeds. For Konclude use `KONCLUDE_QHASH_SEED=0..n` and report the spread, not one run.
- For an exact repeat: same seed, same file, `taskset -c N`, `-w 1`.
- The facts that appear in one run and not in another are all entailed (checked for `dislikes` in DL-1 and for `c0314`). So the **union of several runs is sound and more complete**; a deterministic mode that waits for unfinished individuals would be better.
- The non-determinism was useful: every seed-dependent result I followed up (the T20 hierarchy, the lost `dislikes` facts) pointed to a real bug.
- Not built (needs your OK): a deterministic mode that serializes the realization threads, and a "run N seeds and merge" option.

## 6. Future work: make the cost independent of the order (remember dead ends)

Status: **not built.** The numbers below come from recorded runs, not from a finished feature.

**Words used here.** A *guess* is a choice between alternatives, for example "A or B". A *clash* is a contradiction that shows a guess was wrong. A *dead end* is a branch of the search that ends in a clash.

### 6.1 The problem in plain words
- The same ontology always gives the same **answer**, but the **time and memory** depend on the order in which the file is read and on the seed. DL-5: 41 to 517 seconds. Some generated files need up to 500 times more work in a different order.
- Why: when the reasoner meets "A or B" it has to guess. If an early guess is wrong, it only finds out much later, and all the work after the guess is thrown away.
- We measured on generated files with many such choices: about 99 % of the work is spent in branches that end in a clash. Only 1 to 8 early wrong guesses cause most of it. The same wrong choice is tried again and again (about 127 times).

### 6.2 What does not solve it
- Choosing the guesses more cleverly (with embeddings or language-model hints): no gain.
- Looking a little ahead before choosing: the first events do not show which alternative is right.
- Switching a speed-up off or on: the one big speed-up we found gave **wrong answers** (it said "consistent" for 82 of 89 inconsistent files), so we rejected it.
- Starting again with another order after a fixed time: helps sometimes, but a fixed time is arbitrary (a large ontology needs longer). A time that grows (30 s, 60 s, 120 s, ...) is better, but it only works around the problem.
- Remembering clashes at single named individuals (Konclude does not do this today): same answers on all 136 test files, and about 24 % less work on average. Real, but only part of the gain.

### 6.3 The idea: remember why a guess failed
Picture a maze. Today the reasoner walks into a dead end, comes back, and later walks into the same dead end again from another side. The idea is to **write down what made the dead end** ("this guess together with that guess always fails"), and never walk into it again.

In steps:
1. When a clash happens, find out which earlier guesses caused it. Konclude already knows this (it needs it to jump back to the right place).
2. Write it down as a short note: "these guesses cannot all be true together".
3. Before the reasoner starts a new branch, it checks the notes. If the branch would repeat a failed combination, it skips the branch.
4. Throw away old notes that are never used, so memory stays small.

The effect: a wrong early guess is found wrong **once**. After that the order of the file matters much less. The technical name is *conflict learning*; SAT solvers use the same idea and it is one reason they got so fast.

### 6.4 What we measured so far
- From real runs we collected **3.67 million** such notes from Konclude's own data. **None was wrong** when checked against the final answer.
- Only about **5 %** of them were different from each other, so a small store is enough.
- If the notes had been used, an **estimated 96.8 %** of the work in heavy runs (66 % in light runs) would have been skipped. This is an estimate from replaying the runs, not a measurement of a real feature.
- A first simulation said 99.8 %. It was wrong (the notes were incomplete) and we withdrew it.

### 6.5 Why it is not built yet (the risks, in plain words)
- **It must never give a wrong answer.** If a skipped branch does not pass on *why* it failed, the reasoner may later skip too much and say something false. This is the main danger, so every test of correctness comes before any speed test.
- **The search creates new individuals while it runs**, so the notes must name them in a way that stays the same. That is easy for named individuals that are fixed in the file and harder for the ones the reasoner makes up. So the first version handles **only the fixed named individuals**, which is also where we measured the waste.
- **It has to work together** with Konclude's other speed-ups and with several workers.
- **It helps only some ontologies.** It helps ontologies with a lot of "or" choices. It does **not** help CTO (its choices are almost never wrong: 25 clashes in 650,000 choices) and probably **not** the DL-5 memory problem, which looks like copying data rather than guessing. We have not shown that yet.

### 6.6 Plan
1. **Done:** record the notes from real runs and replay them (numbers above).
2. **Build a first version** behind an option that is **off by default**: fixed named individuals only, small store, one worker.
3. **Check correctness first:** all 89 inconsistent test files must still be found inconsistent, the 136 earlier answers must be identical, plus the standard tests (28 + 9), 231 random small ontologies, and a comparison with a second reasoner. A speed-up that fails one of these is thrown away.
4. **Then measure:** is the difference between the best and the worst order much smaller (today up to about 500 times)? How much faster on heavy generated files, OWL2Bench DL and DL-5?
5. **Later:** more kinds of guesses, several workers, and decide whether it can be on by default.

Effort for a careful first version: several weeks.

### 6.7 Until then
- Restarts with a growing time limit, together with the existing memory limit (`-m`).
- For an ontology that is reasoned about again and again (our updates), reuse the seed that worked.
- Try several seeds at the same time if there is enough memory.
- Always report the spread over several seeds, never a single run.

### 6.8 Open questions
- How much of the cost on **real** ontologies is wasted guessing? In our check of 21 real ontologies most needed no guessing; GALEN wasted about 4.6 % of its first guesses and Wine about 4.3 %. We need real ontologies with a lot of guessing to show a benefit outside generated files.
- Does it help the DL-5 memory problem at all?
- Can several workers share the notes safely?
