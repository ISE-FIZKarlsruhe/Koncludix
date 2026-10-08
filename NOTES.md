# Notes on Konclude

## 1. How is Konclude different from other reasoners?

- It does a quick first pass before the real reasoning, like ELK does. The easy part of the ontology is finished in that pass.
- It remembers results, so it does not repeat the same work.
- It can use many CPU cores.

## 2. It is fast, but it needs a lot of memory


1. Wrote down every guess the reasoner makes.
   - Almost all guesses were the same kind: "which hobby is this?"
   - They were made on thousands of empty hobby nodes.
2. Counted how much memory each guess used.
   - Every guess copies some memory, so thousands of guesses add up to gigabytes.
3. Took the hobby links out of the file and ran it again.
   - Memory dropped from 3.5 GB to 1.1 GB.
   - So the hobbies were the cause.
4. Changed the rule "at least 3 hobbies".
   - Before: it made new empty hobbies for every person, even when the hobbies were already in the data.
   - Now: it counts the hobbies that are already there and makes only the missing ones.

Result (consistency check, 1 worker):

| File | Before | Now |
|---|---|---|
| DL-1 | 34 s, 3.5 GB | 7 s, 1.8 GB |
| DL-5 | no answer in 12 GB | 31 s, 2.1 GB |
| DL-10 | did not finish | 80 s, 3.3 GB |
| DL-20 | did not finish | 169 s, 5.4 GB |

Very large ABoxes that still need a lot of memory:

- Not every ABox is as dense and interconnected as OWL2Bench.
- Some ABoxes have many individuals that look the same, so they need not be computed again and again.
- Idea: run Konclude on a small ABox that has only one individual of each kind, and then copy its results to the other individuals.
- I am not sure how much Konclude already does this inside. I think it shares cached descriptions between similar individuals.
- I already have this as a separate tool.
  - On a test file with 9,215 individuals, only 22 were needed. Time went from 3.3 s to 0.19 s and memory from 312 MB to 38 MB. The output was the same.
  - On OWL2Bench it saves nothing, because the links are random.
- I want to add this as an option inside Konclude. It is for people who know their ABox has many similar individuals.

## 3. Bugs we found and fixed, one by one

1. One worker (`-w 1`) hung forever.
   - Fix: the thread pool was too small by one thread.
2. Crash when using several workers on big files.
   - Fix: a worker wrote to the wrong place in the cache.
3. `materialize` hung or crashed on small random files.
   - Fix: a counter was never released, and some missing data was not checked.
4. Classification hung when individuals were merged.
   - Fix: a guard for merged individuals.
5. Results changed from run to run.
   - Fix: things were ordered by memory address. Now the order is fixed.
6. The output was different on different runs of `materialize`.
   - A true fact was sometimes missing.
   - A fact that is not true was sometimes written.
   - The `sameAs` pairs came out in a different order.
   - All three are fixed.
7. `materialize` was slow when many individuals are linked to each other.
   - Fix: the closure is now computed once per group, not again and again.
   - DL-1: from about 820 s to about 126 s.
8. Many facts were missing from `materialize` on DL-1 (61 `PeopleWithManyHobbies` instead of 536).
   - Cause: two shortcuts said "not an instance" without testing, because they did not count neighbours stored only in the cache.
   - Fix: they count those neighbours now. DL-1 gives 536, DL-2 gives 1,142 (it was 119).
9. Small ontologies with nominals wrote no object property facts at all.
   - Cause: the cache was never published, so realization found no data.
   - Fix: the cache is completed with the data it has.
10. A missed inconsistency with an irreflexive role and its inverse.
   - Fix: the check also looks at the inverse role.
11. I checked the results against a second reasoner (Openllet). That is how 9 and 10 were found.

## 4. Where we are now


- `materialize` is now complete on DL-1 and DL-2, but it takes longer than before: about 3 to 4 minutes on DL-1 and about 19 minutes on DL-2. The old runs were faster because they skipped tests.
- On DL-5, DL-10 and DL-20 `materialize` runs out of memory (10.5 GB and more), although the consistency check needs only 2 to 5 GB.
  - The memory goes in the class classification step, not in `materialize`: the tests load the neighbours of the 28 interest individuals (linked to thousands of persons) into every test.
- You can now choose what `materialize` computes (types, object properties, data properties, sameAs, hierarchies). DL-1 with types only takes 115 s instead of about 195 s.
- Two random files still miss facts in some runs, and one runs a very long search.
- I am running Konclude on more benchmark files to check that it is fast and gives all the facts.
- `materialize` should also be fast enough on larger datasets

## 5. Next steps

- Other reasoners have features that Konclude does not have:
  - explanations (why is something true or inconsistent)
  - a lint check (warns about axioms that are too expensive)
  - entailing on demand -- types, hierarchy, objectproperties.
  - other helper commands
- We already have these as separate tools.
- Plan: put them inside Konclude, so anyone can use them, and add simple hints for people who are new.
- Why explanations matter: I want to change Konclude to work with noisy data.
  - If it can explain what is inconsistent, it can handle the noise.
- Add the option to reason on a small ABox and copy the results to similar individuals (see section 2).
- Keep testing on more benchmarks so we have a good reasoner for `materialize` (fast, complete and correct).
- Perhaps also fidn a way to let konclude decide the worker nodes.
