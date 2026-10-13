# Koncludix: a simple guide

This guide is written in plain words. Every technical word is explained the first time it appears. All numbers come from the other pages in this repository (`README-fixes-and-status.md` and the pages in `docs/`).

Commands in this guide are for Linux or WSL (the Linux shell inside Windows). The program is called `Konclude`.

---

## Words you will meet

| Word | Simple meaning |
|---|---|
| ontology | A file with facts and rules about a topic. |
| axiom | One single statement (one fact or one rule) in an ontology. |
| class | A group of things. Example: `Person`. |
| individual | One single thing. Example: `anna`. |
| property | A link between two things. Example: `anna likes tennis`. |
| TBox | The rules part of an ontology (about classes and properties). |
| ABox | The data part of an ontology (about individuals). |
| reasoner | A program that works out what follows from the facts and rules. Konclude is a reasoner. |
| consistent / inconsistent | Consistent = the facts and rules do not fight each other. Inconsistent = they contradict each other, so they cannot all be true. |
| classification | Working out which class is below which class (the class tree). |
| realization | Working out which classes each individual belongs to (its types). |
| materialize | Writing down all the facts that follow, so you can read them in a file. |
| worker | One thread of work. More workers = more cores used, but more memory. |
| hash seed | A number that changes the order in which Konclude loads things. The answer must not change, but speed and memory can. |
| nominal | A class that is a fixed list of individuals. Example: `{a, b}`. |
| OWL2Bench | A big test data set (files DL-1 to DL-20). |
| time stamp | A start time and an end time stored on a thing. |
| Allen relations | The 13 ways two time intervals can relate: `before`, `meets`, `overlaps`, `starts`, `during`, `finishes`, `equals` and their opposites (`after`, `metBy`, `overlappedBy`, `startedBy`, `contains`, `finishedBy`). |

---

## 1. What this project is

1. Konclude is a reasoner. It is fast, but it needs a lot of memory on big data.
2. Koncludix is your own copy (fork) of Konclude, in `D:\Github\Koncludix`.
3. Goal: use less memory, add features that Konclude does not have (explain, lint, materialize options, interval checks), and later support approximate reasoning.
4. We found and fixed many bugs, and measured the results on real test data (OWL2Bench, 231 small random ontologies, InferTest).
5. The work is not committed yet. The new interval (time) part works only on made-up (synthetic) data so far.

---

## 2. What is new compared to normal Konclude

Run `Konclude --help` to see all commands. The new ones are `materialize`, `explain`, `lint` and `interval`. Several options are new too.

| Feature | What it does (one simple sentence) | Command to use | Example |
|---|---|---|---|
| **materialize** | Writes all facts that follow from the ontology into one file. | `Konclude materialize -w 1 -i IN -o OUT.nt` | `./Konclude materialize -w 1 -i ontology.owl -o inferred.nt` |
| **Bug fixes** | Konclude no longer hangs, crashes, changes its answer between runs, or leaves out true facts (list in section 2.1). | nothing to do, it is built in | |
| **lint** | Reads an ontology (without reasoning) and warns about parts that will be expensive or are not fully handled. | `Konclude lint -i IN [-o REPORT]` | `./Konclude lint -i ontology.owl -o report.json` |
| **explain** | Shows the smallest set of axioms that makes an ontology inconsistent, or that makes a statement follow. | `Konclude explain -i IN` (also `--subclass SUB SUPER`, `--type INDIVIDUAL CLASS`) | `./Konclude explain -i ontology.owl --subclass "<http://ex.org/A>" "<http://ex.org/B>"` |
| **ABox reduction** | Reasons on a small copy of the data (one individual of each kind) and copies the results to the similar individuals. | add `+=Konclude.CLI.Materialize.ReduceABox=on` (`off` default, `auto` possible) | `./Konclude materialize -w 1 -i input.ofn -o output.nt +=Konclude.CLI.Materialize.ReduceABox=on` |
| **Smart number of workers** | If you give no `-w`, Konclude chooses the number of workers itself. | no option needed (or `-w N`, `-w AUTO` as before) | `./Konclude consistency -i big.owl` |
| **`-m` memory supervisor** | (Linux) Stops the run if it uses too much memory and starts again with another hash seed. | `-m MB`, `-m AUTO` or `-m OFF` | `./Konclude classification -m 9000 -i OWL2DL-5.owl -o hierarchy.xml` |
| **Realization fix** | Asserted types are no longer missing from `materialize` and `realization` on big ABoxes. | nothing to do, it is built in | |
| **At-least fix (hobbies)** | Uses much less memory for rules like "a person has at least 3 hobbies". | option `AtLeastBackendNeighbourSatisfaction` (on by default) | switch off only to compare |
| **Interval module** | Checks time stamps and Allen relations, finds conflicts in a timeline, and derives the relations that follow. | `Konclude interval -i IN [-o OUT.nt] [-c OPTIONS.xml]` | `./Konclude interval -i timeline.ttl -o derived-relations.nt` |
| **Interval inside `consistency` and `explain`** | A time conflict now makes the ontology inconsistent, and `explain` shows the facts that cause it. | `Konclude consistency -i IN`, `Konclude explain -i IN` | `./Konclude consistency -i tests/interval/02_cycle.ttl` |
| **Copy facts to same individuals** | Facts of one individual are copied to every individual that is "the same" (`sameAs`). | option `Konclude.CLI.Materialize.CopyFactsToSameIndividuals` (on by default) | see 2.8 |
| **Choose what materialize writes** | You can switch kinds of facts on or off (types, object properties, data properties, sameAs, hierarchies). | keys `Konclude.CLI.Materialize.*` in a config file with `-c` | see 2.8 |

Short help for the common options (from `Konclude --help`):

| Option | Meaning |
|---|---|
| `-i FILE` | the input ontology |
| `-o FILE` | the output file |
| `-c FILE` | a config file with options |
| `-w X` | number of workers (a number or `AUTO`) |
| `-m X` | memory limit in MB, `AUTO` (80 % of the free memory) or `OFF` |
| `-x IRI` | the name of one class or individual (for `satisfiability`, `realization`) |
| `-v` | show loading and processing times in more detail |
| `-h` | show help |

### 2.1 The important bug fixes, in simple words

| What was wrong | What happens now |
|---|---|
| Big files ran out of memory (rule "at least 3 hobbies" made new empty hobbies for every person, even when the hobbies were already in the data). | The rule counts the hobbies that already exist and makes only the missing ones. |
| `-w 1` (one worker) hung forever (the thread pool had no free thread). | The pool is one bigger. `-w 1` works. |
| Crash with several workers on big files (a worker wrote to the wrong place in the cache). | The cache entry is completed before it is written. 12 of 12 runs of DL-5 with 4 workers survive. |
| `materialize` hung or crashed on small files (a counter was never released). | Fixed. 55 of the 56 failing files now finish. |
| The answer changed from run to run (things were ordered by memory address). | The order is fixed. The output is the same every time for the cases found. |
| A true fact was sometimes missing, or a fact that is not true was written. | Fixed. Every written `sameAs` pair is now really true (75 of 75). |
| Many types missing in `materialize` (DL-1: 61 instead of 536 `PeopleWithManyHobbies`). | DL-1 gives 536, DL-2 gives 1,142. |
| No object property facts at all for small ontologies that use nominals. | The facts are written now. |
| An inconsistency was missed (`Irreflexive(t)`, `u` is the inverse of `t`, `u(a,a)`). | The check also looks at the inverse role. It is found now. |
| Facts of `sameAs` individuals were missing. | Facts are copied to all members of the group. Facts missing compared with a second reasoner (Openllet): 1,657 down to 501. |
| A crash (stack overflow) in about 1 of 3 runs of a DL-5 subset (a function called itself again and again). | It only calls itself again for another individual. 20 of 20 runs finish. |
| A true subclass fact was missing in some runs of the DL-5 class tree (`T20CricketFan` below `PeopleWithHobby`). The original Konclude has this error too. | Fixed. All six seeds give the same class tree. |
| About 20 asserted `dislikes` facts of DL-1 were missing from the output. | Fixed. DL-1 writes the same 1,426,298 facts for the seeds tried (0, 3, 5). |

### 2.2 materialize

- It writes all facts that follow: class hierarchy, types of individuals, object property facts, data property facts.
- Use one worker (`-w 1`) for big files. It is the safest choice.
- It is slower than before on big files, because the old runs skipped tests and so gave incomplete answers. Now the answer is complete.

### 2.3 lint

- It only reads the file. It does not reason. It needs no Java.
- It prints a warning plus a `Suggestion:` line for every problem.
- It can write JSON: use `-o report.json` (a name ending in `.json`) or set `KONCLUDE_LINT_JSON=1`.
- New in version 2: it finds "at least n R.F where F is a fixed list of individuals" (the hobby pattern) and estimates how many new nodes will be needed. On OWL2Bench DL-1: 1,983 of 2,488 individuals need new successors, about 4,990 nodes.
- It also warns when the minimum is larger than the list (then the class can have no members).
- The estimate is not calibrated. It does not say seconds or GB.

### 2.4 explain

- It finds a minimal set of axioms: if you remove any one of them, the problem is gone.
- It works by removing axioms and asking Konclude each time.
- Three forms:

| Question | Command |
|---|---|
| Why is it inconsistent? | `Konclude explain -i ontology.owl` |
| Why is A a subclass of B? | `Konclude explain -i ontology.owl --subclass "<http://ex.org/A>" "<http://ex.org/B>"` |
| Why is `i` of class C? | `Konclude explain -i ontology.owl --type "<http://ex.org/i>" "<http://ex.org/C>"` |

- Useful extra options: `-o FILE` (write the answer to a file), `--timeout SECONDS` (per call, default 60), `--call-workers N`, `--no-slicing`, `--no-verify`, `--flatten FILE` (only write the ontology as one axiom per line).
- It gives one minimal set, not all of them.
- Axioms that the parsers drop (`hasKey`, annotations) can never appear.
- Test result: 19 test cases gave the expected sets. It found the 3 injected axioms in OWL2Bench DL-1 in 17 s (84 calls).

### 2.5 ABox reduction

- Idea: if thousands of individuals look the same, reason about only one of them and copy the answer.
- Option: `+=Konclude.CLI.Materialize.ReduceABox=off|on|auto` (default `off`).
- `auto` reduces when the ABox has at least 100,000 individuals, or when its estimated memory is above 50 % of the free memory, and at least 10 % of the individuals can be removed.
- Works for `materialize` and `realization`, with OWL functional syntax and OWL/XML input. It does not work for RDF/XML, Turtle or imports.
- Example: a test file with 9,215 individuals needed only 22. Time 3.3 s to 0.19 s, memory 312 MB to 38 MB, same output.
- On OWL2Bench it saves nothing, because the links are random.
- If the reduced ontology turns out to be inconsistent, Konclude starts again without the reduction.

### 2.6 Number of workers and the `-m` memory supervisor

- Without `-w`, Konclude chooses (called `SMART`): up to 4 workers for inputs up to 1 MB, 2 workers up to 8 MB, 1 worker above, and fewer if the free memory is small.
- With `-m`, the command runs in a child process. If the child uses more memory than the limit, it is stopped and started again with another hash seed (up to 6 times). A finished run gives the same result for every seed.
- Without `-m`, this is done only for input files larger than 4 MB.

### 2.7 The two memory and correctness fixes

**Realization fix (missing types).** On big ABoxes (more than about 5,000 individuals in the hobby test family), `materialize` and `realization` left out types that were asserted in the input. For 5,000 persons the earlier build wrote about 860 fewer, and the original Konclude all but about 90. The cause: an internal item that was created only for property data hid the correct types. Now 5,000 and 10,000 persons (and 20,000 and 40,000) are complete, with the same time and memory.

**At-least fix (hobbies, less memory).** Option `AtLeastBackendNeighbourSatisfaction` (on by default). See section 4 for numbers.

### 2.8 Interval module, and the options for materialize

**The interval module (`Konclude interval`)**

- It reads Allen relations (also the OWL-Time names such as `intervalBefore`) and time stamps (PROV-O `startedAtTime` / `endedAtTime`, or OWL-Time instants).
- It finds conflicts, even those that show only when you combine relations. Example: `a before b`, `b before c`, `c before a` (a cycle). No single pair is wrong, but together they are impossible.
- It explains each conflict with a minimal set of facts.
- It derives the relations that follow. Example: `meets` and `meets` give `before`.
- It does not use the tableau (the normal reasoning method). It runs next to it, like `lint`.
- Command: `Konclude interval -i ontology.ttl [-o derived-relations.nt] [-c options.xml]`. The report goes to the screen. `-o` writes the derived relations as N-Triples.
- Why a module? OWL axioms cannot do this. On 427 impossible interval networks, the OWL axioms alone found 0. The module found 427 of 427.

**Connection to `consistency` and `explain`**

- The interval check is now a first step of `consistency` (it is on by default).
- Conflict found: `consistency` prints the report with the minimal set, says "inconsistent" and stops. `explain` shows the same facts as its answer (for example the claim and the two time stamps).
- No conflict, and the ontology declares the 13 Allen relation properties: the relations derived from the time stamps are added to the ontology, using the ontology's own property names. So a rule like "a test must not start before machining has ended" can see the order, even if the file only has time stamps.
- No relation property declared: nothing is added.
- `explain` always uses the default options (it does not pass `-c` on to its sub-runs).

**Options `Konclude.CLI.Interval.*`**

| Key | Default | Meaning |
|---|---|---|
| `Consistency` | auto | `auto`/`on`: interval check is a step of `consistency`; `off`: not run |
| `AddDerivedRelations` | auto | `auto`: add derived relations when the ontology declares the relation properties; `true`: always; `false`: never |
| `RelationNamespace` | (empty) | only properties in this namespace count as relations |
| `GroupByProperty` | (empty) | derive relations only inside groups linked by this property |
| `DeriveFromTimestamps` | true | switch the derivation from time stamps off |
| `MaxDerivedPairs` | 1,000,000 | limit for the derivation |
| `MaxComponentSize` | 150 | largest group for the full conflict check |
| `MaxMinimizationSize` | 60 | largest group for the minimal conflict set |
| `OutputVocabulary` | owltime | `owltime` (`time:intervalBefore`) or `allen` (`before`) for the derived relations |
| `OutputNamespace` | `http://konclude.com/interval#` | namespace used with `allen` |

(All keys start with `Konclude.CLI.Interval.`)

**How to pass an option with `-c` (before `-i`)**

Write a small XML file. Example, switch the derived relations off (`tests/interval/noderive.config.xml`):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<RequestMessage xmlns="http://www.owllink.org/owllink-xml#">
<Set key="Konclude.CLI.Interval.AddDerivedRelations"><Literal>false</Literal></Set>
</RequestMessage>
```

Then give it with `-c`, before `-i`:

```bash
./Konclude consistency -c noderive.config.xml -i 11_rules_over_derived_relations.ofn
```

**Options for what materialize writes**

All keys start with `Konclude.CLI.Materialize.` and are `true` by default. Put the ones you want to change into a config file and pass it with `-c`.

| Key | What it switches |
|---|---|
| `ClassAssertions` | the types of the individuals |
| `ObjectPropertyAssertions` | object property facts (the expensive part: about 1.4 million of the 1.44 million facts on DL-1) |
| `DataPropertyAssertions` | data property facts |
| `SameIndividuals` | `owl:sameAs` groups |
| `ClassHierarchy` | subclass facts |
| `PropertyHierarchy` | subproperty facts |
| `CopyFactsToSameIndividuals` | copy the facts of one individual to all individuals that are `sameAs` |
| `ReduceABox` | `off`, `on` or `auto` (pass it with `+=`, see 2.5) |

Example: types only (no object properties):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<RequestMessage xmlns="http://www.owllink.org/owllink-xml#">
  <Set key="Konclude.CLI.Materialize.ObjectPropertyAssertions"><Literal>false</Literal></Set>
</RequestMessage>
```

```bash
./Konclude materialize -w 1 -c types-only.xml -i ontology.owl -o inferred.nt
```

Note: object property facts need the types to be computed first. So when you ask for object properties, the types are computed too (but are only written if you ask for them).

Warning about `CopyFactsToSameIndividuals`: for a big `sameAs` group the output grows with the square of the group size (a 200-individual file needed 8 GB). The copied facts are true, but this default is not decided yet. It needs a decision.

---

## 3. How to use: step-by-step recipes

Commands assume you are in the folder with `Konclude` (WSL). Replace the file names with yours.

### Recipe 1: check an ontology before you reason

1. Run lint (it is fast and does not reason):

```bash
./Konclude lint -i ontology.owl
```

2. Read each warning and its `Suggestion:` line.
3. If you want a machine-readable report:

```bash
./Konclude lint -i ontology.owl -o report.json
```

4. Then check if it is consistent, with time and peak memory:

```bash
/usr/bin/time -f "%e s, %M KB" ./Konclude consistency -w 1 -i ontology.owl
```

### Recipe 2: find out why an ontology is inconsistent

1. Check it:

```bash
./Konclude consistency -i ontology.owl
```

2. If the answer is "inconsistent", ask for the reason:

```bash
./Konclude explain -i ontology.owl -o reason.txt
```

3. You get a short list of axioms, one per line. Remove or fix one of them and the problem is gone.
4. To find why a subclass fact or a type follows:

```bash
./Konclude explain -i ontology.owl --subclass "<http://ex.org/A>" "<http://ex.org/B>"
./Konclude explain -i ontology.owl --type "<http://ex.org/i>" "<http://ex.org/C>"
```

### Recipe 3: materialize (write all facts that follow)

1. Run it with one worker:

```bash
./Konclude materialize -w 1 -i ontology.owl -o inferred.nt
```

2. To write fewer kinds of facts, make a config file (see 2.8) and add `-c types-only.xml` before `-i`.
3. For a file with many identical individuals, you can try the reduction:

```bash
./Konclude materialize -w 1 -i input.ofn -o output.nt +=Konclude.CLI.Materialize.ReduceABox=on
```

4. For a very big file, add a memory limit (Linux):

```bash
./Konclude classification -m 9000 -i OWL2DL-5.owl -o hierarchy.xml
```

### Recipe 4: find time conflicts in a timeline

Example file `tests/interval/02_cycle.ttl` has `a before b`, `b before c`, `c before a`.

1. Run the interval command:

```bash
./Konclude interval -i tests/interval/02_cycle.ttl
```

2. The report says `Interval result: inconsistent (1 conflict)` and lists the three facts that contradict each other.
3. The same file with `consistency` also says "inconsistent":

```bash
./Konclude consistency -i tests/interval/02_cycle.ttl
```

4. To get the Allen relations that follow from time stamps (file with `startedAtTime` / `endedAtTime`):

```bash
./Konclude interval -i tests/interval/01_timestamps.ttl -o derived-relations.nt
```

For this file it writes 3 relations, for example `casting intervalMeets forging`, `casting intervalBefore testing`, `forging intervalOverlaps testing`.

5. Other test files: `03_claim_vs_timestamps.ttl` (a claim that contradicts the time stamps), `07_mixed_network.ttl` (two time stamps and two claims, 4 facts in the conflict).

### Recipe 5: rules over time stamps

File: `tests/interval/11_rules_over_derived_relations.ofn`. It has no Allen relation at all, only time stamps and the rule "a test must not start before machining has ended". In sample 1 the test starts after machining (fine). In sample 2 the test starts while machining still runs (violation).

1. Check it:

```bash
./Konclude consistency -i tests/interval/11_rules_over_derived_relations.ofn
```

   Konclude says that 2 derived relations are added, and then that the ontology is inconsistent.

2. Find the reason:

```bash
./Konclude explain -i tests/interval/11_rules_over_derived_relations.ofn
```

   The answer names the rule, the class assertions, the four time stamps of sample 2, the `inSample` links and the sub-property axiom. Nothing about sample 1.

3. See what happens without the derived relations (the rule does not see the order, so the file looks consistent):

```bash
./Konclude consistency -c tests/interval/noderive.config.xml -i tests/interval/11_rules_over_derived_relations.ofn
```

4. The file `12_rules_ok.ofn` (test starts when machining ends: `meets`) is consistent.

---

## 4. What we measured

"Before" means before the changes. "Now" means the final build. Times on the laptop vary by about 10 to 20 % from run to run.

### 4.1 Consistency check (1 worker)

| File | Individuals, facts | Before | Now |
|---|---|---|---|
| DL-1 | 3.7K, 50K | 34 s, 3.5 GB | 7 s, 1.8 GB |
| DL-2 | 7K, 99K | no answer in 900 s | 26 s, 3.7 GB |
| DL-5 | 23K, 325K | no answer in 12 GB | 31 s, 2.1 GB |
| DL-10 | 50K, 711K | no answer in 12 GB | 80 s, 3.3 GB |
| DL-20 | 98K, 1.4M | no answer in 12 GB | 169 s, 5.4 GB |

Correction (from the overnight tests): the "DL-5 no answer in 12 GB" came from a run with many workers. In a controlled run with one worker, the original build answers DL-5 in 48 s and 2.75 GB. The gain at one worker is 48 s to 27 s and 2.75 GB to 2.09 GB. On DL-1 (one worker): 23.3 s and 4.73 GB to 4.4 s and 1.75 GB.

### 4.2 materialize

| File | Before | Now |
|---|---|---|
| DL-1 | 820 s, 3.7 GB (4 workers) | about 3 to 4 minutes, 2.2 GB (1 worker). 1.44 million facts. `PeopleWithManyHobbies`: 61 before, 536 now (correct) |
| DL-2 | 773 s, 5.1 GB | about 19 minutes, 5.4 GB. 5.0 million facts. `PeopleWithManyHobbies`: 119 before, 1,142 now |
| DL-5, DL-10, DL-20 | not tried or no answer | still run out of memory (see section 5) |

The old runs were faster only because they skipped tests, so their answers were incomplete.

DL-1 with different choices of what to write (1 worker):

| What is written | Time | Facts |
|---|---|---|
| Types only | 115 s | 30,749 |
| Everything except object property facts | 138 s | 49,997 |
| Everything | about 195 s | 1,444,364 |
| Object property facts only | about 200 s | 1,398,129 |

In the controlled overnight run, DL-1 `materialize` (all kinds) took 914 s and 4.96 GB with the original build, and 147 s and 2.02 GB with the new one. The kind of output matters little there: types only took 92 s.

### 4.3 The hobby test family (made-up data)

Rule: every person has at least 3 hobbies, hobbies are a fixed list of 8. Persons asserted in the output (n is the number of persons):

| Persons | Original Konclude | Final build |
|---|---|---|
| 1,000 | 5.4 s, 1.85 GB | 5.2 s, 1.05 GB (complete) |
| 2,000 | 50.4 s, 3.91 GB | 33.0 s, 2.96 GB (complete) |
| 5,000 | 23.4 s, only 73 to 93 persons in the output | 3.5 s, 0.90 GB, all 5,000 |
| 10,000 | 120 s, only 144 to 189 persons | 12.1 s, 1.58 GB, all 10,000 |
| 20,000 | out of memory (3 of 3 runs) | 39.4 s, 4.52 GB, all 20,000 |
| 40,000 | not run | 32.5 s, 3.71 GB, all 40,000 (1 further seed hit the 7.5 GB guard) |

Earlier speed-up claims at 5,000 persons and above (for example "7 times faster") were withdrawn. They compared incomplete outputs.

### 4.4 Other tests

| Test | Result |
|---|---|
| 231 small random ontologies, `materialize` | Before: 37 hangs and 17 crashes (about 1 in 4). Now: 229 give the same output every time (5 runs each). `c0314` still changes sometimes, `c0162` runs a very long search. |
| Same 231 files, verdict | 1 changed: `c0266` is now correctly inconsistent (as Openllet says). |
| InferTest | Before: 25 of 28 and 8 of 9. Now: 27 of 28 and 8 of 9 (only the `hasKey` test fails). |
| Realization fix, regression | 229 of 230 files identical (the other: one extra true fact in `c0314`). DL-1: same 1,439,725 ABox lines. |
| Are the written `sameAs` pairs true? | 75 of 75 pairs are entailed. |
| Cardinality change | 185 files and 1,800 small tests: same answers with the option on and off. |
| Comparison with Openllet (225 random files) | Facts that Openllet derives and Konclude missed: 1,657 down to 501. For the valid OWL 2 DL files, a sample of the rest is not entailed, so they are arbitrary merges by Openllet. |
| Konclude's own Tell/Retract (incremental mode) | Not usable (see section 5). |

None of this proves the results are sound and complete. It only finds mistakes. The comparison with a second reasoner is the strongest check so far.

### 4.5 Which options matter (hobby family, 2,000 persons)

- Baseline: 15.3 s and 2.22 GB.
- `AtLeastBackendNeighbourSatisfaction` off: 79 % more time, 3.64 GB.
- `SaturationCriticalConceptTesting` off: 0.3 to 1 s instead of 15 to 28 s, same output. But at 40,000 persons it is worse (178 s, 9.8 to 10.7 GB), and on OWL2Bench it is 1.3 to 1.5 times slower. So the better choice would depend on the data.

### 4.6 Lint

On OWL2Bench DL-1 it flags the hobby pattern. On the OWL2Gen-made files with no enumerated lists and on the 9 control files it stays silent ("No problems found"). Same report as the old Java tool on OWL2Bench DL-1/2/5/10 and EL/QL/RL-1. DL-20: 20 s, 1.7 GB.

### 4.7 Interval module (made-up data)

| Test | Result |
|---|---|
| 427 impossible interval networks (4 intervals) | OWL axioms alone found 0 of 427. The interval module found 427 of 427, with no false alarms. |
| Random check against an exact ground truth | 200 of 200 verdicts agree (131 inconsistent). |
| Test checks in `tests/interval/` | 24 checks, all pass. |
| `consistency` on the 231 test ontologies | Same verdicts (except `c0266`), same total time (3.96 s before, 3.89 s now). |
| Process rules over time stamps, 200 samples (1,515 intervals) | 5,005 relations derived in 0.2 s. 18 of 18 violating samples found, no false alarms. Whole-file `consistency` 0.1 s. |
| Earlier prototype (python) with `explain` | 5,000 samples: 53 of 53 violations found, 880 s (explain took 725 s of this). A single `consistency` check of that file: 2.7 s, 0.50 GB. |

### 4.8 GenACT (conference tweet data) with Konclude

- The generated data is inconsistent with its own DL ontology (found with `explain`): datatype ranges and the exact cardinality of `hasWebsiteURL`.
- After repairing the literals, a window of 1,832 files (10,425 axioms) needed: consistency 17 s and 2.3 GB, materialize 60 s. Too slow for sliding windows with this ontology.
- The answers were correct: the entailment queries gave the same rows as the plain join.

---

## 5. What does not work yet (honest limits)

| Item | What we know |
|---|---|
| `materialize` on DL-5, DL-10, DL-20 | Runs out of memory. DL-5: memory grows from 4.8 GB to more than 10 GB in the step that computes the types of the individuals. DL-10 and DL-20 classification are still too big. |
| DL-5 output size | The object property facts alone would be about 52 million for one role (`hasSameHomeTownWith`), so the result file would be many GB even without the memory problem. |
| `materialize` is slow on big files | DL-2 takes about 19 minutes. |
| Small ontologies with an enumerated class | The cost grows very fast when the ABox grows (34 of 58 files blow up at 10 times the ABox). Not solved. |
| Konclude's own incremental mode (Tell/Retract) | Works after our two fixes (the instance list was incomplete after a `Tell`; `IncrementalRebuild=on` crashed). With the real PMD terminology a new period costs about 0.7 s instead of about 11 s, and the answers are the same as when everything is loaded again. For small ontologies it makes no difference. Details: `docs/incremental-revisions.md`. |
| `hasKey` | Not enforced (the parsers drop it). This is the one InferTest test that fails. |
| Facts that follow only from counting | `c0314` misses merges that are forced only by a counting argument over several possibilities. Nothing in the output is wrong, but it is not complete there. It depends on thread timing. |
| `c0162` | One test searches a very long time. |
| Order of axioms | For a few ontologies the answer depends on the order of the axioms. |
| Several workers on big files | Memory grows. Use `-w 1`. |
| `CopyFactsToSameIndividuals` | For a big `sameAs` group the output grows with the square of the group size (8 GB for a 200-individual file). Needs your decision. |
| ABox reduction | Memory saving is small so far (11 % on DL-1 with 2,000 added individuals: 2.15 GB against 2.41 GB). Not for RDF/XML, Turtle, imports. |
| Lint | The estimate is not calibrated. It only counts asserted neighbours. Suggestions are templates; a person must decide. |
| Interval: path consistency is incomplete | A conflict it finds is certain. A network it accepts can still be inconsistent (the general problem is NP-hard). Exact for the 4-interval networks tested. |
| Interval: other limits | Needs the 13 relation properties declared to add derived relations. `explain` uses default options only. `sameAs` intervals are not merged. Point events (start = end) are ignored with a warning. A time without a zone is read as UTC. |
| Interval and the rule layer: synthetic data only | Tested on made-up data. Not on real PMD timelines yet. |
| Data | Test data is synthetic or benchmark data. The GenACT test is a first run, not a comparison with the stream engines. |
| Not checked yet | The other OWL2Bench profiles (EL, QL, RL) for `materialize`. |
| Nothing is committed | The changes are in the working tree, not committed. |
| Parts not repeated | The InferTest suite and the 1,800-test comparison with HermiT and Openllet were not rerun after the realization fix (both should be run before committing). |
| Rules and incremental reasoning | Analysed and measured, not built. The parser reads `DLSafeRule` and drops it. |

---

## 6. How to run the tests

### 6.1 The interval tests (in this repository)

From the repository folder in WSL (the path after the script is your Konclude program):

```bash
python3 tests/interval/run_interval_tests.py ~/kon_all2/Release/Konclude
```

- It runs 14 cases for the `interval` command, 9 for `consistency` and `explain`, and a random check of 200 networks against an exact ground truth. All 24 checks should print `PASS`.
- Use `--random N` for a different number of random networks. Example: `--random 20` is quicker.
- It ends with a line like `24 of 24 checks passed` (or the number of checks you ran). The exit code is 0 if everything passes.

| Case | What it checks |
|---|---|
| 01 | Allen relations derived from time stamps |
| 02 | a cycle of `before` (conflict only through composition) |
| 03 | a claim that contradicts the time stamps |
| 04 | `meets` and `meets` give `before` |
| 05 | two relations for the same pair |
| 06 | OWL-Time intervals and instants |
| 07 | time stamps plus two claims about an interval without stamps |
| 08 | warnings for a zero-length interval and unreadable time |
| 09 | relations only inside a group (sample) |
| 10 | the same conflict in functional syntax |
| 13 to 16 | PMD order relations (`precedes`, `preceded by`): a cycle, a chain, two opposite links, a link against time stamps |
| C1 to C7 | `consistency`: conflicts, switch off, rules over derived relations |
| E1, E2 | `explain`: interval conflict and rule violation |

### 6.2 Other tests (scripts are not all in this repository)

- InferTest (28 entailment tests and 9 inconsistency tests): script `tools/run-infertest.ps1` (needs the Windows runner).
- Hobby family and option sweeps: scripts and raw data in `experiments/ab_at_least/` and `experiments/overnight/`. The check that the output has all asserted facts is `experiments/ab_at_least/check_asserted.py`.
- Time pipeline demo: `experiments/temporal/`. GenACT windows: `experiments/genact/`. Konclude's own Tell/Retract tests: `experiments/incremental/`.
- The 231 small random ontologies and the comparison scripts are kept in `D:\scratch_logs` and the WSL home folder (`~/fz`), not in this repository.

---

## Temporal layer (new)

**What it is.** A tool (`tools/temporal/tlayer.py`) that answers questions about **time** on top of Konclude. Facts can be true only for a while ("Alice is a manager from 1 June"). You ask things like "does every employee become a manager sometime?" It does not change Konclude.

**How it works, in simple steps.**
1. It finds every day on which some fact starts or stops. Between two such days nothing changes, so each period is one **state**.
2. For each state it asks Konclude what follows (Alice is an employee, because managers are employees).
3. It checks your time rule over the list of states.
4. If only a few people changed, it reuses the old answers for everybody else. This is the **cache**.

**How to run it.**

```bash
export KONCLUDE_BIN=~/kon_all2/Release/Konclude
python3 tools/temporal/tlayer.py --tbox tests/temporal/hr_tbox.ofn --facts tests/temporal/hr_facts.tl --formula "G(Employee(?x) -> F Manager(?x))" --show problems --explain
```

**The facts file** has one fact per line: `Manager(Alice) [2025-06-01, inf)`. The ontology must be a `.ofn` file.

**The time words:**

| Word | Means |
|---|---|
| `F p` | p is true **sometime** from now on |
| `G p` | p is true **always** from now on |
| `X p` | p is true in the **next** state |
| `p U q` | p stays true **until** q becomes true |
| `F[a,b] p`, `G[a,b] p` | **deadlines**: p is true sometime / always between a and b days from now (needs `--now`) |
| `O p`, `H p`, `Y p`, `p S q` | the same about the **past** |

**The four answers.** TRUE and FALSE: sure, whatever happens later. UNKNOWN: the ontology and the facts do not say (OWL does not treat a missing fact as false). PENDING: not decided yet, something later can still change it; the report says what it waits for.

**Useful options.**

| Option | What it does |
|---|---|
| `--closed` | the history is finished: promises not kept count as broken |
| `--validation` | a fact that is not there counts as false (easier to read, but not pure OWL) |
| `--now DATE` | stop the history at this day |
| `--at DATE` | start checking at this day |
| `--why` | show why over time: the smallest set of facts and periods that decides each sure answer |
| `--explain` | show the smallest set of ontology axioms and facts behind each fact |
| `--timeline` | list all states |
| `-o file.json` | save the result |

**Derived classes ("temporal definitions").** You can also let time **create** knowledge. Example: "a former employee is somebody who was an employee once and is not one now". You write it as `--define "FormerEmployee(?x) := O Employee(?x) & !Employee(?x)"`. The tool works out for whom and when it is true and gives them the class `FormerEmployee` for exactly that time. After that the ontology can use it like any other class. For example, if the ontology says "a former employee is an alumnus" and "an alumnus is never a student", then somebody who becomes a student again after leaving gets reported as a contradiction, and the report tells you which fact came from your definition. A definition may only look at the present and the past, and may not use another defined class.

**"Why?" over time (`--why`).** For every sure answer (TRUE or FALSE) the tool shows the smallest set of facts and time periods that decide it. Example: "Bob is FALSE for the deadline because: he was an employee from 1 February, and he was not a manager at any time during the 200 days." Nothing else is shown, and every shown item is really needed (the tests check this on thousands of random cases). With `--explain` you also see the ontology axioms behind each fact.

**I need good examples of reasoning for a talk or paper.** Run `python3 tools/konclude_examples.py ~/kon_all2/Release/Konclude my.ttl --classes "heat treatment" --involving`. It finds conclusions that are not written in your ontology and that need several axioms, shows the smallest set of axioms behind each, ranks them (more axioms and a richer mix come first), and writes each in plain English using your labels. `--classes` limits it to the classes you name; with `--involving` it also finds examples where the class is only part of the reasoning. Details: `docs/reasoning-examples.md`.

**Why is my reasoning slow, or why does it not finish?** Run `python3 tools/konclude_diagnose.py ~/kon_all2/Release/Konclude my.owl --task classification --timeout 120`. It runs the task with a time and memory limit and then tells you: which stage grew (loading, preprocessing, classification, ...), which parts of your ontology the reasoner was busy with (for example "a choice among 28 enumerated hobbies: 72 % of all the work"), which lint warnings explain it, and what to try. It works even if the run never finishes: the reasoner writes a small table every few seconds, so the last table shows what it was doing when it was stopped. `Konclude lint` prints a last line that points to this tool. Details: `docs/lint-and-diagnose.md`.

**Tests.** `python3 tests/temporal/run_temporal_tests.py ~/kon_all2/Release/Konclude` (20 checks, all pass).

**Speed.** When reasoning is cheap, the cache does not matter (about 35 ms per Konclude call). When reasoning is expensive: 3,000 people, 100 states: 10.8 s with the cache and 42.0 s without, same answers.

**Limits.** People linked to one shared item (a lab, a machine) form one big group, so the cache cannot help. If the ontology uses nominals (`ObjectOneOf`, `ObjectHasValue`), the tool reasons about the whole snapshot at once. Time is in days or ticks, the ontology does not change over time, and tests used only made-up data.

Full description: `docs/temporal-layer.md`.
