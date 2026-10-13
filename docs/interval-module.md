# The interval module: `Konclude interval`

A theory module inside Konclude for Allen's interval algebra. It reads the interval relations and the time stamps of the individuals of an ontology, finds contradictions (also those that only show through composition), explains each one with a minimal set of facts, and derives the relations that follow. It runs next to the tableau reasoner (it does not use the tableau), the same way `lint` does, and it is a new command.

```
Konclude interval -i ontology.ttl [-o derived-relations.nt] [-c options.xml]
```
The report goes to the standard output; `-o` writes the derived relations as N-Triples. The input is read through the normal ontology loader (tested with Turtle and OWL functional syntax; RDF/XML and OWL/XML use the same paths).

## Why a module (what the OWL axioms cannot do)

The composition of Allen's relations (169 table entries) and the consistency of a network of intervals cannot be put into an OWL ontology: the roles would not be simple, and Konclude ignores irreflexivity, disjointness and `Self` on non-simple roles (measured). On 427 inconsistent interval networks the OWL axioms alone found none, Allen's path consistency found all. The module adds exactly this part.

## What it reads

| Input | Recognised by |
|---|---|
| Allen relations between individuals | object property assertions or triples whose property has one of the local names `before, meets, overlaps, starts, during, finishes, equals, after, metBy, overlappedBy, startedBy, contains, finishedBy`, or the OWL-Time names `intervalBefore, intervalMeets, …`, in any namespace (restrict with `Konclude.CLI.Interval.RelationNamespace`) |
| BFO order relations (PMD) | `precedes` (`BFO_0000063`) and `preceded by` (`BFO_0000062`): "x precedes y" means the end of x is not after the start of y, that is **before or meets** (a set of two Allen relations, not one). They take part in the composition and the conflict search like the other relations. |
| Time stamps, PROV-O | `startedAtTime` and `endedAtTime` on the individual |
| Time stamps, OWL-Time | `hasBeginning` / `hasEnd` to an instant that has `inXSDDateTimeStamp`, `inXSDDateTime` or `inXSDDate` |
| Values | xsd:dateTime and date (no time zone is read as UTC), or plain integers (used as they are, for synthetic data and simulation ticks) |

## What it does

1. **Derives** the Allen relation of every pair of intervals with time stamps. The pairs can be limited to intervals of one process with `Konclude.CLI.Interval.GroupByProperty` (an object property that links the intervals of a sample or process); without it, all intervals with time stamps form one group (a warning appears if that would be more than `MaxDerivedPairs`, default 1,000,000).
2. **Checks** every group of individuals that are connected by asserted relations: the asserted relations and the time stamps are combined into a constraint network and Allen's path consistency runs on it (groups up to 150 individuals; larger groups are checked pair by pair against the time stamps and a warning says so).
3. **Explains** each conflict: a deletion-based search returns a **minimal set of facts** that contradict each other (asserted relations and the time stamps of individuals), for groups up to 60 individuals.
4. **Derives** from the composition: relations that follow from the asserted ones (for example `meets` and `meets` give `before`) for pairs without a stamp pair and without an asserted relation, when the result is a single relation.
5. **Reports** warnings: intervals with start equal to or after the end (ignored), time values that cannot be read.

Example (`tests/interval/07_mixed_network.ttl`: two intervals with numeric time stamps and two claims about an interval without time stamps):
```
[CONFLICT 1] 4 facts contradict each other (a minimal set; the empty constraint arose between a and b):
       asserted: c after b
       asserted: c during a
       time stamps: a [10 .. 20]
       time stamps: b [30 .. 40]
Interval result: inconsistent (1 conflict)
```
A cycle `a before b, b before c, c before a` is reported with the three assertions (`02_cycle.ttl`), a claim against time stamps with the claim and both stamps (`03_claim_vs_timestamps.ttl`).

## Options (`-c` with an OWLlink request file of `Set` commands, given before `-i`)

| Key | Default | Meaning |
|---|---|---|
| `Konclude.CLI.Interval.RelationNamespace` | (empty) | only properties in this namespace are relations |
| `Konclude.CLI.Interval.GroupByProperty` | (empty) | derive relations only within groups linked by this property |
| `Konclude.CLI.Interval.DeriveFromTimestamps` | true | switch the derivation from time stamps off |
| `Konclude.CLI.Interval.MaxDerivedPairs` | 1,000,000 | limit for the derivation |
| `Konclude.CLI.Interval.MaxComponentSize` | 150 | largest group for the path consistency check |
| `Konclude.CLI.Interval.MaxMinimizationSize` | 60 | largest group for the minimal conflict set |
| `Konclude.CLI.Interval.OutputVocabulary` | owltime | `owltime` (`time:intervalBefore`) or `allen` (`before`) for the derived relations |
| `Konclude.CLI.Interval.OutputNamespace` | `http://konclude.com/interval#` | namespace for `allen` |

## Inside `consistency` and `explain` (the connection)

The interval theory is a step of the normal `consistency` command, so every command that builds on it (`explain`, and the workflows around it) sees it too.

```
Konclude consistency -i ontology.ttl          # interval theory on (default)
Konclude consistency -c off.config.xml -i ontology.ttl   # Consistency = off: plain tableau as before
```

What happens, in this order:

1. The interval analyser runs first on the loaded ontology (no cost for an ontology without time stamps or Allen relations; the 231 test ontologies take the same total time as before).
2. **Conflict found**: `consistency` prints the interval report (the minimal set of facts), answers "Ontology ... is inconsistent." and stops; the tableau is not needed. Same for `explain`: the justification contains the asserted relations and the time stamps (start and end stamps of both intervals when a claim is against the stamps), for example 5 axioms for `10_functional_syntax.ofn`.
3. **No conflict, and the ontology declares the Allen relation properties** (any of the 13 names, as object properties): the relations derived from the time stamps are added to the ontology, using **the ontology's own property IRIs** (an inverse declaration decides which of `before`/`after` is used), and the tableau runs on the ontology plus the derived relations. This is how a process-order rule such as `Test ⊑ ∀notAfter.¬Machining` over the class hierarchy sees order that exists only as time stamps. `11_rules_over_derived_relations.ofn` contains no relation at all, only PROV-O stamps; the rule violation of sample 2 is found, and `explain` returns the rule, the class assertion, the two stamps and the sub-property axiom, and nothing about sample 1. `12_rules_ok.ofn` (test starts when machining ends: `meets`) is consistent.
4. **No relation property declared** (and no wish to add relations): nothing is added, the tableau runs on the original ontology. A pure PROV-O ontology therefore costs only the check of step 1.

Options (same `-c` mechanism, before `-i`):

| Key | Default | Meaning |
|---|---|---|
| `Konclude.CLI.Interval.Consistency` | auto | `auto`/`on`: the interval theory is a step of `consistency`; `off`: not run |
| `Konclude.CLI.Interval.AddDerivedRelations` | auto | `auto`: add the derived relations when the ontology declares the relation properties; `true`: always (with the configured vocabulary); `false`: never |

The derivation for the connection groups intervals by connectivity (intervals linked by any object property, for example `inSample`), so a rule never sees a relation between two different samples. (For the stand-alone `interval` command all intervals form one group unless `GroupByProperty` is set.)

## Tests (`tests/interval/`)

`python3 tests/interval/run_interval_tests.py PATH_TO_KONCLUDE [--random N]` runs fourteen cases for the command, nine for `consistency` and `explain`, and a randomized check:

| Case | What it shows |
|---|---|
| 01 | Allen relations derived from PROV-O time stamps (meets, before, overlaps) |
| 02 | a cycle of `before`: no pair of facts is contradictory, the conflict needs composition |
| 03 | a claim that contradicts the time stamps (minimal set: the claim and both stamps) |
| 04 | `meets` and `meets` derive `before` |
| 05 | two relations for one pair |
| 06 | OWL-Time intervals and instants |
| 07 | time stamps plus two claims about an interval without stamps (four facts) |
| 08 | warnings: zero-length interval, unreadable time |
| 09 | groups: relations only inside a sample (2 pairs instead of 6), output in the Allen vocabulary |
| 10 | the same conflict as 03 in functional syntax (axioms instead of triples) |
| 13 | BFO relations: a cycle of `precedes` (no two facts contradict each other, the cycle does) |
| 14 | BFO relations: `preceded by` twice gives `after` (derived by composition) |
| 15 | BFO relations: `preceded by` and `precedes` for the same pair (the two directions together are impossible) |
| 16 | BFO relation against time stamps (`a precedes b`, but a ends after b starts) |
| C1-C2 | `consistency`: the cycle and the claim against the stamps make the ontology inconsistent, with the report |
| C3 | `consistency`: consistent stamps stay consistent |
| C4 | `consistency` with `Consistency = off`: the cycle is not seen (plain tableau, as before) |
| C5-C6 | `consistency`: the rule over relations derived from stamps: violation found / order kept |
| C7 | `AddDerivedRelations = false`: the rule does not see the order |
| E1 | `explain`: the 5-axiom justification of the interval conflict |
| E2 | `explain`: the justification of the rule violation (rule, class, stamps, sub-property axiom; sample 1 not involved) |
| random | 200 random networks of four intervals: the verdict of the command against an **exact ground truth** (all 545,835 weak orders of the eight end points are enumerated): 131 inconsistent, **200 of 200 verdicts agree** |

All 24 checks pass, on both build trees. Regression: `consistency` verdicts of the new binary and the previous release agree on 230 of the 231 test ontologies; the one difference is `c0266`, which was corrected earlier by the realization fix (see `README-fixes-and-status.md`), not by the interval step. Total time over the 231 files: unchanged (3.96 s for the previous release, 3.89 s for the new one).

## With the DL part (the whole pipeline)

`experiments/temporal/pipeline_demo.py` runs the chain on PMD-style data: typed activities with PROV-O time stamps and a link to their sample (no Allen relations) go into `Konclude interval`, which derives the relations; they are added to the interval theory and the process-order rules (`Test ⊑ ∀notAfter.¬Machining`, over the class hierarchy), and `consistency` plus `explain` locate every violating timeline. For 200 samples (1,515 intervals): the interval command derives 5,005 relations in **0.2 s**; Konclude finds **18 of 18** violating samples, no false alarms (explain 37 s in total). The python step of the first prototype is no longer needed.

## Limits

- **Path consistency is not complete** for general networks of intervals (the problem is NP-hard): an inconsistency it finds is certain, a network it accepts can still be inconsistent. It is exact for the four-interval networks of the random test and for networks of basic relations with complete information.
- The connection works on what the analyser reads: the derivation of relations for the rules needs the 13 relation properties declared in the ontology (otherwise `consistency` only checks and does not add). `explain` and `consistency` run the interval step on the whole ontology; the options (`-c`) are read by `consistency` and not passed on to the sub-runs of `explain`, so `explain` always uses the defaults.
- A rule over derived relations is only as good as the grouping: the connectivity groups of the connection assume that intervals of one process are linked by some object property.
- Relations are read per individual; interval individuals that are the same only through `sameAs` are not merged.
- Only intervals with a start before the end are used; point events (start = end) are ignored with a warning. Time zones: a value without zone is read as UTC.
- Tested on synthetic data and on the cases above, not on real PMD timelines yet.

Source: `Source/Control/Loader/CIntervalAlgebra.*` (relations, composition table derived by brute force, path consistency), `CIntervalOntologyAnalyser.*` (reading, groups, conflicts, explanation, derivation), `CCLIIntervalBatchProcessingLoader.*` and `Interface/CommandLine/CCLIIntervalComandLinePreparationTranslator.*` (the command).
