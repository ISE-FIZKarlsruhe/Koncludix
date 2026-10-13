# The temporal layer: facts that are true only for a while

`tools/temporal/tlayer.py` lets you ask questions about **time** on top of Konclude. It does not change Konclude. It cuts the history into short periods, asks Konclude what is true in each period, and then checks your time rule over those periods.

## The idea in five lines

1. Your ontology (the rules about the world) stays **fixed**: "every Manager is an Employee".
2. Your **facts** have a validity time: "Alice is a Manager from 1 June 2025".
3. The layer finds every day on which some fact starts or stops. Between two such days nothing changes. Each such period is one **state** (a snapshot).
4. For each state, Konclude works out what follows (Alice is an Employee, because she is a Manager).
5. A small **time logic** (LTLf) is checked over the list of states: "always", "sometime later", "until", "before".

## How to use it

```
python3 tools/temporal/tlayer.py --tbox ontology.ofn --facts facts.tl --formula "F Employee(Alice)" [options]
```
Set `KONCLUDE_BIN` (or `--konclude PATH`) to your Konclude binary. The ontology must be in OWL functional syntax (`.ofn`).

**The facts file** (one fact per line):
```
Employee(Alice)           [2025-01-01, 2025-08-31]
Manager(Alice)            [2025-06-01, inf)
worksFor(Alice, ACME)     [2025-01-01, 2025-08-31]
Contractor(Carla)                                      # no time = always valid
```
`]` means the end day is included, `)` means it is not. Use dates (`YYYY-MM-DD`, one day is one step) or whole numbers (one tick is one step). `inf` means "never ends".

**The formulas:**

| Write | Means |
|---|---|
| `Employee(Alice)`, `worksFor(Alice, ACME)` | a fact about one individual; `?x` is a variable that stands for every individual |
| `! & \| ->` | not, and, or, if-then |
| `F p` | **sometime** from now on, p holds |
| `G p` | p holds **always** from now on |
| `X p` | p holds in the **next** state |
| `p U q` | p holds **until** q holds (q must happen) |
| `O p`, `H p`, `Y p`, `p S q` | the same ideas about the **past**: once, always so far, in the previous state, since |
| `F[a,b] p`, `G[a,b] p` | **deadlines**: p holds sometime / always between a and b days (or ticks) from now. Example: `F[0,5] Test(x)` = a test within 5 days |
| `O[a,b] p`, `H[a,b] p` | the same into the past: p held once / always during the days a to b ago |

Example rule: `G(Employee(?x) -> F Manager(?x))` ("everybody who is an employee becomes a manager at some time"). The layer checks it for every individual and tells you who breaks it.

## The answers: four values, not two

OWL does not say "false" when a fact is simply missing. So each answer is one of:

| Answer | Meaning |
|---|---|
| **TRUE** | follows from the facts and the ontology, whatever happens later |
| **FALSE** | the opposite follows, whatever happens later |
| **UNKNOWN** | the ontology and facts neither say yes nor no (open world) |
| **PENDING** | not decided yet: something in the future could still make it true or false. The report says what it is still waiting for, and what the answer would be if the history ended now |

Two options change how you read the end of the history:

- `--closed`: the history is complete. A promise that was not kept ("sometime later ...") counts as broken.
- `--validation`: a fact that does not follow is counted as **false** (the "closed world" way of checking data). This is easy to read, but it is not pure OWL, so you must ask for it.

Formulas with `[a,b]` bounds are checked **day by day** (one step is one day or tick; atoms stay constant within a state), so `X` and `Y` mean the next and the previous day in such a formula. They need `--now DATE`, the day on which the observation ends: a deadline whose last day is before `--now` is decided (a missed `F[0,5]` becomes FALSE exactly on the day start + 5, and the report says on which day it was fixed); a deadline that ends after `--now` stays PENDING. The argument of a bounded operator may not contain a future operator.

`--why` adds the explanation over time (see above). `--define` and `--define-file` add temporal definitions (see below). Other options: `--now DATE` (cut the history at that day), `--at DATE` (start checking at that day, the past is still visible), `--explain` (show **why**: the smallest set of axioms and facts behind each atom, from Konclude `explain`), `--timeline` (list the states), `--show problems` (with variables, hide the TRUE ones), `-o result.json`.

## Temporal definitions: time results that go back into the reasoner

The time rules above only **check**. A temporal definition **derives**: it gives a class to an individual for exactly the periods in which a formula about the past is true, and the derived class then takes part in the ordinary reasoning (class hierarchy, disjointness, inverse properties, everything the ontology says).

```
--define "FormerEmployee(?x) := O Employee(?x) & !Employee(?x)"
--define "Overdue(?x) := H[0,35] !UnderMachining(?x) & O UnderMachining(?x) & !O UnderTensileTest(?x)"
```
How it works, in two passes:
1. The facts are reasoned over as usual (every snapshot).
2. For every individual, the definition is evaluated over the history. Where it is TRUE, the class is added as a fact with its validity period. The snapshots are rebuilt with these facts and reasoned over again (unchanged groups come from the cache).

Then the ontology can use the derived class. Example (`tests/temporal/hr_tbox_defs.ofn`): `FormerEmployee ⊑ Alumnus` and `Alumnus` disjoint from `Student`. Bob stopped being an employee on 31 March, so from 1 April the layer derives `FormerEmployee(Bob)`, the reasoner concludes `Alumnus(Bob)`, and `F Alumnus(Bob)` is TRUE (fixed on 2025-04-01) with the justification "SubClassOf(FormerEmployee Alumnus), ClassAssertion(FormerEmployee Bob) <- derived by the definition of FormerEmployee". Yan became a student again in May, so from 1 May the combination of the derived `Alumnus` and the asserted `Student` is **inconsistent**, and the report says which fact came from a definition.

Rules for a definition: one variable; the formula may only look at the **present and the past** (`O`, `H`, `Y`, `S`, `O[a,b]`, `H[a,b]`, atoms; `inf` is allowed as the upper bound, so `O[30,inf]` means "at least 30 days ago"); it may use classes and properties of the ontology and the facts but **not another defined class** (so definitions cannot depend on each other or on themselves); bounds need `--now`. Because a definition looks only at the past and does not use other definitions, there are no circular definitions and the result is exact; time inside class expressions (the undecidable temporal description logics) is avoided. A derived class is not fed back into its own definition: the definition sees the first pass, the formulas and the ontology see both.

## Why an answer? The explanation over time (`--why`)

For every TRUE or FALSE answer that is fixed, `--why` gives the **smallest set of atom values over time** that decides it. Every value in the set is needed: if you take any one away, the answer is no longer decided. (It is found by removing values and checking, and the test suite checks both properties on thousands of random cases.) `--explain` adds, for each value the reasoner derived, the smallest set of ontology axioms and facts behind it, with the facts from definitions marked.

Example (`tests/temporal`, rule `G(Employee(?x) -> F[0,200] Manager(?x))`, observation until 2025-12-31):
```
?x=Bob  FALSE   (fixed at 2025-08-20)
  why FALSE: the smallest set of atom values over time that decides it (every one is needed):
    Employee(Bob)     true   2025-02-01 .. 2025-02-28
        ClassAssertion(Employee Bob)
    Manager(Bob)      false  2025-02-01 .. 2025-08-31   (not entailed: counts as false in validation mode)
```
Read: Bob was an employee from 1 February, and he was not a manager during the whole deadline window (200 days, until 20 August). Nothing else matters for this answer. In the PMD-style campaign the same output explains a late test: "no machining 2028-01-28 .. 2028-03-05, no test on the days before, test starts 2028-03-05". Values are shown as periods of states (the layer works with states, so a period is rounded to the states that contain the needed days). A value marked "not entailed" counts as false only because of `--validation`.

## What you get

```
[q1] G(Employee(?x) -> F Manager(?x))
  3 FALSE, 2 TRUE
  ?x=Carla  FALSE
      Employee(Carla): true [2025-03-01, ∞]
        why true at 2025-03-01:
          SubClassOf(Contractor Employee)
          ClassAssertion(Contractor Carla)
      Manager(Carla): false [2025-01-01, ∞]
```
For each answer: the periods in which each atom is true or false, and the justification.

## Live mode (`--live`)

Normally the layer starts a new Konclude process for every changed group of individuals in every period, so the terminology is prepared again each time. With `--live` it starts **one** Konclude (OWLlink server, `IncrementalRebuild` on) with **one** knowledge base: the TBox is loaded once, and at each change point only the facts that began are told and the facts that ended are retracted. The types of the changed individuals are read with `GetFlattenedTypes`, the role fillers with `GetFlattenedObjectPropertyTargets`. The answers are the same as without `--live`; if anything goes wrong the layer says so (`WARNING: live mode stopped ...`) and continues with one process per group. An inconsistent group is found by testing it alone, kept out of the knowledge base, and reported as before. `TLAYER_LIVE_TRACE=1` prints every request with its time.

**Open world and validation mode.** In the open-world mode the layer needs to know when a fact is *entailed to be false* (for example Dan is a student, so he is not an employee). The live mode asks for the instances of the complement of the class (`GetFlattenedInstances` of `ObjectComplementOf(C)`); it needs no helper class in the terminology (a class defined as a complement would make Konclude classify the whole terminology again after every update). In **validation mode** ("not entailed counts as false") the negative information is not needed at all and is not asked for.

**Safety nets.** (1) A fact that comes back after it was retracted is never re-told into the same knowledge base (Konclude can lose it): the layer starts a fresh knowledge base from the TBox (`knowledge base restarts` in the live line). (2) A request that takes longer than `--live-timeout` seconds (default 300) makes the layer give up live mode and continue with one process per group, with the same answers (a hang in Konclude's incremental mode was seen in rare cases).

**Checked:** all 20 checks of `tests/temporal/` give the same result with `TLAYER_EXTRA=--live`, and the random timelines (with inconsistent periods) give identical answers in normal and live mode. **Real PMD ontology** (`experiments/temporal/tlayer_live_pmd.py`; terminology 3,331 axioms, one creep test record of about 450 facts per period, validation mode, WSL, one worker):

| | normal (a process per period) | live |
|---|---|---|
| 8 periods, facts stay | 121 s (15 s per period) | 16.7 s (about 10 s for the first period, then about 1 s) |
| 12 periods, facts stay | 200 s | 22.8 s (**8.8 times faster**) |
| 8 periods, every record valid for 3 periods (3,122 facts retracted along the way) | 127 s | 25.5 s (5.0 times faster) |

In all three runs the answers are identical (for example 564 TRUE of 2,264 individuals at 12 periods). Single runs on one machine. The saving grows with the number of periods because the first period (10 s) is paid once.

## Inconsistent states

If the facts of some individuals contradict the ontology in one period (for example somebody is a Student and an Employee while the ontology says these are disjoint), the layer reports that period, prints the smallest set of axioms that contradict each other, treats these individuals as UNKNOWN in that period, and carries on with everybody else.

## How it stays fast

Reasoning every period from scratch is expensive, but from one period to the next only a few facts change. The layer splits each state into **connected groups** of individuals (individuals that are linked by a property). If a group has exactly the same facts as in an earlier period, the earlier answer is reused. Only changed groups go to Konclude, in one batch per state.

This is safe only if groups cannot influence each other. They can when the ontology contains **nominals** (`ObjectOneOf`, `ObjectHasValue`), individual axioms, keys or the universal role. The layer checks the ontology text for these. If it finds one it reasons over the whole snapshot at once and tells you why (`--whole-snapshots` forces this too).

## A realistic test with known answers (made-up PMD-style data)

`experiments/temporal/pmd_like_eval.py KONCLUDE N SEED [--now-day D] [--interval]` builds a materials-testing campaign and checks the layer against the truth. Every sample goes through casting, forging, heat treatment (annealing, quenching, tempering), machining and tensile/hardness test, and is archived afterwards. The facts are "Under<step>(sample) [from, to]" in days. About 18 % of the samples get one injected problem:

| Problem | How it shows up |
|---|---|
| order | a step starts while an earlier step still runs, or before it |
| no_test | the sample is archived but was never tensile-tested |
| quarantine | the sample is put on hold while its tensile test runs; the ontology says these are disjoint, so the period is inconsistent |
| overdue (a temporal definition, not injected) | `Overdue := H[0,35] !UnderMachining & O UnderMachining & !O UnderTensileTest` (machining ended at least 36 days ago, no test yet); the ontology makes `Overdue` a `NeedsAttention`, and `G(!NeedsAttention(?x))` is checked |
| late_test | the tests start 40 to 70 days late; the deadline rule is `G((UnderTensileTest(?x) & !Y UnderTensileTest(?x)) -> O[0,35] UnderMachining(?x))` (a test must start within 35 days after machining) |

The truth comes from a **literal day-by-day simulation** of the rules, which does not use Konclude, the layer or Allen's algebra. The rules are, in the layer's language: `G(Under<Later>(?x) -> (!Under<Earlier>(?x) & O Under<Earlier>(?x)))` for five (later, earlier) pairs (the class hierarchy of the ontology applies: a rule about heat treatment also covers quenching) and `G(Archived(?x) -> O UnderTensileTest(?x))`. The check runs with `--validation`.

| Run | States | Violating samples (truth): order, no_test, quarantine, late_test | Found | Missed | False alarms | Time |
|---|---|---|---|---|---|---|
| 300 samples, whole campaign | 1,298 | 26, 12, 11, 12 | 26, 12, 11, 12 | 0 | 0 | 106 s |
| 300 samples, history cut at day 700 (campaign still running) | 567 | 14, 6, 4, 4 | 14, 6, 4, 4 | 0 | 0 | 42 s |

99 % of the component checks (256,087 of 258,842) came from the cache. An earlier run of the same campaign (without the deadline rule, 300 samples) was also checked with the interval module and DL rules (Allen relations derived by Konclude from time stamps, `consistency` and `explain` per violating sample): 42 of 42 order violations found, none missed, none invented, and the two ways of stating the order rule (day-by-day and Allen) picked the same 42 samples. In validation mode an inconsistent snapshot keeps its atoms UNKNOWN (it is not turned into false); otherwise a quarantined sample would produce false 'test starts again' events.

## Tests (`tests/temporal/run_temporal_tests.py PATH_TO_KONCLUDE [--random N]`)

| Check | What it shows |
|---|---|
| evaluator | 2,000 random formulas on random traces: with a complete history the value equals the textbook definition; TRUE and FALSE are never wrong for any continuation (checked for all continuations up to 3 states) |
| HR example (7 checks) | an inferred fact, the three values, PENDING and UNKNOWN for open obligations, validation mode, past operators, property atoms, `--now` |
| inconsistent state | it is reported with its cause and does not hide the other people |
| cache | same answers as without the cache, fewer Konclude calls |
| random timelines | per-component cached, uncached and whole-snapshot runs give the same answers (25 random timelines, 4 formulas each) |

All 20 pass (two of them are about the explanation over time: the random check that every set decides the answer and is minimal, and the Bob example; four of them are about definitions: derivation and the effect on the ontology, an inconsistency caused by a derived class, a definition with a deadline, and the rules for definitions). The evaluator check includes the deadline operators (47 % of the random formulas contain one). Two checks are about deadlines: the day on which a missed deadline becomes FALSE, and the error message when `--now` is missing.

## Speed (measured, WSL, 1 worker, 100 states)

| Ontology | People | With cache | Without cache | Same answers |
|---|---|---|---|---|
| light (subclass, disjointness) | 400 | 4.8 s | 5.3 s | yes |
| light | 3,000 | 15.8 s | 15.5 s | yes |
| heavy (`≥3 likes.Interest`, private interests) | 200 | 5.1 s | 5.3 s | yes |
| heavy | 1,000 | 6.0 s | 15.1 s | yes |
| heavy | 3,000 | **10.8 s** | **42.0 s** | yes |

When reasoning is cheap, the time is the fixed cost of about 35 ms per Konclude call, and the cache does not matter. When reasoning per snapshot is expensive, the cache saves most of the work (3.9 times at 3,000 people). An earlier version had a quadratic step in handing out Konclude's answers; the profile showed it and it is fixed (31 s to 16 s at 3,000 people).

## Limits (honest)

- **Individuals that are linked to a shared hub** (a lab, a machine, an enumerated list) form one big group, so the cache cannot help there. Nominals switch it off completely.
- **Konclude's own incremental mode is not used** (it is not usable for this). The cache lives in the layer.
- Time is discrete (days or ticks), one linear timeline, no branching futures. The ontology itself does not change over time. Time operators only act on facts and formulas, not inside OWL class expressions.
- Only class atoms and property atoms between named individuals. A property atom is TRUE or UNKNOWN (an absent link is not entailed false). Data properties are not read.
- Single class names only in atoms (define a named class in the ontology for a complex expression). The names `G F X H O Y U S` cannot be used for classes.
- The interval theory (`docs/interval-module.md`) runs inside every `consistency` call, so Allen relations given as property facts (for example `before(a, b)`) are checked in each state. Time stamps as data values (PROV-O) cannot be written in the facts file yet, and `materialize` does not use relations that the module derives.
- At most two variables per formula. Variables range over all individuals that appear in the facts.
- Tested on synthetic data. A sample of real PMD time stamps is still missing.
