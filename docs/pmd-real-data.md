# What time looks like in real PMD data (Creep Testing Ontology, 12 real creep tests)

Data: `D:\Github\PMDco-Reasoning-Workshop\part3-reasoners\data` (`cto_small.ttl` is one real test, `cto_full.ttl` is all twelve). Check script: `experiments/pmd_creep/pmd_order_check.py`.

## 1. There are no calendar time stamps

The records contain **no dates and no clock times**. The temporal information is of two kinds only:

| Kind | Where | Example |
|---|---|---|
| **Order of processes** | `x preceded by y` (BFO_0000062) between processes | manufacturing, heat treatment, phase transformation, supply chain, microscopy, data processing, each linked to the creep testing process |
| **Durations** | value specifications with unit `HR` on `heating_time`, `soaking_time`, `test_duration`, `creep_rupture_time` | heating 3.45 h, soaking 1.41 h, test 78.7 h |

The temporal instants (`temporal_instant_start/end`) exist as individuals but carry no value.

Durations of the 12 tests (hours):

| Test | rupture time | heating | soaking | test duration |
|---|---|---|---|---|
| C-78 | 78.7 | 3.45 | 1.41 | 78.7 |
| C-80 | 72.4 | 1.87 | 1.32 | 72.4 |
| C-81 | 491 | 2.35 | 1.92 | 491 |
| C-82 | 180 | 1.67 | 1.88 | 180 |
| C-85 | none | 2.04 | 2.03 | 1030 |
| C-89 | 74.5 | 2.99 | 1.58 | 74.5 |
| C-91 | 492 | 2.06 | 2.12 | 492 |
| C-94 | none | 1.74 | 1.82 | 1011 |
| C-95 | none | 1.61 | 2.81 | 1010 |
| C-97 | 161 | 2.02 | 2.13 | 161 |
| C-98 | 484 | 1.43 | 2.69 | 484 |
| C-99 | 170 | 2.03 | 1.91 | 170 |

Observation (data only, no claim about materials): in all 9 tests that have a rupture time, the test duration equals it. The three tests without a rupture time ran about 1,010 to 1,030 hours.

## 2. A real finding: the order links point the wrong way

In **all 12 records** the same six links appear, always as "`<process>` preceded by `<creep testing process>`", and `precedes` (BFO_0000063) is never used.

The ontology itself defines the relation as: "x is preceded by y iff the time point at which y ends is before or equivalent to the time point at which x starts" (and declares `preceded by` the inverse of `precedes`). So, read as defined, the data says that the creep test took place **before** the manufacturing, the heat treatment, the phase transformation and the supply chain of the same test piece. The ontology's own text for the manufacturing description says the opposite ("a manufacturing process which performed for preparing the sample before creep testing process"), and its axioms (`manufacturing process and (preceded by some creep testing process)`) use the same direction as the data. The data and the axioms agree with each other, but not with the definition of the relation or with the descriptions. (The annotation `skos:definition "b preceded by c =Def b precedes c"` of the imported relation reads the other way round, which may be the source of the confusion. This needs a check with the authors of the ontology.)

How the tools show it (plain OWL, no time module):

1. Add the rule "a manufacturing process, heat treatment, phase transformation or supply chain process is never preceded by a creep testing process" (`SubClassOf(C ObjectAllValuesFrom(preceded_by ObjectComplementOf(CreepTestingProcess)))`).
2. `Konclude consistency` on the data as published: **inconsistent**. `Konclude explain` returns a minimal justification of 4 axioms (the rule, the "preceded by" link, and the two class assertions), 2.9 s.
3. With the four links turned around (`precedes`), the same rule: **consistent**. Without the rule: consistent (nothing is wrong formally, the problem is the meaning).

Reproduce: `python3 experiments/pmd_creep/pmd_order_check.py KONCLUDE cto_small.ttl OUTDIR`.

**The interval module on the real data.** `Konclude interval` now reads the BFO relations `precedes` and `preceded by` (as "before or meets" and "after or met by"). On all 12 real tests it reads the 72 order links (6 per test), checks the 12 groups and finds them consistent, as expected for a star-shaped structure (every process linked to the test, no chain, no cycle): it derives nothing new here, in 0.3 s; `consistency` gives the same verdict as before at the same cost. The reading matters for datasets with chains of processes (supply chain, manufacturing, heat treatment, test, each linked to the next): then composition derives the implied order, and a cycle, or a contradiction with time stamps or with an opposite link, is found without any rule. Test cases 13 to 16 in `tests/interval/` show this.

## 3. What this means for the language of the temporal layer

| What the real data contains | What is needed |
|---|---|
| Order of processes without time stamps | **qualitative order** (before/after, Allen relations): the interval module, or plain OWL rules as above |
| Durations with units | **duration constraints** (for example "test duration = rupture time", "heating plus soaking is shorter than the test"): metric constraints over intervals, not snapshot logic |
| No calendar time, no stream | the snapshot logic (LTLf over states) has nothing to run on here |

So for **PMD-type process data** the first language should be **qualitative order plus duration constraints over process individuals** (Allen relations with numeric bounds), and the snapshot logic with deadlines is the second layer for operational data with real time stamps (a lab that logs events over days).
