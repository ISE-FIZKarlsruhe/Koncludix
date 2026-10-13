# Demo kit: the temporal work in one command

```bash
# Linux or WSL, from the repository folder
bash demo/run_demo.sh                 # all steps (about 5 minutes)
QUICK=1 bash demo/run_demo.sh         # without the 300-sample campaign (about 1 minute)
bash demo/run_demo.sh /path/to/Konclude   # give the Konclude build of this repository yourself
```
The script looks for a Konclude build that has the `interval` command (first `~/kon_all2/Release/Konclude`, then `Release/Konclude` in the repository).

| Step | What you see | What to say |
|---|---|---|
| 1 | `Konclude consistency` on a cycle `a before b, b before c, c before a`: **inconsistent**, with the three facts | Plain OWL axioms cannot see this (0 of 427 such conflicts found by axioms, 427 of 427 by the interval step). |
| 2 | `Konclude explain` on a claim ("a after b") that contradicts the time stamps: 5 axioms | The justification contains the claim and the time stamps, not only the claim. |
| 3 | A process-order rule over time stamps (`11_rules_over_derived_relations.ofn`): the file has **no** Allen relation, the reasoner derives them; sample 2 breaks the rule; 11-axiom justification | Time stamps are enough; the rule is ordinary OWL. |
| 4 | The temporal layer on the HR example with a deadline rule `G(Employee(?x) -> F[0,200] Manager(?x))`, with `--why --explain` | The four answers; the smallest set of facts and periods behind each sure answer ("he was an employee from 1 Feb and not a manager during the 200 days"); "FALSE (fixed at 2025-08-20)" = Bob's deadline passed on that day; Alice is PENDING; each atom comes with its justification. |
| 4b | Temporal definition: `FormerEmployee(?x) := O Employee(?x) & !Employee(?x)`; the ontology says `FormerEmployee ⊑ Alumnus` and `Alumnus` is disjoint from `Student`; Yan becomes a student again and the period is reported inconsistent, the cause naming the definition | Time **creates** knowledge that the reasoner then uses (past-only, no recursion, so it stays exact). |
| 4c | The diagnosis tool (`tools/konclude_diagnose.py`) on a generated hobby ontology: stages with memory, the concept expressions ranked by their share of the new nodes and branches ("a choice among 8 enumerated individuals"), what to try | When reasoning is slow or does not finish, this says why. Works on a stopped run too (see `docs/lint-and-diagnose.md`: DL-5 classification stopped at 70 s: 72 % of the work is the 28-way hobby enumeration). |
| 4d | `tools/konclude_examples.py` on a small ontology: the best multi-step conclusions, each with its smallest set of axioms in plain English; `--classes` limits it to classes you name | The feature for finding "nice examples" of reasoning, also on the real PMD ontology . |
| 5 | **Real PMD data** (Creep Testing Ontology, 12 real creep tests): the rule "manufacturing is never preceded by creep testing" makes the published data inconsistent; with the links turned around it is consistent | All 12 records say the test came before the manufacturing and the heat treatment (as the ontology defines `preceded by`). Needs confirmation by the authors. |
| 6 | The tests: interval module 24 checks, temporal layer 20 checks | The evaluator is checked against a brute-force definition (2,000 random formulas, many with deadlines). |
| 7 | A campaign of 300 made-up samples with four kinds of injected violations (order, no test, quarantine, late test) plus a derived class `Overdue`; truth from a separate day-by-day simulation | Found exactly: 26, 12, 11, 12 and 24 samples, no misses, no false alarms, 116 s. |

Documents: `docs/simple-guide.md` (everything in simple words), `docs/temporal-layer.md`, `docs/pmd-real-data.md`.
