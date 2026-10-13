# Lint and diagnose: what exactly takes the time and the memory

When reasoning is slow, uses too much memory, or does not finish, two tools tell you why.

| Tool | What it does | Needs a run? |
|---|---|---|
| `Konclude lint -i FILE` | Reads the ontology and warns about constructs that are expensive or not handled (static, instant). It cannot know what really costs. | No |
| `python3 tools/konclude_diagnose.py KONCLUDE FILE --task ...` | **Runs** the task with a profiler, stops it when a time or memory limit is reached, and tells you which stage grows, which concept expressions the reasoner spent its work on, which lint findings explain them, and what to try. | Yes, up to your limits |

The last line of the lint report points to the second tool.

## How to use

```bash
python3 tools/konclude_diagnose.py ~/kon_all2/Release/Konclude my.owl --task consistency
python3 tools/konclude_diagnose.py ~/kon_all2/Release/Konclude my.owl --task classification --timeout 120 --max-memory-mb 6000
python3 tools/konclude_diagnose.py ~/kon_all2/Release/Konclude my.owl --task materialize --json report.json
```
`--task` is `consistency`, `classification`, `realization` or `materialize`. `--timeout` and `--max-memory-mb` stop the run (a stopped run is a result, not a failure: the report shows what the reasoner was busy with). `-w N` sets the workers (default 1, which gives the most exact numbers), `-c FILE` passes a configuration file.

The profiler itself is inside the reasoner and switched on by environment variables, so you can also use it without the tool: `KONCLUDE_PROFILE=1 KONCLUDE_PROFILE_FILE=profile.txt KONCLUDE_PROFILE_INTERVAL=5 Konclude classification -i my.owl -o out.owx`. The reasoner writes the table every 5 seconds (and at the end), always into the same file, so a killed run leaves the latest table.

## What the report contains

1. **Result:** finished, or stopped by the time or memory limit, with time and peak memory.
2. **Stages of the run** (loading, preprocessing, precomputing, classification, realization) with the time and the memory at each, and where the memory grows fastest.
3. **What the tableau worked on:** the concept expressions ranked by their **share** of all new nodes and new branches created. These are the things that have to be stored and processed, so they are what costs memory and time. For each: calls, new nodes, new branches, the time spent inside the rule code, and a plain meaning ("a choice among 28 enumerated individuals", "a minimum cardinality creates new nodes", ...).
4. **Lint findings** that match the concepts in the table, with their suggestions.
5. **What to try**, taken from the top rows.

## Example 1: OWL2Bench DL-1, consistency (8 s, 1.7 GB)

```
57%   0   462,840 branches   a choice among 28 enumerated individuals (the hobbies, "Interest")
32%   0   262,515 branches   a choice among 10 alternatives (a DisjointUnion of the engineering disciplines)
 9%   0    71,595 branches   a choice among 5 alternatives (the disciplines)
 0% 1,140 nodes              a minimum cardinality creates new nodes (>=3 likes.Interest)
Time inside tableau rule code: 1.3 s of 8.4 s (16 %).
```
So 89 % of the new branches come from two choice patterns. The report also lists the matching lint findings (large nominal enumerations, wide disjunctions, qualified cardinalities) with their suggestions.

The same file with the at-least optimisation switched off (`Konclude.Calculation.Optimization.AtLeastBackendNeighbourSatisfaction` = false): 5,933 new nodes (3,945 of them from `>=3 likes.Interest`), 2.1 million branches, 42.8 s and 3.5 GB, against 2,142 nodes, 0.8 million branches, 10.8 s and 1.8 GB with it. The profile numbers move exactly where the earlier hand-made investigation said they would (3,945 fresh nodes created by that rule).

## Example 2: OWL2Bench DL-5, classification, stopped after 70 s

The run did not finish. The report said: precomputing finished after 38 s (1.5 GB), then classification; the last table (at 61 s) shows 11.3 million new branches: **72 %** from the 28-way hobby enumeration, **21 %** from the 10-way disciplines, 6 % from the 5-way faculties; the nominal rule for single hobbies such as `{Travelling}` was among the slowest rules (1.7 s). Lint's findings for the same concepts (enumerations, wide disjunctions) were marked as seen in the profile.

## What the numbers do and do not mean (honest list)

- **Concept expressions are the preprocessed ones.** The reasoner works on a normalised form of your axioms (a covering `DisjointUnion` or a defined class appears as a negated conjunction, `¬(¬A AND ¬B ...)` = "A or B or ..."). Class and property names are readable, internal numbers are removed. It shows *which expression*, not yet *which line of your file*; the lint finding and the names usually make the connection obvious.
- **Share, not seconds.** The time spent inside the rule code is only 16 to 18 % of the run in both examples. The rest is task handling and memory copying for the branches that were created, plus parsing, preprocessing and saturation, none of which is attributed to a concept. That is why the ranking is by **new nodes and new branches** (what has to be stored), not by rule time.
- **Memory is attributed indirectly.** The report gives the real memory per stage and per 10 seconds (measured from the process), and the number of nodes and branches per concept. It does not measure bytes per concept. Bytes per branch are not constant (copy-on-write).
- **Only tableau work is counted.** Work in the saturation, the ABox backend cache, the realizer's bookkeeping and the parser does not appear in the table. If no tableau rule runs at all, the report says so and you read the stages.
- **One worker is exact.** With several workers the counters are kept per thread and merged when written; the totals are approximate.
- The profiler is **off by default** and costs nothing then (DL-1: 7.4 s with it, 7.6 to 9.0 s without: noise). With it on there is no measurable difference either, but it writes a table every few seconds.
- The "what to try" lines are hints from what we measured so far. A rewrite of the ontology can change its meaning; the lint suggestions say under which condition it does not.

## Tests

`python3 tests/diagnose/run_diagnose_tests.py PATH_TO_KONCLUDE` (7 checks): the profiler is off by default and changes nothing; with it on the table is written and the output file still comes out; on a generated hobby ontology (`tests/diagnose/make_hobby.py`) the top concept is the enumeration and the minimum cardinality shows new nodes; the numbers add up; a run stopped by the time limit and one stopped by the memory limit are reported as such and still show the last table. Also checked: `materialize` over the 231 small test ontologies with the profiler on gives no crash (one known timeout, `c0162`, with or without the profiler), and the `consistency` verdicts are unchanged (only the earlier known difference `c0266`).

Source: `Source/Utilities/CRuleProfiler.h` (header only), hooks in `CCalculationTableauCompletionTaskHandleAlgorithm.cpp` (the one function through which every tableau rule application passes, node creation, branch creation), `tools/konclude_diagnose.py`, and the pointer line in `CLintOntologyAnalyser.cpp`.
