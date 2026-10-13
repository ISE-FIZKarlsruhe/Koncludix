# Commands: what you can run

Everything below is run from the repository folder. `Konclude` means the built program (`./Konclude` on Linux, `Binaries\Konclude.exe` on Windows).
Ontology files can be `.owl`, `.ofn`, `.ttl`, `.rdf`, `.nt`, `.owx`.

## 1. Reasoning with Konclude

```bash
./Konclude consistency   -i ontology.owl            # is the ontology consistent?
./Konclude classification -i ontology.owl -o tree.owl   # the class tree
./Konclude materialize   -w 1 -i ontology.owl -o inferred.nt   # write ALL facts that follow
```

Choose what `materialize` writes (each switch is `true` or `false`, given in a small config file with `-c switches.xml`):
types of individuals, facts between individuals, same-as pairs, class tree, property tree
(keys `Konclude.CLI.Materialize.ObjectPropertyAssertions`, `DataPropertyAssertions`, `SameIndividuals`, `ClassHierarchy`, `PropertyHierarchy`).
Only the types of individuals is about half the time of everything.

## 2. Tools for ontology developers

| I want to know | Command |
|---|---|
| What in my ontology will be expensive? (2 seconds, no reasoning) | `./Konclude lint -i ontology.owl` |
| Why is it inconsistent? | `./Konclude explain -i ontology.owl` |
| Why is X a member of class C? | `./Konclude explain -i ontology.owl --type "<X>" "<C>"` |
| Why is A a subclass of B? | `./Konclude explain -i ontology.owl --subclass "<A>" "<B>"` |
| Where does the time and memory go? | `python3 tools/konclude_diagnose.py ./Konclude ontology.owl --task materialize` |
| Nice examples of reasoning, in English | `python3 tools/konclude_examples.py ./Konclude ontology.owl --classes "Some class" --top 5` |
| A cheaper copy of the ontology (closed lists and covering axioms taken out) | `python3 tools/konclude_relax.py ./Konclude ontology.owl relaxed.ofn` |

`konclude_diagnose.py` options: `--task consistency|classification|realization|materialize`, `--max-memory-mb 9000` (the run is stopped there, and that is a result),
`--timeout 600`, `-w 1`, `-c config.xml`, `--json report.json`.
`konclude_examples.py` options: `--classes`, `--kind types|subclasses`, `--top N`, `--include-chains`, `--out examples.md`.

## 3. Updates without starting again (incremental)

```bash
./Konclude owllinkserver -p 8080 -w 1 -c experiments/incremental/incr_cfg_on.xml &
# then send OWLlink requests: <Tell> to add facts, <Retract> to take them back, any query to ask
curl -X POST --data '<Tell> ... </Tell>' http://localhost:8080
```
Details and the 18 tests: `docs/incremental-revisions.md`.

## 4. Time

```bash
./Konclude interval -i ontology.ttl -o derived-relations.nt     # time stamps and Allen relations, cycles found as inconsistencies
python3 tools/temporal/tlayer.py --tbox ontology.ofn --facts facts.tl --formula "F Employee(Alice)" --live    # questions about time
```
Details: `docs/interval-module.md`, `docs/temporal-layer.md`. One-command demo: `bash demo/run_demo.sh`.

## 5. Tests

```bash
python3 tests/incremental/run_revision_tests.py ./Konclude
python3 tests/diagnose/run_diagnose_tests.py   ./Konclude
python3 tests/examples/run_examples_tests.py   ./Konclude
python3 tests/interval/run_interval_tests.py   ./Konclude
python3 tests/temporal/run_temporal_tests.py   ./Konclude
```

## 6. How the examples in plain English are made (short)

1. `Konclude materialize` gives everything that follows: the class tree and the types of every individual.
2. Conclusions you did not write down are the candidates (plain chains of SubClassOf are boring and dropped).
3. For each candidate `Konclude explain` finds the smallest set of axioms that makes it follow.
4. Each axiom is turned into a sentence by fixed templates (no language model): `SubClassOf(A B)` becomes "Every A is a B.",
   `ObjectMinCardinality(3 likes Interest)` becomes "something that likes at least 3 interest", and so on. Names come from `rdfs:label` when the ontology has labels, otherwise the class name split into words.
5. Examples are ranked by size and by the mix of kinds of axioms (definition, restriction, cardinality, enumeration, disjointness, property).

More: `docs/simple-guide.md`.
