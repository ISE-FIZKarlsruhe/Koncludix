# Finding nice examples of reasoning in your ontology

You need good examples of what a reasoner does for a talk, a paper or a lecture. Finding by hand "a conclusion that needs five steps" is slow. `tools/konclude_examples.py` finds them for you, and you can tell it which classes you care about.

```bash
python3 tools/konclude_examples.py ~/kon_all2/Release/Konclude my.ttl --top 10 --out examples.md
python3 tools/konclude_examples.py ~/kon_all2/Release/Konclude my.ttl --classes "heat treatment,creep testing process" --involving --max-explain 60
```
Formats: OWL functional syntax (.ofn), OWL/XML (.owx), and Turtle, RDF/XML, N-Triples (these need the Python package `rdflib`: `pip install rdflib`).

## What it does, in simple steps

1. Konclude works out everything that follows (`materialize`).
2. Conclusions that you never wrote down are the candidates. Conclusions that only follow a chain of "A is a kind of B, B is a kind of C" are boring and left out (`--include-chains` brings them back). Of the types of an individual it keeps only the most specific ones.
3. For each candidate, Konclude `explain` finds the **smallest set of axioms** that makes it follow. Nothing can be removed from that set (the test suite checks this independently).
4. The examples are ranked: more axioms and a wider **mix of kinds** (definitions, restrictions, inverse or transitive properties, cardinalities, disjointness, enumerations) rank higher than a plain chain.
5. Examples that follow exactly the same rules for different individuals are merged: "The same pattern also applies to: Ni, Hf, Sb, ... and 19 more".
6. Each example is written in plain English, using the **labels** of your ontology (English first, other languages are matched too), with the original axiom below each line.

## Asking for examples about certain classes

| Option | Meaning |
|---|---|
| `--classes "A,B"` | class names, local names or labels (any language, partial words work if nothing matches exactly). Only conclusions **about** these classes. |
| `--involving` | also examples where the class only appears **somewhere in the reasoning** (in one of the axioms of the justification). Needs more explanations: raise `--max-explain`. |
| `--with-subclasses` | the subclasses of those classes count too |
| `--kind types\|subclasses\|all` | conclusions about individuals, about classes, or both |
| `--min-axioms N` | the smallest justification that counts as an example (default 3) |
| `--top N`, `--out FILE`, `--json FILE` | how many to write, and where |
| `--max-explain N`, `--jobs N`, `--timeout S` | how many candidates are explained (default 40), in parallel, with a limit per explanation |

## Example (the small test ontology `tests/examples/university.ofn`)

```
Example 1: anna is a busy person.            9 axioms; kinds: cardinality, definition, fact about a pair, subclass
  1. Every graduate student is a student.
  2. Every student is a person.
  3. A busy person is defined as: person and something that attends at least 2 course.
  4. anna is a graduate student.     5. c1 is a course.     6. c2 is a course.
  7. c1 and c2 are different individuals.     8. anna attends c1.     9. anna attends c2.

Example 2: cs is a project group.            5 axioms; kinds: definition, fact about a pair, property, restriction
  1. 'is a member of' implies 'is part of'.  2. A project group is defined as: department and something that is part of some department.
  3. cs is a department.  4. faculty is a department.  5. cs is a member of faculty.

Example 4: bob is a teacher.                 3 axioms; kinds: fact about a pair, property
  1. 'teaches' and 'is taught by' are inverse.  2. Anything that teaches something is a teacher.  3. c1 is taught by bob.
```

## On the real PMD creep-test ontology

`--classes "creep testing process" --involving` on `cto_small.ttl` (13,900 triples, 9,172 inferred subclass conclusions, 233 individuals): 160 candidates, 60 explained in 2 minutes, for example "Tl (the element thallium in a composition record) is a creep test piece chemical composition": 9 axioms (a definition with three restrictions, an inverse property, and six facts about the test piece and its process); the same pattern holds for 27 other elements. "A value specification is a percentage reduction of area after creep fracture value": 6 axioms.

## Limits (honest list)

- "Steps" is approximated by the **number of axioms** in the justification and the variety of their kinds. Konclude gives **one** minimal justification, not all of them.
- Each explanation costs many reasoner calls (seconds on a big ontology), so only a sample of the candidates is explained (`--max-explain`, chosen at random with a fixed seed, conclusions about your classes first). `--involving` examples are found among that sample; raise the number to find more.
- Conclusions are about classes and the types of individuals. Inferred property assertions are not used yet.
- The English text is only as good as your labels; long definitions are written out in full and can be hard to read.
- Detecting "this follows a plain chain" needs the asserted axioms, which are read with the built-in reader; if that fails the tool says so and recognises chains after the explanation.

## Tests

`python3 tests/examples/run_examples_tests.py PATH_TO_KONCLUDE` (12 checks): the expected four examples with 9, 5, 4 and 3 axioms; ranking; chains left out and brought back; the class filter by label, by name, with an unknown name, and with `--involving`; labels in the text; every justification is tight (on its own it entails the conclusion and no axiom can be dropped); an ontology with nothing interesting.
