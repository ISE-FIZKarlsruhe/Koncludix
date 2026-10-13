#!/usr/bin/env python3
"""Writes a small ontology with the hobby pattern that is expensive for tableau reasoners (the same pattern as in OWL2Bench and in experiments/ab_at_least):
persons, an enumerated class Hobby of K individuals, and  Person SubClassOf (at least 3 hasHobby.Hobby).  Every person has between 0 and 4 asserted hobbies, so most
persons need new successors, and each new successor must choose among the K hobbies.   usage: make_hobby.py OUT.ofn PERSONS [K]"""
import sys
out, n = sys.argv[1], int(sys.argv[2]); k = int(sys.argv[3]) if len(sys.argv) > 3 else 8
L = ["Prefix(:=<http://hobby.example/o#>)", "Ontology(<http://hobby.example/o>",
     "Declaration(Class(:Person))", "Declaration(Class(:Hobby))", "Declaration(ObjectProperty(:hasHobby))"]
L += ["Declaration(NamedIndividual(:hobby%d))" % i for i in range(k)]
L.append("EquivalentClasses(:Hobby ObjectOneOf(%s))" % " ".join(":hobby%d" % i for i in range(k)))
L += ["ClassAssertion(:Hobby :hobby%d)" % i for i in range(k)]
L.append("SubClassOf(:Person ObjectMinCardinality(3 :hasHobby :Hobby))")
L.append("ObjectPropertyRange(:hasHobby :Hobby)")
for i in range(n):
    L.append("ClassAssertion(:Person :person%d)" % i)
    for j in range((0, 1, 2, 3, 4)[i % 5]):
        L.append("ObjectPropertyAssertion(:hasHobby :person%d :hobby%d)" % (i, (i + j) % k))
L.append(")")
open(out, "w").write("\n".join(L) + "\n")
