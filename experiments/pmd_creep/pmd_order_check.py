#!/usr/bin/env python3
"""Real PMD data (Creep Testing Ontology, 12 real creep tests, workshop files in PMDco-Reasoning-Workshop/part3-reasoners/data).
The records say, for six processes of every test, '<process> preceded by <creep testing process>' (BFO_0000062, 'x preceded by y' = y comes first).
Rule written in OWL (no time module): a manufacturing, heat treatment, phase transformation or supply chain process is never preceded by a creep testing process.
Builds three files, runs Konclude consistency and explain:  as published + rule,  edges turned around (BFO_0000063 'precedes') + rule,  as published without the rule.
usage (WSL): pmd_order_check.py KONCLUDE DATA_TTL OUTDIR"""
import os, re, subprocess, sys
K, DATA, OUT = sys.argv[1:4]
os.makedirs(OUT, exist_ok=True)
OBO = "http://purl.obolibrary.org/obo/"
B62, B63 = OBO + "BFO_0000062", OBO + "BFO_0000063"
TEST = "https://w3id.org/pmd/co/PMD_0000589"
PRE = {"manufacturing process": "https://w3id.org/pmd/co/PMD_0000833", "heat treatment": "https://w3id.org/pmd/co/PMD_0000779",
       "phase transformation": "https://w3id.org/pmd/co/PMD_0020136", "supply chain process": "https://w3id.org/pmd/log/1.0.2/LOG_10001247"}
PRE_ONLY = {"manufacturing_process", "heat_treatment", "phase_transformation", "supply_chain_process"}
rules = []
for i, (n, c) in enumerate(PRE.items()):
    rules += ["_:r%d <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Restriction> ." % i,
              "_:r%d <http://www.w3.org/2002/07/owl#onProperty> <%s> ." % (i, B62),
              "_:r%d <http://www.w3.org/2002/07/owl#allValuesFrom> _:n%d ." % (i, i),
              "_:n%d <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class> ." % i,
              "_:n%d <http://www.w3.org/2002/07/owl#complementOf> <%s> ." % (i, TEST),
              "<%s> <http://www.w3.org/2000/01/rdf-schema#subClassOf> _:r%d ." % (c, i)]
lines = open(DATA, encoding="utf-8").read().splitlines()
def swap(l):
    m = re.match(r"^<([^>]*/(%s)_Vh5205[^>]*)> <%s> <([^>]*)> \.$" % ("|".join(PRE_ONLY), re.escape(B62)), l)
    return "<%s> <%s> <%s> ." % (m.group(3), B62, m.group(1)) if False else ("<%s> <%s> <%s> ." % (m.group(1), B63, m.group(3)) if m else l)
files = {"as_published_with_rule": lines + rules, "edges_turned_around_with_rule": [swap(l) for l in lines] + rules, "as_published_no_rule": lines}
env = dict(os.environ, KONCLUDE_SUPERVISED="1")
for name, ls in files.items():
    p = os.path.join(OUT, name + ".ttl"); open(p, "w", encoding="utf-8", newline="\n").write("\n".join(ls) + "\n")
    r = subprocess.run([K, "consistency", "-w", "1", "-i", p], capture_output=True, text=True, env=env, timeout=900)
    verdict = "inconsistent" if "is inconsistent" in r.stdout + r.stderr else ("consistent" if "is consistent" in r.stdout + r.stderr else "?")
    print("%-34s changed edges: %-3d verdict: %s" % (name, sum(1 for a, b in zip(lines, ls) if a != b) if len(ls) >= len(lines) else 0, verdict))
    if name == "as_published_with_rule":
        e = subprocess.run([K, "explain", "-i", p], capture_output=True, text=True, env=env, timeout=1800)
        print("  explain:", "\n  ".join((e.stdout + e.stderr).splitlines()[-8:]))
