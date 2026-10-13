#!/usr/bin/env python3
"""Minimal reproduction of the 'lost instances after Tell' problem of Konclude's OWLlink revisions.
usage: incr_min.py KONCLUDE [--variant NAME] [--base K1,K2,...] [--delta K1,...] [--workdir DIR]
A person with k asserted hobbies is written 'k'. Requests: CreateKB, Tell(TBox + base persons), GetFlattenedInstances(Person), Tell(delta persons), GetFlattenedInstances(Person).
Prints which persons are returned before and after the second Tell (a Tell can only add instances, so nothing may disappear).
Variants of the TBox:  mincard (Person SubClassOf >=3 hasHobby.Hobby, Hobby = oneOf 8)  |  oneof (only the enumeration)  |  mincard_plain (>=3 hasHobby.Thing)  |  none (no axioms)"""
import os, re, subprocess, sys, tempfile
args = sys.argv[1:]
K = args[0]
def opt(name, default):
    return args[args.index(name) + 1] if name in args else default
variant = opt("--variant", "mincard")
second = opt("--second", "persons")
config = opt("--config", "")             # an OWLlink Set request file (for example incr_cfg_on.xml: IncrementalRebuild on)      # persons | declaration (an unrelated axiom) | nothing (a second query on the same revision)
base = [int(x) for x in opt("--base", "0,1,2,3,4,0,1,2").split(",")]
delta = [int(x) for x in opt("--delta", "0,1").split(",")]
work = opt("--workdir", tempfile.mkdtemp(prefix="incr_min_"))
os.makedirs(work, exist_ok=True)
NS = "http://hobby#"; KB = "http://hobby/kb"
def cls(n): return '<owl:Class IRI="%s%s"/>' % (NS, n)
def ind(n): return '<owl:NamedIndividual IRI="%s%s"/>' % (NS, n)
def persons(first, counts):
    ax = []
    for i, k in enumerate(counts):
        p = "p%d" % (first + i)
        ax.append("<owl:ClassAssertion>%s%s</owl:ClassAssertion>" % (cls("Person"), ind(p)))
        for j in range(k):
            ax.append('<owl:ObjectPropertyAssertion><owl:ObjectProperty IRI="%shasHobby"/>%s%s</owl:ObjectPropertyAssertion>' % (NS, ind(p), ind("hh%d" % (j + 1))))
    return "\n".join(ax)
tbox = []
if variant in ("mincard", "oneof"):
    tbox.append('<owl:EquivalentClasses>%s<owl:ObjectOneOf>%s</owl:ObjectOneOf></owl:EquivalentClasses>' % (cls("Hobby"), "".join(ind("hh%d" % i) for i in range(1, 9))))
    tbox += ["<owl:ClassAssertion>%s%s</owl:ClassAssertion>" % (cls("Hobby"), ind("hh%d" % i)) for i in range(1, 9)]
else:
    tbox += ["<owl:ClassAssertion>%s%s</owl:ClassAssertion>" % (cls("Thing2"), ind("hh%d" % i)) for i in range(1, 9)]
if variant == "mincard":
    tbox.append('<owl:SubClassOf>%s<owl:ObjectMinCardinality cardinality="3"><owl:ObjectProperty IRI="%shasHobby"/>%s</owl:ObjectMinCardinality></owl:SubClassOf>' % (cls("Person"), NS, cls("Hobby")))
if variant == "mincard_plain":
    tbox.append('<owl:SubClassOf>%s<owl:ObjectMinCardinality cardinality="3"><owl:ObjectProperty IRI="%shasHobby"/><owl:Class IRI="http://www.w3.org/2002/07/owl#Thing"/></owl:ObjectMinCardinality></owl:SubClassOf>' % (cls("Person"), NS))
q = '<GetFlattenedInstances kb="%s">%s</GetFlattenedInstances>' % (KB, cls("Person"))
msg = ['<?xml version="1.0" encoding="UTF-8"?>', '<RequestMessage xmlns="http://www.owllink.org/owllink-xml#" xmlns:owl="http://www.w3.org/2002/07/owl#">',
       '<CreateKB kb="%s"/>' % KB, '<Tell kb="%s">\n%s\n%s\n</Tell>' % (KB, "\n".join(tbox), persons(0, base)), q,
       {"persons": '<Tell kb="%s">\n%s\n</Tell>' % (KB, persons(len(base), delta)),
        "declaration": '<Tell kb="%s">\n<owl:Declaration><owl:Class IRI="%sUnrelated"/></owl:Declaration>\n</Tell>' % (KB, NS), "nothing": ""}[second], q, "</RequestMessage>"]
req, resp = os.path.join(work, "req.xml"), os.path.join(work, "resp.xml")
open(req, "w", newline="\n").write("\n".join(msg) + "\n")
env = dict(os.environ, KONCLUDE_SUPERVISED="1")
cmd = [K, "owllinkfile", "-w", "1"] + (["-c", config] if config else []) + ["-i", req, "-o", resp]
r = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=300)
t = open(resp).read() if os.path.exists(resp) else ""
sets = [set(re.findall(r'IRI="[^"]*#(p\d+)"', s)) for s in re.findall(r'<SetOfIndividuals.*?</SetOfIndividuals>', t, re.S)]
name = lambda x: "p%d(k=%d)" % (x, (base + delta)[x])
print("variant %-9s second=%-11s base %s + delta %s: answers %s" % (variant, second, base, delta, [len(s) for s in sets]), end="")
if len(sets) >= 2:
    lost = sorted(sets[0] - sets[1], key=lambda x: int(x[1:]))
    print("   LOST after the Tell: %s" % [name(int(x[1:])) for x in lost] if lost else "   nothing lost")
else:
    print("   (no second answer; rc=%d)" % r.returncode)
