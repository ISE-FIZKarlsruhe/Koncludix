#!/usr/bin/env python3
"""
konclude_diagnose.py - what exactly takes the time and the memory (or why does the reasoning not finish)?

  python3 tools/konclude_diagnose.py KONCLUDE ONTOLOGY [--task consistency|classification|realization|materialize]
                                     [--timeout 120] [--max-memory-mb 6000] [-w 1] [-c config.xml] [--json report.json]

It combines three things:
  1. `Konclude lint`        what is expensive or not handled, judging from the ontology alone (no reasoning)
  2. the run itself         started with the profiler on (environment KONCLUDE_PROFILE): time and memory per stage of the run (memory sampled twice a second),
                            stopped when the time or memory limit is reached
  3. the rule profile       which concept expressions the tableau spent its work on (rule applications, time inside the rule, new nodes, new branches), written by the
                            reasoner every few seconds, so a run that has to be stopped still leaves the table of its last seconds
and prints a diagnosis: the stage that grows, the concepts that cause it, the lint findings that explain them, and what to try.
"""
import argparse, json, os, re, shutil, signal, subprocess, sys, tempfile, threading, time

STAGE_MARKERS = [
    (r"Starting consistency|Starting classification|Starting realization|Starting materializ", "start"),
    (r"Loading ontology|Loaded \d+ axiom|loading", "loading"),
    (r"Preprocessing ontology", "preprocessing"),
    (r"Finished preprocessing", "preprocessing finished"),
    (r"Precomputing ontology", "precomputing (consistency, saturation)"),
    (r"Finished precomputing", "precomputing finished"),
    (r"Classifying|Starting classification of|class classification|Finished class classification|Finished classification", "classification"),
    (r"Realizing ontology|Finished realizing|Realization", "realization"),
    (r"Writing|written", "output"),
    (r"is (in)?consistent", "verdict"),
]

def vmrss_mb(pid):
    try:
        with open("/proc/%d/status" % pid) as f:
            for line in f:
                if line.startswith("VmRSS:"): return int(line.split()[1]) / 1024.0
    except Exception: pass
    return None

def pretty_concept(text):
    t = re.sub(r"^\S*¬?\d+ := ", "", text)
    t = re.sub(r"^¬\d+ := ", "", t)
    t = re.sub(r"\d+~", "", t)
    t = re.sub(r"https?://[^\s#>)]*[#/]", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t[:150]

def classify(row):
    t, neg, op, nodes, branches = row["full"], row["neg"], row["opcode"], row["nodes"], row["branches"]
    parts = t.count(" AND ") + 1
    if "ATLEAST" in t and nodes > 0:
        return "a minimum cardinality creates new nodes", "at_least"
    if "ATMOST" in t and nodes > 0 and branches == 0:
        return "a negated at-most restriction (a minimum cardinality) creates new nodes", "at_least"
    if "ATMOST" in t and branches > 0:
        return "an at-most restriction creates choices (which neighbours to merge)", "at_most"
    if branches > 0 and "{" in t and parts > 3 and "ATLEAST" not in t and "ATMOST" not in t:
        return "a choice among %d enumerated individuals (an enumerated class used as a filler or range)" % parts, "enumeration"
    if branches > 0 and parts > 3 and "ATLEAST" not in t and "ATMOST" not in t:
        return "a choice among %d alternatives (a wide disjunction, a covering axiom or a DisjointUnion)" % parts, "wide_or"
    if branches > 0:
        return "a disjunction (choice among %d alternatives)" % parts, "or"
    if nodes > 0:
        return "an existential restriction creates new nodes", "some"
    return "rule application", "other"

HINTS = {
    "at_least": "Konclude counts the neighbours that are already asserted and only creates the missing ones (option Konclude.Calculation.Optimization.AtLeastBackendNeighbourSatisfaction, on by default). Assert the missing neighbours in the data, or avoid the minimum cardinality on a class with many individuals.",
    "at_most": "At-most restrictions on many individuals lead to merge or choose decisions. Check whether the property is meant to be functional, and whether the data already respects the limit.",
    "enumeration": "A class made of an enumeration of k individuals, used as a filler or range, makes every individual that needs a successor choose among k alternatives. Give the members as instances of an ordinary class, or reduce the number of individuals that need the choice.",
    "wide_or": "A covering axiom or a DisjointUnion with many parts makes every instance choose one part. Drop the covering part if it need not be complete, or add the missing type facts to the data.",
    "or": "Disjunctions create choices; many individuals times many alternatives gives many decisions. Check which classes in the disjunction can be decided from the data.",
    "some": "Existential restrictions on many individuals create many new nodes. Check whether the successor already exists in the data.",
    "other": "",
}

def run(cmd, env=None, timeout=None):
    return subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=timeout)

def main():
    ap = argparse.ArgumentParser(description="what takes the time and the memory in a Konclude run")
    ap.add_argument("konclude"); ap.add_argument("ontology")
    ap.add_argument("--task", default="consistency", choices=["consistency", "classification", "realization", "materialize"])
    ap.add_argument("--timeout", type=float, default=120, help="seconds before the run is stopped (a timeout is a result)")
    ap.add_argument("--max-memory-mb", type=float, default=6000, help="the run is stopped when it uses more")
    ap.add_argument("-w", "--workers", default="1")
    ap.add_argument("-c", "--config", help="Konclude configuration file (OWLlink Set requests)")
    ap.add_argument("--interval", type=float, default=3, help="seconds between the profile tables written by the reasoner")
    ap.add_argument("--json", help="write the report as JSON")
    a = ap.parse_args()

    work = tempfile.mkdtemp(prefix="diagnose_")
    env = dict(os.environ, KONCLUDE_SUPERVISED="1", KONCLUDE_PROFILE="1", KONCLUDE_PROFILE_FILE=os.path.join(work, "profile.txt"), KONCLUDE_PROFILE_INTERVAL=str(a.interval))
    report = {"ontology": a.ontology, "task": a.task}

    # ---- 1. lint
    print("1. Lint (static, no reasoning) ...")
    lint_json = os.path.join(work, "lint.json")
    r = run([a.konclude, "lint", "-i", a.ontology, "-o", lint_json], env=env, timeout=600)
    findings = []
    if os.path.exists(lint_json):
        try: findings = json.load(open(lint_json)).get("findings", [])
        except Exception: pass
    report["lint"] = findings
    print("   %d finding(s): %s" % (len(findings), "; ".join("%s: %s" % (f.get("level"), f.get("title")) for f in findings[:6]) or "none"))

    # ---- 2. the run
    print("2. The run (%s, %s worker(s), limit %.0f s and %.0f MB) ..." % (a.task, a.workers, a.timeout, a.max_memory_mb))
    cmd = [a.konclude, a.task, "-w", str(a.workers)]
    if a.config: cmd += ["-c", a.config]
    cmd += ["-i", a.ontology]
    if a.task != "consistency": cmd += ["-o", os.path.join(work, "out.owx")]
    t0 = time.time()
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env, bufsize=1)
    stages, samples, verdict_line = [], [], []
    def reader():
        for line in proc.stdout:
            line = line.rstrip("\n")
            el = time.time() - t0
            for pat, name in STAGE_MARKERS:
                if re.search(pat, line):
                    stages.append((el, name, vmrss_mb(proc.pid), line.strip()[:140])); break
            if "is consistent" in line or "is inconsistent" in line: verdict_line.append(line.strip())
    th = threading.Thread(target=reader, daemon=True); th.start()
    outcome, peak = "finished", 0.0
    while proc.poll() is None:
        el = time.time() - t0
        m = vmrss_mb(proc.pid)
        if m is not None:
            samples.append((el, m)); peak = max(peak, m)
            if m > a.max_memory_mb: outcome = "stopped: memory limit of %.0f MB reached" % a.max_memory_mb; proc.kill(); break
        if el > a.timeout: outcome = "stopped: time limit of %.0f s reached" % a.timeout; proc.kill(); break
        time.sleep(0.4)
    proc.wait(); th.join(timeout=2)
    wall = time.time() - t0
    report.update(outcome=outcome, seconds=round(wall, 1), peak_mb=round(peak), stages=[{"at_s": round(s[0], 1), "stage": s[1], "rss_mb": s[2] and round(s[2])} for s in stages], verdict=verdict_line[-1] if verdict_line else None)
    print("   %s after %.1f s, peak memory %.0f MB%s" % (outcome, wall, peak, ("  [" + verdict_line[-1][-80:] + "]") if verdict_line else ""))

    # ---- 3. the profile
    rows, head = [], {}
    pf = os.path.join(work, "profile.txt")
    if os.path.exists(pf):
        lines = open(pf, encoding="utf-8", errors="replace").read().splitlines()
        if lines and lines[0].startswith("KONCLUDE-PROFILE"):
            head = dict(kv.split("=") for kv in lines[0].split()[2:] if "=" in kv)
            for ln in lines[2:]:
                c = ln.split("\t")
                if len(c) >= 9:
                    rows.append(dict(self_ms=int(c[1]), incl_ms=int(c[2]), calls=int(c[3]), nodes=int(c[4]), branches=int(c[5]), opcode=int(c[6]), neg=c[7] == "1", concept=pretty_concept(c[8]), full=c[8]))
    report["profile_header"] = head

    print("\n" + "=" * 100)
    print("DIAGNOSIS  %s  (%s)" % (os.path.basename(a.ontology), a.task))
    print("=" * 100)
    print("Result: %s; %.1f s; peak memory %.0f MB." % (outcome, wall, peak))

    # stages
    if stages:
        print("\nStages of the run (time since start, memory at that moment):")
        last = None
        shown = []
        for el, name, rss, line in stages:
            if name != last: shown.append((el, name, rss)); last = name
        for i, (el, name, rss) in enumerate(shown):
            grow = ""
            if i + 1 < len(shown) and rss is not None and shown[i + 1][2] is not None: grow = "   (+%d MB until the next stage)" % round(shown[i + 1][2] - rss) if shown[i + 1][2] - rss > 50 else ""
            print("  %7.1f s  %-42s %s%s" % (el, name, ("%5d MB" % round(rss)) if rss is not None else "", grow))
    if samples:
        # where did the memory grow most (per 10 s window)?
        wins = {}
        for el, m in samples: wins.setdefault(int(el // 10), []).append(m)
        keys = sorted(wins)
        growth = [(k, max(wins[k]) - (max(wins[keys[i - 1]]) if i else 0)) for i, k in enumerate(keys)]
        worst = max(growth, key=lambda g: g[1])
        print("  memory grows fastest between %d s and %d s (+%d MB)." % (worst[0] * 10, worst[0] * 10 + 10, worst[1]))

    # profile
    if rows:
        tot_work = sum(r["nodes"] + r["branches"] for r in rows) or 1
        print("\nWhat the tableau worked on (last table written by the reasoner at %s s; %s rule applications, %s new nodes, %s new branches in total):" % (head.get("elapsed_s"), head.get("rule_calls"), head.get("nodes"), head.get("branches")))
        print("  'share' = the part of all new nodes and branches (the work that has to be stored and processed) that this concept created; 'rule ms' is only the time inside the rule code.")
        print("  %5s %9s %10s %10s %9s  %s" % ("share", "nodes", "branches", "calls", "rule ms", "concept expression, and what it means"))
        seen = set()
        ranked = sorted(rows, key=lambda r: -(r["nodes"] + r["branches"]))
        for r in ranked[:8]:
            if r["nodes"] + r["branches"] == 0: continue
            meaning, kind = classify(r)
            share = 100.0 * (r["nodes"] + r["branches"]) / tot_work
            print("  %4.0f%% %9d %10d %10d %9d  %s" % (share, r["nodes"], r["branches"], r["calls"], r["self_ms"], r["concept"]))
            print("  %5s %9s %10s %10s %9s  -> %s" % ("", "", "", "", "", meaning))
            r["kind"] = kind; r["share"] = round(share, 1)
        print("\n  Slowest by time inside the rule code:")
        for r in sorted(rows, key=lambda r: -r["self_ms"])[:4]:
            print("    %7d ms  %8d calls  %s" % (r["self_ms"], r["calls"], r["concept"]))
        report["profile"] = ranked[:15]
        total_rule_ms = int(head.get("self_ms", 0) or 0)
        if wall > 0 and total_rule_ms:
            print("\n  Time inside tableau rule code: %.1f s of %.1f s run time (%.0f %%). The rest is parsing, preprocessing, saturation, task handling and memory copying for the created branches." % (total_rule_ms / 1000.0, wall, 100.0 * total_rule_ms / 1000.0 / wall))
    else:
        print("\nNo tableau rule applications were recorded: the time and memory went into work outside the tableau (loading, preprocessing, saturation or classification bookkeeping, realization). See the stages above.")

    # match lint findings with the profile
    names = set()
    for r in rows[:25]:
        names |= set(re.findall(r"[A-Za-z_][A-Za-z0-9_]{3,}", r["concept"]))
    explained = []
    for f in findings:
        text = " ".join([str(f.get("title", "")), str(f.get("detail", "")), " ".join(map(str, f.get("examples", [])))])
        fnames = set(re.findall(r"[A-Za-z_][A-Za-z0-9_]{3,}", text))
        hit = (fnames & names) - {"ATLEAST", "ATMOST", "SOME", "Individual", "individuals", "class", "Class", "http"}
        if len(hit) >= 2: explained.append((f, sorted(hit)[:5]))
    if findings:
        print("\nLint findings (static):")
        for f in findings:
            mark = "  <- also seen in the profile above" if any(f is e[0] for e in explained) else ""
            print("  [%s] %s%s" % (f.get("level"), f.get("title"), mark))
            if mark and f.get("suggestion"): print("        suggestion: %s" % f["suggestion"])
    # what to try
    kinds = []
    for r in report.get("profile", [])[:3]:
        k = r.get("kind")
        if k and k not in kinds and HINTS.get(k): kinds.append(k)
    if kinds:
        print("\nWhat to try (from the top rows):")
        for k in kinds: print("  - " + HINTS[k])
    if outcome != "finished":
        print("\nThe run did not finish. The table above is the last one the reasoner wrote, so it shows what the reasoner was busy with when the run was stopped.")
    if a.json:
        json.dump(report, open(a.json, "w"), indent=1, default=str)
        print("\nReport written to %s" % a.json)
    shutil.rmtree(work, ignore_errors=True)
    return 0

if __name__ == "__main__":
    sys.exit(main())
