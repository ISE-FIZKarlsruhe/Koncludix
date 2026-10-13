#!/usr/bin/env python3
"""
tlayer.py - a temporal layer around Konclude (no change to the reasoner).

  Static OWL 2 DL ontology (TBox)  +  facts with validity intervals  +  temporal formulas
  -> change-point snapshots -> Konclude per snapshot -> temporal evaluation
  -> TRUE / FALSE / UNKNOWN / PENDING with witnesses and justifications.

Usage:
  tlayer.py --tbox tbox.ofn --facts facts.tl --formula "G(Employee(?x) -> F Manager(?x))" [options]

Facts file (one per line, '#' starts a comment):
  Employee(Alice)            [2025-01-01, 2025-08-31]
  Manager(Alice)             [2025-06-01, inf)
  worksFor(Alice, ACME)      [2025-01-01, 2025-08-31]
  Employee(Carol)                                       (no interval: always valid)
A closing ']' includes the end, ')' excludes it; an opening '(' excludes the start.
Times are dates (YYYY-MM-DD, one day = one step) or whole numbers (one tick = one step).

Formulas: atoms Class(ind), prop(ind, ind), variables ?x; ! & | -> ; future G F X U ; past H O Y S.

Live mode (--live): instead of one Konclude process per changed component, ONE running Konclude (owllinkserver) holds ONE knowledge base:
the TBox is loaded once, and at every change point only the facts that began are told and the facts that ended are retracted (OWLlink revisions,
Konclude.Ontology.Revision.IncrementalRebuild on). With a large terminology this saves the cost of preparing the terminology in every period.
The answers are the same as without --live; if anything goes wrong the layer says so and continues with one process per component.
"""
import argparse, atexit, datetime, hashlib, itertools, json, os, re, socket, subprocess, sys, tempfile, time, urllib.request
import xml.etree.ElementTree as ET
from xml.sax.saxutils import quoteattr
from functools import lru_cache

# ---------------------------------------------------------------- time

INF = None
class TimeAxis:
    """Dates or integers, both mapped to integers."""
    def __init__(self):
        self.mode = None
    def parse(self, s):
        s = s.strip()
        if s.lower() in ("inf", "+inf", "∞", "open", "none", ""):
            return INF
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
            mode, v = "date", datetime.date.fromisoformat(s).toordinal()
        elif re.fullmatch(r"-?\d+", s):
            mode, v = "int", int(s)
        else:
            raise ValueError("cannot read the time value '%s' (use YYYY-MM-DD or a whole number)" % s)
        if self.mode and self.mode != mode:
            raise ValueError("dates and plain numbers cannot be mixed ('%s')" % s)
        self.mode = mode
        return v
    def fmt(self, v):
        if v is None: return "∞"
        if self.mode == "date": return datetime.date.fromordinal(v).isoformat()
        return str(v)

# ---------------------------------------------------------------- facts

class Fact:
    __slots__ = ("name", "args", "start", "end", "line")
    def __init__(self, name, args, start, end, line):
        self.name, self.args, self.start, self.end, self.line = name, tuple(args), start, end, line
    def key(self):
        return "%s(%s)" % (self.name, ",".join(self.args))

FACT_RE = re.compile(r"^([^\s(]+)\(\s*([^)]*?)\s*\)\s*(?:([\[(])\s*([^,\]\)]*?)\s*,\s*([^\]\)]*?)\s*([\])]))?\s*$")

def parse_facts(text, axis):
    facts = []
    for n, line in enumerate(text.splitlines(), 1):
        line = line.split("#", 1)[0].strip()
        if not line: continue
        m = FACT_RE.match(line)
        if not m:
            raise ValueError("facts line %d cannot be read: %s" % (n, line))
        name, args = m.group(1), [a.strip() for a in m.group(2).split(",") if a.strip()]
        if len(args) not in (1, 2):
            raise ValueError("facts line %d: a fact has one (class) or two (property) arguments" % n)
        start, end = None, INF
        if m.group(3):
            start = axis.parse(m.group(4))
            end = axis.parse(m.group(5))
            if start is not None and m.group(3) == "(": start += 1
            if end is not None and m.group(6) == ")": end -= 1
            if start is not None and end is not None and end < start:
                raise ValueError("facts line %d: the interval is empty" % n)
        facts.append(Fact(name, args, start, end, n))
    if not facts: raise ValueError("no facts")
    for f in facts:
        if f.start is None:
            f.start = -10**9 if axis.mode != "date" else 1
    return facts

# ---------------------------------------------------------------- formulas

T = ("c", "T"); F = ("c", "F"); U = ("c", "U")
OPS = {"G", "F", "X", "H", "O", "Y", "U", "S", "not", "and", "or", "true", "false"}
TOKEN_RE = re.compile(r"\s*(<->|->|\?\w+|\d+|[A-Za-z_][\w:.#/\-]*|[()!&|,\[\]])")
METRIC = ("Fm", "Gm", "Om", "Hm", "Fabs", "Gabs")
BIG = 10**9   # 'inf' as the upper bound of a deadline operator

def tokenize(s):
    pos, out = 0, []
    s = s.strip()
    while pos < len(s):
        m = TOKEN_RE.match(s, pos)
        if not m: raise ValueError("cannot read the formula near '%s'" % s[pos:pos + 12])
        out.append(m.group(1)); pos = m.end()
        while pos < len(s) and s[pos].isspace(): pos += 1
    return out

class Parser:
    def __init__(self, text):
        self.toks, self.i = tokenize(text), 0
    def peek(self): return self.toks[self.i] if self.i < len(self.toks) else None
    def eat(self, t=None):
        x = self.peek()
        if x is None or (t and x != t): raise ValueError("formula: expected '%s' but found '%s'" % (t, x))
        self.i += 1; return x
    def parse(self):
        f = self.imp()
        if self.peek() is not None: raise ValueError("formula: unexpected '%s'" % self.peek())
        check_past(f)
        return f
    def imp(self):
        a = self.orr()
        if self.peek() == "->":
            self.eat(); return mk_or(mk_not(a), self.imp())
        if self.peek() == "<->":
            self.eat(); b = self.imp()
            return mk_and(mk_or(mk_not(a), b), mk_or(mk_not(b), a))
        return a
    def orr(self):
        a = self.andd()
        while self.peek() in ("|", "or"):
            self.eat(); a = mk_or(a, self.andd())
        return a
    def andd(self):
        a = self.until()
        while self.peek() in ("&", "and"):
            self.eat(); a = mk_and(a, self.until())
        return a
    def until(self):
        a = self.unary()
        if self.peek() in ("U", "S"):
            op = self.eat(); b = self.until()
            return (op, a, b)
        return a
    def unary(self):
        t = self.peek()
        if t in ("!", "not"): self.eat(); return mk_not(self.unary())
        if t in ("G", "F", "X", "H", "O", "Y"):
            self.eat()
            if t in ("G", "F", "H", "O") and self.peek() == "[":
                self.eat("["); a = self.eat(); self.eat(","); b = self.eat(); self.eat("]")
                if b == "inf": b = str(BIG)
                if not (a.isdigit() and b.isdigit()) or int(a) > int(b): raise ValueError("bounds [a,b] need whole numbers with a <= b (in days or ticks); b may be inf")
                child = self.unary()
                if has_future(child): raise ValueError("an operator with bounds (F[a,b], G[a,b], O[a,b], H[a,b]) cannot contain a future operator inside")
                return (t + "m", child, int(a), int(b))
            return (t, self.unary())
        if t == "(":
            self.eat(); f = self.imp(); self.eat(")"); return f
        if t == "true": self.eat(); return T
        if t == "false": self.eat(); return F
        if t is None: raise ValueError("formula ends too early")
        if t in OPS or not re.match(r"[A-Za-z_]", t): raise ValueError("formula: unexpected '%s'" % t)
        name = self.eat()
        self.eat("(")
        args = []
        while self.peek() != ")":
            args.append(self.eat())
            if self.peek() == ",": self.eat()
        self.eat(")")
        if len(args) not in (1, 2): raise ValueError("atom %s needs one or two arguments" % name)
        return ("atom", name, tuple(args))

def mk_not(a):
    if a[0] == "c": return {"T": F, "F": T, "U": U}[a[1]]
    if a[0] == "not": return a[1]
    return ("not", a)

def _nary(op, items):
    absorb, ident = (F, T) if op == "and" else (T, F)
    flat = []
    for it in items:
        if it[0] == op: flat.extend(it[1])
        else: flat.append(it)
    s = set()
    for it in flat:
        if it == absorb: return absorb
        if it == ident: continue
        s.add(it)
    if not s: return ident
    # absorption (valid in three-valued logic): x | (x & y) = x   and   x & (x | y) = x
    other = "or" if op == "and" else "and"
    s = {it for it in s if not (it[0] == other and any(x in s for x in it[1]))}
    if len(s) == 1: return next(iter(s))
    return (op, tuple(sorted(s, key=repr)))

def mk_and(a, b): return _nary("and", [a, b])
def mk_or(a, b): return _nary("or", [a, b])

@lru_cache(maxsize=None)
def has_future(f):
    k = f[0]
    if k in ("c", "atom"): return False
    if k in ("X", "F", "G", "U", "Nx", "Fm", "Gm", "Fabs", "Gabs"): return True
    if k in ("not", "H", "O", "Y", "Hm", "Om"): return has_future(f[1])
    if k in ("and", "or"): return any(has_future(x) for x in f[1])
    if k in ("S",): return has_future(f[1]) or has_future(f[2])
    raise ValueError(k)

def check_past(f):
    k = f[0]
    if k in ("H", "O", "Y", "Hm", "Om"):
        if has_future(f[1]): raise ValueError("a past operator (H, O, Y, S) cannot contain a future operator (G, F, X, U)")
        check_past(f[1])
    elif k == "S":
        if has_future(f[1]) or has_future(f[2]): raise ValueError("a past operator (H, O, Y, S) cannot contain a future operator (G, F, X, U)")
    elif k in ("not", "G", "F", "X", "Fm", "Gm"): check_past(f[1])
    elif k == "U": check_past(f[1]); check_past(f[2])
    elif k in ("and", "or"):
        for x in f[1]: check_past(x)

def variables(f, acc=None):
    acc = [] if acc is None else acc
    k = f[0]
    if k == "atom":
        for a in f[2]:
            if a.startswith("?") and a not in acc: acc.append(a)
    elif k in ("and", "or"):
        for x in f[1]: variables(x, acc)
    elif k in ("U", "S"): variables(f[1], acc); variables(f[2], acc)
    elif k != "c": variables(f[1], acc)
    return acc

def atoms_of(f, acc=None):
    acc = set() if acc is None else acc
    k = f[0]
    if k == "atom": acc.add(f)
    elif k in ("and", "or"):
        for x in f[1]: atoms_of(x, acc)
    elif k in ("U", "S"): atoms_of(f[1], acc); atoms_of(f[2], acc)
    elif k != "c": atoms_of(f[1], acc)
    return acc

def subst(f, b):
    k = f[0]
    if k == "atom": return ("atom", f[1], tuple(b.get(a, a) for a in f[2]))
    if k == "c": return f
    if k in ("and", "or"): return _nary(k, [subst(x, b) for x in f[1]])
    if k in ("U", "S"): return (k, subst(f[1], b), subst(f[2], b))
    if k == "not": return mk_not(subst(f[1], b))
    if k in METRIC: return (k, subst(f[1], b)) + tuple(f[2:])
    return (k, subst(f[1], b))

def show(f):
    k = f[0]
    if k == "c": return {"T": "true", "F": "false", "U": "unknown"}[f[1]]
    if k == "atom": return "%s(%s)" % (f[1], ", ".join(f[2]))
    if k == "not": return "!" + show(f[1])
    if k in ("and", "or"): return "(" + (" & " if k == "and" else " | ").join(show(x) for x in f[1]) + ")"
    if k in ("U", "S"): return "(%s %s %s)" % (show(f[1]), k, show(f[2]))
    if k in ("Fm", "Gm", "Om", "Hm"): return "%s[%d,%s] %s" % (k[0], f[2], "inf" if f[3] >= BIG else f[3], show(f[1]))
    if k in ("Fabs", "Gabs"): return "%s[day %d..%d] %s" % (k[0], f[2], f[3], show(f[1]))
    return "%s %s" % ("X" if k == "Nx" else k, show(f[1]))

def uses_metric(f):
    k = f[0]
    if k in ("Fm", "Gm", "Om", "Hm"): return True
    if k in ("c", "atom"): return False
    if k in ("and", "or"): return any(uses_metric(x) for x in f[1])
    if k in ("U", "S"): return uses_metric(f[1]) or uses_metric(f[2])
    return uses_metric(f[1])

# ---------------------------------------------------------------- temporal evaluation

class Monitor:
    """Formula progression over the state sequence (three-valued: T, F, U) and direct evaluation of past formulas."""
    def __init__(self, atomval):
        self.atomval = atomval      # (name, args, i) -> T/F/U constant
        self.past = {}
    def evalpast(self, f, i):
        key = (f, i)
        if key in self.past: return self.past[key]
        k = f[0]
        if k == "c": r = f
        elif k == "atom": r = self.atomval(f[1], f[2], i)
        elif k == "not": r = mk_not(self.evalpast(f[1], i))
        elif k in ("and", "or"): r = _nary(k, [self.evalpast(x, i) for x in f[1]])
        elif k == "Y": r = F if i == 0 else self.evalpast(f[1], i - 1)
        elif k == "O":
            r = self.evalpast(f[1], i)
            if i > 0: r = mk_or(r, self.evalpast(f, i - 1))
        elif k == "H":
            r = self.evalpast(f[1], i)
            if i > 0: r = mk_and(r, self.evalpast(f, i - 1))
        elif k == "Om" and f[3] >= BIG:
            r = self.evalpast(f[1], i - f[2]) if i - f[2] >= 0 else F
            if i > 0: r = mk_or(r, self.evalpast(f, i - 1))
        elif k == "Hm" and f[3] >= BIG:
            r = self.evalpast(f[1], i - f[2]) if i - f[2] >= 0 else T
            if i > 0: r = mk_and(r, self.evalpast(f, i - 1))
        elif k == "Om":
            r = _nary("or", [self.evalpast(f[1], j) for j in range(max(0, i - f[3]), i - f[2] + 1)]) if i - f[2] >= 0 else F
        elif k == "Hm":
            lo, hi = max(0, i - f[3]), i - f[2]
            r = _nary("and", [self.evalpast(f[1], j) for j in range(lo, hi + 1)]) if hi >= 0 and hi >= lo else T
        elif k == "S":
            r = self.evalpast(f[2], i)
            if i > 0: r = mk_or(r, mk_and(self.evalpast(f[1], i), self.evalpast(f, i - 1)))
        else: raise ValueError("cannot evaluate %s in the past" % k)
        self.past[key] = r
        return r
    def prog(self, f, i):
        k = f[0]
        if k == "c": return f
        if not has_future(f): return self.evalpast(f, i)
        if k == "not": return mk_not(self.prog(f[1], i))
        if k in ("and", "or"): return _nary(k, [self.prog(x, i) for x in f[1]])
        if k == "X": return ("Nx", f[1])
        if k == "Nx": return self.prog(f[1], i)
        if k == "F": return mk_or(self.prog(f[1], i), f)
        if k == "G": return mk_and(self.prog(f[1], i), f)
        if k == "U": return mk_or(self.prog(f[2], i), mk_and(self.prog(f[1], i), f))
        if k in ("Fm", "Gm"): return self.prog((k[0] + "abs", f[1], i + f[2], i + f[3]), i)
        if k in ("Fabs", "Gabs"):
            lo, hi = f[2], f[3]
            if i < lo: return f
            val = self.evalpast(f[1], i)
            if k == "Fabs":
                if i >= hi: return val
                return mk_or(val, f)
            if i >= hi: return val
            return mk_and(val, f)
        raise ValueError(k)
    def run(self, f, start, n):
        """Progress f through states start..n-1. Returns (residual, index where the value became fixed or None)."""
        R, decided = f, None
        for i in range(start, n):
            R = self.prog(R, i)
            if R[0] == "c":
                decided = i
                break
        return R, decided

def finish(R):
    """The history ends here (closed trace, LTLf): obligations about future states can no longer be met."""
    k = R[0]
    if k == "c": return R
    if k == "not": return mk_not(finish(R[1]))
    if k in ("and", "or"): return _nary(k, [finish(x) for x in R[1]])
    if k in ("G", "Gabs"): return T
    if k in ("F", "U", "Nx", "X", "Fabs"): return F
    raise ValueError(k)

def status(R):
    if R[0] == "c": return {"T": "TRUE", "F": "FALSE", "U": "UNKNOWN"}[R[1]]
    return "PENDING"

def minimal_support(g, atomval, first, n, closed=False, step_state=None):
    """Why is a ground formula TRUE or FALSE?  The smallest set of cells (atom, state) whose values decide the answer: if every other value is replaced
    by UNKNOWN the answer stays the same, and no single cell can be left out (found by deleting chunks of cells, then single cells; sound because
    less information can never turn an undecided answer into a decided one).
    atomval(name, args, state) gives T or F (or U); step_state maps a step of the evaluation (a day) to a state, None = identity.
    Returns (answer, cells) with cells = sorted list of (name, args, state), or (answer, None) if the answer is not TRUE or FALSE."""
    ns = (lambda i: i) if step_state is None else (lambda i: step_state[i])
    def run(removed, nsteps):
        def av(name, args, i):
            st = ns(i)
            return U if (name, args, st) in removed else atomval(name, args, st)
        R, decided = Monitor(av).run(g, first, nsteps)
        return status(finish(R) if closed else R), R, decided
    answer, R0, decided0 = run(frozenset(), n)
    if answer not in ("TRUE", "FALSE"): return answer, None
    nsteps = n if (decided0 is None or closed) else decided0 + 1      # nothing after the day on which the answer was fixed matters
    atoms = sorted(atoms_of(g), key=repr)
    states = sorted({ns(i) for i in range(first, nsteps)})
    cells = [(a[1], a[2], st) for a in atoms for st in states if atomval(a[1], a[2], st) != U]
    everything = frozenset(cells)
    def holds(keep):
        st, R, _ = run(everything - frozenset(keep), nsteps)
        return st == answer and (closed or R[0] == "c")
    keep = list(cells)
    chunk = max(1, len(keep) // 2)
    while chunk >= 1 and keep:
        i = 0
        while i < len(keep):
            trial = keep[:i] + keep[i + chunk:]
            if holds(trial): keep = trial
            else: i += chunk
        if chunk == 1: break
        chunk //= 2
    return answer, keep

# ---------------------------------------------------------------- Konclude interface

class Konclude:
    def __init__(self, binary, workdir=None, timeout=600):
        self.bin, self.timeout = binary, timeout
        self.dir = workdir or tempfile.mkdtemp(prefix="tlayer_")
        os.makedirs(self.dir, exist_ok=True)
        self.calls = {"consistency": 0, "materialize": 0, "explain": 0}
        self.seconds = 0.0
        self.cfg = os.path.join(self.dir, "mat.xml")
        with open(self.cfg, "w") as f:
            f.write('<?xml version="1.0" encoding="UTF-8"?>\n<RequestMessage xmlns="http://www.owllink.org/owllink-xml#">\n'
                    '<Set key="Konclude.CLI.Materialize.ClassHierarchy"><Literal>false</Literal></Set>\n'
                    '<Set key="Konclude.CLI.Materialize.PropertyHierarchy"><Literal>false</Literal></Set>\n'
                    '<Set key="Konclude.CLI.Materialize.DataPropertyAssertions"><Literal>false</Literal></Set>\n'
                    '</RequestMessage>\n')
        self.n = 0
    def _run(self, args):
        env = dict(os.environ, KONCLUDE_SUPERVISED="1")
        t0 = time.time()
        r = subprocess.run([self.bin] + args, capture_output=True, text=True, env=env, timeout=self.timeout)
        self.seconds += time.time() - t0
        return r
    def _write(self, text):
        self.n += 1
        p = os.path.join(self.dir, "s%d.ofn" % self.n)
        with open(p, "w", encoding="utf-8") as f: f.write(text)
        return p
    def consistent(self, text):
        self.calls["consistency"] += 1
        r = self._run(["consistency", "-w", "1", "-i", self._write(text)])
        out = r.stdout + r.stderr
        if "is inconsistent" in out: return False
        if "is consistent" in out: return True
        raise RuntimeError("Konclude gave no verdict:\n" + out[-800:])
    def materialize(self, text):
        self.calls["materialize"] += 1
        inp = self._write(text); outp = inp + ".out.xml"
        r = self._run(["materialize", "-w", "1", "-c", self.cfg, "-i", inp, "-o", outp])
        if not os.path.exists(outp): raise RuntimeError("Konclude wrote no output:\n" + (r.stdout + r.stderr)[-800:])
        return parse_materialized(outp)
    def explain(self, text):
        self.calls["explain"] += 1
        r = self._run(["explain", "-i", self._write(text)])
        lines, take = [], False
        for ln in (r.stdout + r.stderr).splitlines():
            if "is inconsistent because of these" in ln: take = True; continue
            if take and ln.startswith("  "): lines.append(ln.strip())
            elif take and ln.strip() == "": break
        return lines

class LiveKB:
    """One running Konclude (owllinkserver) with one knowledge base: the TBox is loaded once, facts are told and retracted afterwards."""
    OWL_NS = "http://www.w3.org/2002/07/owl#"
    def __init__(self, binary, ontology_text, timeout=600):
        self.timeout = timeout
        self.dir = tempfile.mkdtemp(prefix="tlayer_live_")
        self.kb = "http://tlayer/live"
        self.calls = {"requests": 0, "told": 0, "retracted": 0, "restarts": 0}
        self.seconds = 0.0
        self.epoch = 0
        onto = self.onto = os.path.join(self.dir, "tbox.ofn")
        with open(onto, "w", encoding="utf-8") as f: f.write(ontology_text)
        cfg = os.path.join(self.dir, "live.xml")
        with open(cfg, "w") as f:
            f.write('<?xml version="1.0" encoding="UTF-8"?>\n<RequestMessage xmlns="http://www.owllink.org/owllink-xml#">\n'
                    '<Set key="Konclude.Ontology.Revision.IncrementalRebuild"><Literal>true</Literal></Set>\n</RequestMessage>\n')
        with socket.socket() as sk:
            sk.bind(("127.0.0.1", 0)); self.port = sk.getsockname()[1]
        self.proc = subprocess.Popen([binary, "owllinkserver", "-p", str(self.port), "-w", "1", "-c", cfg],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=dict(os.environ, KONCLUDE_SUPERVISED="1"))
        atexit.register(self.close)
        for _ in range(600):
            if self.proc.poll() is not None: raise RuntimeError("the Konclude server did not start")
            try:
                self.ask('<CreateKB kb="%s"/>' % self.kb, raw=True); break
            except OSError:
                time.sleep(0.1)
        else:
            raise RuntimeError("the Konclude server did not answer")
        self.ask('<LoadOntologies kb="%s"><OntologyIRI IRI=%s/></LoadOntologies>' % (self.kb, quoteattr(onto)))
    def restart_kb(self):
        """A new, empty knowledge base with the same TBox (the old one is released). Used when a fact has to be told again after it was retracted: Konclude's update mechanism
        can lose such a fact (see docs/incremental-revisions.md), so the layer starts again from the TBox instead of relying on it."""
        old = self.kb
        self.epoch += 1
        self.kb = "http://tlayer/live/%d" % self.epoch
        self.ask('<CreateKB kb="%s"/>' % self.kb)
        self.ask('<LoadOntologies kb="%s"><OntologyIRI IRI=%s/></LoadOntologies>' % (self.kb, quoteattr(self.onto)))
        try: self.ask('<ReleaseKB kb="%s"/>' % old)
        except Exception: pass
        self.calls["restarts"] += 1
    def ask(self, body, raw=False):
        """Send one OWLlink request message; returns the answers (OK elements left out); any Error answer raises."""
        data = ('<?xml version="1.0" encoding="UTF-8"?>\n<RequestMessage xmlns="http://www.owllink.org/owllink-xml#" xmlns:owl="%s">\n%s\n</RequestMessage>' % (self.OWL_NS, body)).encode("utf-8")
        t0 = time.time(); self.calls["requests"] += 1
        if os.environ.get("TLAYER_LIVE_LOG"):          # for debugging: every request, so that a sequence can be replayed against another configuration
            with open(os.environ["TLAYER_LIVE_LOG"], "a", encoding="utf-8") as lf: lf.write(json.dumps({"ontology": os.path.join(self.dir, "tbox.ofn"), "body": body}) + "\n")
        req = urllib.request.Request("http://127.0.0.1:%d/" % self.port, data=data, headers={"Content-Type": "application/xml"})
        resp = urllib.request.urlopen(req, timeout=self.timeout).read()
        self.seconds += time.time() - t0
        if os.environ.get("TLAYER_LIVE_TRACE"): sys.stderr.write("live request: %s ... %.2f s\n" % (re.sub(r"\s+", " ", body)[:70], time.time() - t0))
        out = []
        for c in ET.fromstring(resp):
            tag = c.tag.split("}")[-1]
            if tag == "Error": raise RuntimeError("Konclude: " + (c.get("error") or "")[:300].replace("\n", " "))
            if tag not in ("OK", "KB"): out.append(c)
        return out
    def close(self):
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            try: self.proc.wait(timeout=10)
            except Exception: self.proc.kill()

OWL = "{http://www.w3.org/2002/07/owl#}"
def parse_materialized(path):
    types, props = {}, set()
    for _, el in ET.iterparse(path):
        tag = el.tag
        if tag == OWL + "ClassAssertion":
            c, i = el.find(OWL + "Class"), el.find(OWL + "NamedIndividual")
            if c is not None and i is not None:
                types.setdefault(i.get("IRI"), set()).add(c.get("IRI"))
            el.clear()
        elif tag == OWL + "ObjectPropertyAssertion":
            p, ind = el.find(OWL + "ObjectProperty"), el.findall(OWL + "NamedIndividual")
            if p is not None and len(ind) == 2: props.add((p.get("IRI"), ind[0].get("IRI"), ind[1].get("IRI")))
            el.clear()
    return types, props

# ---------------------------------------------------------------- the layer

GLOBAL_RE = re.compile(r"ObjectOneOf|ObjectHasValue|TopObjectProperty|topObjectProperty|HasKey|SameIndividual|DifferentIndividuals|"
                       r"(?<![A-Za-z])ClassAssertion|(?<![A-Za-z])ObjectPropertyAssertion|DataPropertyAssertion|NegativeObjectPropertyAssertion")

class Layer:
    def __init__(self, tbox_text, facts, konclude, ns=None, use_cache=True, axis=None, whole_snapshots=False):
        self.axis, self.facts, self.k = axis, facts, konclude
        idx = tbox_text.rstrip().rfind(")")
        if idx < 0 or "Ontology(" not in tbox_text: raise ValueError("the TBox must be in OWL functional syntax: Ontology( ... )")
        self.head, self.tail = tbox_text[:idx], tbox_text[idx:]
        m = re.search(r"Prefix\(\s*:\s*=\s*<([^>]*)>\s*\)", tbox_text)
        self.ns = ns or (m.group(1) if m else "http://example.org/t#")
        body = re.sub(r"#[^\n]*", "", tbox_text)
        g = GLOBAL_RE.search(body)
        self.global_reason = g.group(0) if g else None
        self.per_component = not g and not whole_snapshots
        self.cache_enabled = use_cache
        self.cache = {}
        self.stats = {"states": 0, "components": 0, "cache_hits": 0, "batches": 0}
        self.justcache = {}
        self.validation = False
        self._iri = {}
        self._av = {}
        self.live = None                           # a LiveKB when --live is on
        self.live_requested = False
        self.live_timeout = 300                    # seconds one live request may take; after that live mode is given up and the layer continues with one process per group
        self.live_current = {}                     # axiom (functional syntax) -> fact, for what the live knowledge base holds now
        self.live_declared = set()
        self.live_ever_retracted = set()           # axioms retracted from the current live knowledge base at some time
        self.live_note = None
    # names
    def iri(self, name):
        r = self._iri.get(name)
        if r is None:
            if name.startswith("<") and name.endswith(">"): r = name[1:-1]
            elif "://" in name: r = name
            else: r = self.ns + name
            self._iri[name] = r
        return r
    def negiri(self, name):
        return self.iri("TLneg_" + re.sub(r"\W", "_", name))
    # trace
    def build_trace(self, now=None):
        self.now = now
        pts = set()
        for f in self.facts:
            pts.add(f.start)
            if f.end is not None: pts.add(f.end + 1)
        pts = sorted(pts)
        if now is not None:
            pts = [p for p in pts if p <= now]
            if not pts: raise ValueError("--now is before the first change")
        self.points = pts
        self.windows = [(pts[i], (pts[i + 1] - 1) if i + 1 < len(pts) else (now if now is not None else INF)) for i in range(len(pts))]
        self.state_facts = []
        for (s, e) in self.windows:
            self.state_facts.append([f for f in self.facts if f.start <= s and (f.end is None or f.end >= s)])
        self.individuals = sorted({a for f in self.facts for a in f.args})
    # ontology text
    def axiom(self, f):
        if len(f.args) == 1: return "ClassAssertion(<%s> <%s>)" % (self.iri(f.name), self.iri(f.args[0]))
        return "ObjectPropertyAssertion(<%s> <%s> <%s>)" % (self.iri(f.name), self.iri(f.args[0]), self.iri(f.args[1]))
    def ontology(self, comps, extra=(), helpers=True):
        """helpers=False leaves out the classes TLneg_C that stand for 'not C' (the live mode in validation mode: a negative type changes no answer there, and a class defined as a
        complement makes Konclude classify the whole terminology again after every update)"""
        ax = []
        decl = set()
        for c in comps:
            for f in c["facts"]:
                decl.add(("Class" if len(f.args) == 1 else "ObjectProperty", self.iri(f.name)))
                ax.append(self.axiom(f))
        out = [self.head]
        for kind, i in sorted(decl): out.append("Declaration(%s(<%s>))" % (kind, i))
        for name in sorted(self.class_atoms):
            out.append("Declaration(Class(<%s>))" % self.iri(name))
            if helpers:
                out.append("Declaration(Class(<%s>))" % self.negiri(name))
                out.append("EquivalentClasses(<%s> ObjectComplementOf(<%s>))" % (self.negiri(name), self.iri(name)))
        out.extend(ax); out.extend(extra); out.append(self.tail)
        return "\n".join(out)
    # components
    def components(self, facts):
        if not facts: return []
        parent = {}
        def find(x):
            while parent.setdefault(x, x) != x:
                parent[x] = parent[parent[x]]; x = parent[x]
            return x
        for f in facts:
            find(f.args[0])
            if len(f.args) == 2: parent[find(f.args[0])] = find(f.args[1])
        groups = {}
        for f in facts: groups.setdefault(find(f.args[0]), []).append(f)
        if not self.per_component:
            groups = {0: list(facts)}
        comps = []
        for fs in groups.values():
            fs = sorted(fs, key=lambda f: (f.key()))
            key = "|".join(f.key() for f in fs)
            inds = {a for f in fs for a in f.args}
            comps.append({"key": key, "facts": fs, "inds": inds})
        return comps
    def analyse(self, formulas, definitions=()):
        names = {a[1] for f in formulas for a in atoms_of(f) if len(a[2]) == 1}
        for (dname, var, body, text) in definitions:
            names.add(dname)
            names |= {a[1] for a in atoms_of(body) if len(a[2]) == 1}
        self.class_atoms = sorted(names)
        props = {a[1] for f in formulas for a in atoms_of(f) if len(a[2]) == 2}
        for (dname, var, body, text) in definitions: props |= {a[1] for a in atoms_of(body) if len(a[2]) == 2}
        self.prop_atoms = sorted(props)
        if self.live_requested and self.live is None and self.live_note is None: self.start_live()
        self.base_facts = list(self.facts)
        self.derived, self.derived_axioms = [], {}
        self.reason_states()                      # stage 1: the facts as given
        if definitions:
            self.apply_definitions(definitions)   # temporal definitions look at the past of stage 1 and add facts
            self._av = {}
            self.reason_states()                  # stage 2: the facts as given plus the derived ones
    def reason_states(self):
        self.types, self.props, self.bad, self.present, self.state_comps = [], [], [], [], []
        for i, facts in enumerate(self.state_facts):
            comps = self.components(facts)
            self.state_comps.append(comps)
            self.stats["states"] += 1; self.stats["components"] += len(comps)
            todo = []
            for c in comps:
                if self.cache_enabled and c["key"] in self.cache: self.stats["cache_hits"] += 1
                else: todo.append(c)
            if todo:
                if self.live is not None:
                    try: self.run_batch_live(todo, comps)
                    except Exception as e:
                        self.live_note = "live mode stopped (%s); continuing with one Konclude process per component" % e
                        sys.stderr.write("WARNING: " + self.live_note + "\n")
                        self.close_live(); self.run_batch(todo)
                else: self.run_batch(todo)
            types, props, bad, present = {}, set(), set(), set()
            for c in comps:
                e = self.cache[c["key"]]
                present |= c["inds"]
                if not e["ok"]: bad |= c["inds"]; continue
                for ind, ts in e["types"].items(): types.setdefault(ind, set()).update(ts)
                props |= e["props"]
            self.types.append(types); self.props.append(props); self.bad.append(bad); self.present.append(present)
    def apply_definitions(self, defs):
        """A definition  Name(?x) := <past formula>  gives every individual the class Name in exactly those periods in which the formula is TRUE."""
        import bisect
        new_facts = []
        for (name, var, body, text) in defs:
            metric = uses_metric(body)
            if metric:
                if self.now is None: raise ValueError("a definition with bounds (O[a,b], H[a,b]) needs --now: the day on which the observation ends")
                t0 = self.windows[0][0]; n = self.now - t0 + 1
                starts = [w[0] for w in self.windows]
                state_of = [max(0, bisect.bisect_right(starts, t0 + d) - 1) for d in range(n)]
                span = lambda i, j, t0=t0: (t0 + i, t0 + j)
            else:
                n = len(self.windows); state_of = None
                span = lambda i, j: (self.windows[i][0], self.windows[j][1])
            for ind in self.individuals:
                g = subst(body, {var: ind})
                mon = Monitor(lambda nm, ar, i, so=state_of: self.atomval(nm, ar, so[i])) if state_of is not None else Monitor(self.atomval)
                run = None
                runs = []
                for i in range(n):
                    if mon.evalpast(g, i) == T:
                        if run is None: run = [i, i]
                        else: run[1] = i
                    elif run is not None:
                        runs.append(run); run = None
                if run is not None: runs.append(run)
                for r0, r1 in runs:
                    st, en = span(r0, r1)
                    f = Fact(name, [ind], st, en, 0)
                    new_facts.append(f)
                    self.derived_axioms[self.axiom(f)] = name
                    self.derived.append({"definition": name, "individual": ind, "from": self.axis.fmt(st), "to": self.axis.fmt(en) if en is not None else self.axis.fmt(None)})
        self.facts = self.base_facts + new_facts
        self.build_trace(self.now)
    # ---- live mode: one knowledge base, only the changes per period
    def start_live(self):
        extra = ["Declaration(ObjectProperty(<%s>))" % self.iri(p) for p in self.prop_atoms]
        try:
            self.live = LiveKB(self.k.bin, self.ontology([], extra=extra, helpers=False), timeout=self.live_timeout)
        except Exception as e:
            self.live = None
            self.live_note = "live mode could not start (%s); using one Konclude process per component" % e
            sys.stderr.write("WARNING: " + self.live_note + "\n")
    def close_live(self):
        if self.live is not None:
            self.live.close(); self.live_stats = dict(self.live.calls, seconds=round(self.live.seconds, 2)); self.live = None
    def axiom_xml(self, f):
        if len(f.args) == 1:
            return ('<owl:ClassAssertion><owl:Class IRI=%s/><owl:NamedIndividual IRI=%s/></owl:ClassAssertion>' % (quoteattr(self.iri(f.name)), quoteattr(self.iri(f.args[0]))),
                    [("Class", self.iri(f.name))])
        return ('<owl:ObjectPropertyAssertion><owl:ObjectProperty IRI=%s/><owl:NamedIndividual IRI=%s/><owl:NamedIndividual IRI=%s/></owl:ObjectPropertyAssertion>'
                % (quoteattr(self.iri(f.name)), quoteattr(self.iri(f.args[0])), quoteattr(self.iri(f.args[1]))), [("ObjectProperty", self.iri(f.name))])
    def live_set(self, wanted):
        """Make the knowledge base hold exactly these facts (axiom -> fact): retract what ended, tell what began. Returns whether it is consistent."""
        lv, cur = self.live, self.live_current
        if any(a in self.live_ever_retracted for a in wanted if a not in cur):      # a fact comes back after it was retracted: start a fresh knowledge base
            lv.restart_kb(); cur.clear(); self.live_declared.clear(); self.live_ever_retracted.clear()
        gone = [a for a in cur if a not in wanted]
        new = [a for a in wanted if a not in cur]
        sat = True
        # Retract and Tell go in separate requests, each ending with a consistency test: with IncrementalRebuild on, a Tell that follows a Retract
        # before any question was asked wrongly sees the retracted facts again (a Konclude bug, see docs/incremental-revisions.md).
        if gone:
            res = lv.ask('<Retract kb="%s">\n%s\n</Retract>\n<IsKBSatisfiable kb="%s"/>' % (lv.kb, "\n".join(self.axiom_xml(cur[a])[0] for a in gone), lv.kb))
            sat = res[-1].get("result") == "true"
            for a in gone: del cur[a]
            self.live_ever_retracted.update(gone)
            lv.calls["retracted"] += len(gone)
        if new:
            xs, decl = [], set()
            for a in new:
                x, d = self.axiom_xml(wanted[a]); xs.append(x); decl |= set(d)
            decl -= self.live_declared; self.live_declared |= decl
            xs = ["<owl:Declaration><owl:%s IRI=%s/></owl:Declaration>" % (k, quoteattr(i)) for k, i in sorted(decl)] + xs
            res = lv.ask('<Tell kb="%s">\n%s\n</Tell>\n<IsKBSatisfiable kb="%s"/>' % (lv.kb, "\n".join(xs), lv.kb))
            sat = res[-1].get("result") == "true"
            for a in new: cur[a] = wanted[a]
            lv.calls["told"] += len(new)
        if os.environ.get("TLAYER_LIVE_TRACE"):          # for debugging: what is told and retracted at each change
            short = lambda a: a.replace(self.ns, "")
            sys.stderr.write("live: retract %s, tell %s -> %s\n" % ([short(a) for a in gone], [short(a) for a in new], "consistent" if sat else "INCONSISTENT"))
        return sat
    def run_batch_live(self, todo, comps):
        """The components in todo are new. Bring the live knowledge base to the facts of this state, then read the types and role fillers of their individuals."""
        self.stats["batches"] += 1
        bad_keys = set()
        def wanted():
            w = {}
            for c in comps:
                if c["key"] in bad_keys: continue
                e = self.cache.get(c["key"])
                if e is not None and not e["ok"]: continue
                for f in c["facts"]: w[self.axiom(f)] = f
            return w
        if not self.live_set(wanted()):
            for c in todo:
                if len(todo) == 1 or not self.k.consistent(self.ontology([c])): bad_keys.add(c["key"])
            if not bad_keys or not self.live_set(wanted()):
                raise RuntimeError("the knowledge base stayed inconsistent although every changed component is consistent")
        for c in todo:
            if c["key"] in bad_keys: self.cache[c["key"]] = {"ok": False, "types": {}, "props": set(), "facts": c["facts"]}
        good = [c for c in todo if c["key"] not in bad_keys]
        if not good: return
        lv = self.live
        owner = {self.iri(a): c["key"] for c in good for a in c["inds"]}
        inds = sorted({a for c in good for a in c["inds"]})
        queries, body = [], []
        if self.class_atoms:
            # the entailed types of each individual (C), and for the open world the individuals for which 'not C' is entailed: the instances of the complement of C.
            # No helper class TLneg_C is in the live terminology: a class defined as a complement makes Konclude classify the terminology again after every update.
            for a in inds:
                queries.append(("types", a)); body.append('<GetFlattenedTypes kb="%s"><owl:NamedIndividual IRI=%s/></GetFlattenedTypes>' % (lv.kb, quoteattr(self.iri(a))))
            if not self.validation:
                for name in self.class_atoms:
                    queries.append(("neg", name)); body.append('<GetFlattenedInstances kb="%s"><owl:ObjectComplementOf><owl:Class IRI=%s/></owl:ObjectComplementOf></GetFlattenedInstances>' % (lv.kb, quoteattr(self.iri(name))))
        for p in self.prop_atoms:
            for a in inds:
                queries.append(("prop", p, a))
                body.append('<GetFlattenedObjectPropertyTargets kb="%s"><owl:ObjectProperty IRI=%s/><owl:NamedIndividual IRI=%s/></GetFlattenedObjectPropertyTargets>' % (lv.kb, quoteattr(self.iri(p)), quoteattr(self.iri(a))))
        res = lv.ask("\n".join(body)) if body else []
        if len(res) != len(queries): raise RuntimeError("expected %d answers, got %d" % (len(queries), len(res)))
        types = {self.iri(a): set() for a in inds}
        cprops = {c["key"]: set() for c in good}
        atom_iris = {self.iri(n) for n in self.class_atoms}
        for q, r in zip(queries, res):
            if q[0] == "types":
                types[self.iri(q[1])] = {x.get("IRI") for x in r.iter() if x.tag.endswith("}Class") and x.get("IRI") in atom_iris}
            elif q[0] == "neg":
                for x in r:
                    if x.get("IRI") in types: types[x.get("IRI")].add(self.negiri(q[1]))
            else:
                for n in [(x.get("IRI") or "") for x in r]: cprops[owner[self.iri(q[2])]].add((self.iri(q[1]), self.iri(q[2]), n))
        for c in good:
            self.cache[c["key"]] = {"ok": True, "types": {a: types.get(self.iri(a), set()) for a in c["inds"]}, "props": cprops[c["key"]], "facts": c["facts"]}
    def run_batch(self, comps):
        self.stats["batches"] += 1
        def materialize(group):
            types, props = self.k.materialize(self.ontology(group))
            owner = {self.iri(a): c["key"] for c in group for a in c["inds"]}
            cprops = {c["key"]: set() for c in group}
            for pr in props:
                key = owner.get(pr[1])
                if key is not None: cprops[key].add(pr)
            for c in group:
                self.cache[c["key"]] = {"ok": True, "types": {a: types.get(self.iri(a), set()) for a in c["inds"]},
                                        "props": cprops[c["key"]], "facts": c["facts"]}
        if self.k.consistent(self.ontology(comps)):
            materialize(comps)
            return
        good = []
        if len(comps) == 1: bad = comps
        else:
            bad = []
            for c in comps: (good if self.k.consistent(self.ontology([c])) else bad).append(c)
        for c in bad: self.cache[c["key"]] = {"ok": False, "types": {}, "props": set(), "facts": c["facts"]}
        if good: materialize(good)
    # atom values
    def atomval(self, name, args, i):
        key = (name, args, i)
        r = self._av.get(key)
        if r is None:
            v = self.atomval_open(name, args, i)
            # validation mode: a fact that is not entailed counts as false, but never for individuals whose snapshot is inconsistent (their value stays unknown)
            blank = any(a in self.bad[i] for a in args)
            r = F if (self.validation and v == U and not blank) else v
            self._av[key] = r
        return r
    def atomval_open(self, name, args, i):
        a = self.iri(args[0])
        ind = args[0]
        if ind not in self.present[i] or ind in self.bad[i]: return U
        if len(args) == 1:
            ts = self.types[i].get(ind, set())
            if self.iri(name) in ts: return T
            if self.negiri(name) in ts: return F
            return U
        if args[1] not in self.present[i] or args[1] in self.bad[i]: return U
        return T if (self.iri(name), a, self.iri(args[1])) in self.props[i] else U
    def atom_kind(self, name, args, i):
        """'entailed' (the reasoner derived it), 'assumed' (not entailed, counts as false in validation mode)"""
        return "assumed" if (self.validation and self.atomval_open(name, args, i) == U) else "entailed"
    def atom_windows(self, atom):
        """Time windows (merged) where the ground atom is TRUE / FALSE."""
        out = {"T": [], "F": []}
        for i, w in enumerate(self.windows):
            v = self.atomval(atom[1], atom[2], i)[1]
            if self.validation and v == "U" and not any(a in self.bad[i] for a in atom[2]): v = "F"
            if v in out:
                if out[v] and out[v][-1][1] == i - 1: out[v][-1][1] = i
                else: out[v].append([i, i])
        return out
    def fmt_range(self, a, b):
        return "[%s, %s]" % (self.axis.fmt(self.windows[a][0]), self.axis.fmt(self.windows[b][1]))
    def tag(self, line):
        name = getattr(self, "derived_axioms", {}).get(line)
        return line + "      <- derived by the definition of %s" % name if name else line
    # justification
    def justify(self, atom, i, truth):
        ind = atom[2][0]
        comp = next((c for c in self.state_comps[i] if ind in c["inds"]), None)
        if comp is None: return []
        ck = (comp["key"], atom, truth)
        if ck in self.justcache: return self.justcache[ck]
        if len(atom[2]) == 1:
            ca = "ClassAssertion(%s <%s>)" % (("ObjectComplementOf(<%s>)" if truth == "T" else "<%s>") % self.iri(atom[1]), self.iri(ind))
        else:
            ca = "NegativeObjectPropertyAssertion(<%s> <%s> <%s>)" % (self.iri(atom[1]), self.iri(ind), self.iri(atom[2][1]))
        lines = [l for l in self.k.explain(self.ontology([comp], extra=[ca])) if l != ca]
        lines = [self.tag(l) for l in lines]
        self.justcache[ck] = lines
        return lines

# ---------------------------------------------------------------- driver

def evaluate(layer, formula, closed, start, want_bindings=None, max_bindings=20000, now=None):
    """Returns a list of result dicts (one per binding of the variables).
    A formula with bounds F[a,b], G[a,b], O[a,b], H[a,b] is evaluated on the trace with one step per day (or tick); atoms are constant within a state."""
    vs = variables(formula)
    if len(vs) > 2: raise ValueError("at most two variables per formula")
    dom = layer.individuals
    if len(dom) ** len(vs) > max_bindings: raise ValueError("too many bindings (%d); raise --max-bindings" % (len(dom) ** len(vs)))
    metric = uses_metric(formula)
    if metric:
        if now is None: raise ValueError("a formula with bounds (F[a,b], ...) needs --now: the day on which the observation ends")
        t0 = layer.windows[0][0]
        n = now - t0 + 1
        if n <= 0: raise ValueError("--now is before the first change")
        starts = [w[0] for w in layer.windows]
        import bisect
        state_of = [max(0, bisect.bisect_right(starts, t0 + d) - 1) for d in range(n)]
        time_of = lambda i: t0 + i
        first = 0
        if start:
            first = max(0, layer.windows[start][0] - t0)
    else:
        n = len(layer.windows)
        time_of = lambda i: layer.windows[i][0]
        first = start
    res = []
    for combo in itertools.product(dom, repeat=len(vs)):
        b = dict(zip(vs, combo))
        g = subst(formula, b)
        if metric:
            mon = Monitor(lambda name, args, i, so=state_of: layer.atomval(name, args, so[i]))
        else:
            mon = Monitor(layer.atomval)
        R, decided = mon.run(g, first, n)
        fin = finish(R)
        r = {"binding": b, "result": status(finish(R) if closed else R), "residual": show(R) if R[0] != "c" else None,
             "if_history_ends_now": status(fin), "decided_at": None if decided is None else layer.axis.fmt(time_of(decided)),
             "ground": g, "ctx": (g, first, n, state_of if metric else None, closed)}
        res.append(r)
    return res

def why_cells(layer, r):
    """The smallest set of atom values over time behind the answer of one result, as periods: list of dicts."""
    g, first, n, state_of, closed = r["ctx"]
    answer, cells = minimal_support(g, layer.atomval, first, n, closed, state_of)
    if cells is None: return []
    by = {}
    for (name, args, st) in cells: by.setdefault((name, args), []).append(st)
    out = []
    for (name, args), sts in sorted(by.items(), key=lambda kv: (min(kv[1]), kv[0])):
        sts.sort()
        runs = []
        for st in sts:
            v = layer.atomval(name, args, st)[1]
            if runs and runs[-1][1] == st - 1 and runs[-1][2] == v: runs[-1][1] = st
            else: runs.append([st, st, v])
        for a, b, v in runs:
            kinds = {layer.atom_kind(name, args, st) for st in range(a, b + 1) if layer.atomval(name, args, st)[1] == v}
            out.append({"atom": show(("atom", name, args)), "value": "true" if v == "T" else "false", "from": layer.axis.fmt(layer.windows[a][0]), "to": layer.axis.fmt(layer.windows[b][1]),
                        "assumed": kinds == {"assumed"}, "first_state": a, "name": name, "args": args})
    return out

def load_formulas(args):
    out = []
    for i, t in enumerate(args.formula or []):
        out.append(("q%d" % (i + 1), t))
    if args.formula_file:
        for n, line in enumerate(open(args.formula_file, encoding="utf-8"), 1):
            line = line.split("#", 1)[0].strip()
            if not line: continue
            m = re.match(r"^([A-Za-z_]\w*)\s*:\s*(.*)$", line)
            out.append((m.group(1), m.group(2)) if m and not m.group(2).startswith(":") else ("f%d" % n, line))
    if not out: raise ValueError("give a formula with --formula or --formula-file")
    return [(name, text, Parser(text).parse()) for name, text in out]

DEF_RE = re.compile(r"^\s*([A-Za-z_]\w*)\(\s*(\?\w+)\s*\)\s*:=\s*(.+?)\s*$")

def load_definitions(args):
    texts = list(args.define or [])
    if args.define_file:
        for line in open(args.define_file, encoding="utf-8"):
            line = line.split("#", 1)[0].strip()
            if line: texts.append(line)
    defs = []
    for t in texts:
        m = DEF_RE.match(t)
        if not m: raise ValueError("a definition looks like   Name(?x) := <formula about ?x>   but this is: " + t)
        name, var, body_text = m.group(1), m.group(2), m.group(3)
        body = Parser(body_text).parse()
        if has_future(body): raise ValueError("definition of %s: a definition may only look at the present and the past (no G, F, X, U, F[..], G[..])" % name)
        if any(v != var for v in variables(body)): raise ValueError("definition of %s: the only variable allowed is %s" % (name, var))
        defs.append((name, var, body, t))
    names = {d[0] for d in defs}
    for name, var, body, t in defs:
        if any(a[1] in names for a in atoms_of(body)): raise ValueError("definition of %s: a definition cannot use another defined class (the ontology classes and the facts only)" % name)
    return defs

def main(argv=None):
    ap = argparse.ArgumentParser(description="Temporal layer around Konclude")
    ap.add_argument("--tbox", required=True, help="static ontology, OWL functional syntax (.ofn)")
    ap.add_argument("--facts", required=True, help="facts with validity intervals")
    ap.add_argument("--formula", action="append", help="temporal formula (can be repeated)")
    ap.add_argument("--formula-file", help="file with 'name: formula' lines")
    ap.add_argument("--define", action="append", help="temporal definition  Name(?x) := <formula about the past of ?x>  (can be repeated): the class Name is derived for ?x while the formula holds and goes into the reasoner")
    ap.add_argument("--define-file", help="file with one definition per line")
    ap.add_argument("--konclude", default=os.environ.get("KONCLUDE_BIN", "Konclude"), help="path of the Konclude binary")
    ap.add_argument("--ns", help="namespace for the short names (default: the default prefix of the TBox)")
    ap.add_argument("--now", help="the history ends at this time (default: the trace ends at the last change)")
    ap.add_argument("--at", help="evaluate the formula from this time on (earlier states stay available to past operators)")
    ap.add_argument("--closed", action="store_true", help="the history is complete: unmet obligations count (finite-trace semantics)")
    ap.add_argument("--validation", action="store_true", help="validation mode: an atom that is not entailed counts as false (closed-world reading of missing data; not pure OWL)")
    ap.add_argument("--why", action="store_true", help="for every TRUE or FALSE answer that is fixed: the smallest set of atom values over time that decides it (every value in it is needed)")
    ap.add_argument("--explain", action="store_true", help="add the justification of the atoms (Konclude explain)")
    ap.add_argument("--timeline", action="store_true", help="print the snapshots")
    ap.add_argument("--show", choices=["all", "problems"], default="all", help="with variables: list all bindings or only those that are not TRUE")
    ap.add_argument("--live", action="store_true", help="keep ONE running Konclude with ONE knowledge base: load the TBox once, then tell the facts that began and retract the facts that ended at each change point (OWLlink revisions, IncrementalRebuild on). Same answers; far cheaper per period when the TBox is large")
    ap.add_argument("--live-timeout", type=int, default=300, help="with --live: seconds one request to the running Konclude may take; when it takes longer (a hang in Konclude's incremental mode was seen in rare cases) live mode is given up and the layer continues, with the same answers, with one process per group")
    ap.add_argument("--no-cache", action="store_true", help="re-reason every snapshot completely (same answers, slower)")
    ap.add_argument("--whole-snapshots", action="store_true", help="reason over each snapshot as one ontology instead of per connected component (an inconsistency then blanks every individual of that snapshot)")
    ap.add_argument("--max-bindings", type=int, default=20000)
    ap.add_argument("-o", "--json", help="write the results as JSON")
    a = ap.parse_args(argv)

    axis = TimeAxis()
    facts = parse_facts(open(a.facts, encoding="utf-8").read(), axis)
    defs = load_definitions(a)
    a.formula = a.formula or []
    forms = load_formulas(a) if (a.formula or a.formula_file) else []
    if not forms and not defs: raise ValueError("give a formula (--formula, --formula-file) or a definition (--define, --define-file)")
    k = Konclude(a.konclude)
    layer = Layer(open(a.tbox, encoding="utf-8").read(), facts, k, ns=a.ns, use_cache=not a.no_cache, axis=axis, whole_snapshots=a.whole_snapshots)
    layer.validation = a.validation
    layer.live_requested = a.live
    layer.live_timeout = a.live_timeout
    now = axis.parse(a.now) if a.now else None
    layer.build_trace(now)
    start = 0
    if a.at:
        t = axis.parse(a.at)
        start = max([i for i, w in enumerate(layer.windows) if w[0] <= t] or [0])
    t0 = time.time()
    layer.analyse([f for _, _, f in forms], defs)
    t_reason = time.time() - t0

    print("Trace: %d states (change points), %s .. %s%s" % (len(layer.windows), axis.fmt(layer.windows[0][0]), axis.fmt(layer.windows[-1][1]),
          "" if now is not None else " (the last state is open)"))
    print("Reasoning: %s; %d component checks, %d answered from the cache, %d Konclude calls (%.2f s)" % (
          ("per component" + (" with cache" if layer.cache_enabled else ", cache off")) if layer.per_component else ("whole snapshots" + ((" (the TBox contains %s: components may interact)" % layer.global_reason) if layer.global_reason else " (asked for)") + ("" if layer.cache_enabled else ", cache off")),
          layer.stats["components"], layer.stats["cache_hits"], sum(k.calls.values()), k.seconds))
    if a.live:
        if layer.live is not None: layer.live_stats = dict(layer.live.calls, seconds=round(layer.live.seconds, 2))
        print("Live mode: %s" % (layer.live_note or ("one running Konclude, %d requests, %d facts told, %d retracted, %d knowledge base restarts (a fact came back after it was retracted), %.2f s" % (layer.live_stats["requests"], layer.live_stats["told"], layer.live_stats["retracted"], layer.live_stats["restarts"], layer.live_stats["seconds"]))))
    if a.timeline:
        print("\nSnapshots:")
        for i, w in enumerate(layer.windows):
            print("  S%-3d %s .. %s   %d facts, %d components%s" % (i, axis.fmt(w[0]), axis.fmt(w[1]), len(layer.state_facts[i]), len(layer.state_comps[i]),
                  ("   INCONSISTENT: " + ", ".join(sorted(layer.bad[i]))) if layer.bad[i] else ""))
    if defs:
        print("\nTemporal definitions (the derived classes go into the reasoner):")
        for (dname, var, body, text) in defs:
            ds = [d for d in layer.derived if d["definition"] == dname]
            inds = sorted({d["individual"] for d in ds})
            print("  %s: derived for %d individual(s) in %d period(s)%s" % (text, len(inds), len(ds), (" - e.g. " + "; ".join("%s %s..%s" % (d["individual"], d["from"], d["to"]) for d in ds[:4])) if ds else ""))
    # inconsistent periods: consecutive states with the same cause are reported once
    reported = {}
    for i in range(len(layer.windows)):
        for c in layer.state_comps[i]:
            if not layer.cache[c["key"]]["ok"]:
                reported.setdefault(c["key"], {"comp": c, "states": []})["states"].append(i)
    for key, rec in reported.items():
        c, sts = rec["comp"], rec["states"]
        runs = []
        for i in sts:
            if runs and runs[-1][1] == i - 1: runs[-1][1] = i
            else: runs.append([i, i])
        where = "; ".join("S%d..S%d (%s .. %s)" % (r0, r1, axis.fmt(layer.windows[r0][0]), axis.fmt(layer.windows[r1][1])) if r0 != r1 else "S%d (%s .. %s)" % (r0, axis.fmt(layer.windows[r0][0]), axis.fmt(layer.windows[r0][1])) for r0, r1 in runs)
        print("\nWARNING: inconsistent for %s in %s; its atoms count as UNKNOWN. Cause:" % (", ".join(sorted(c["inds"])), where))
        for l in k.explain(layer.ontology([c])): print("    " + layer.tag(l))

    allres = []
    for name, text, f in forms:
        res = evaluate(layer, f, a.closed, start, max_bindings=a.max_bindings, now=now)
        counts = {}
        for r in res: counts[r["result"]] = counts.get(r["result"], 0) + 1
        print("\n[%s] %s" % (name, text))
        if len(res) > 1 or res[0]["binding"]:
            print("  " + ", ".join("%d %s" % (v, kk) for kk, v in sorted(counts.items())))
        for r in res:
            if a.show == "problems" and r["binding"] and r["result"] == "TRUE": continue
            b = ", ".join("%s=%s" % kv for kv in r["binding"].items())
            line = "  %s%s" % ((b + "  ") if b else "", r["result"])
            if r["decided_at"]: line += "   (fixed at %s)" % r["decided_at"]
            if r["result"] == "PENDING":
                wait = r["residual"]
                if len(wait) > 160: wait = wait[:140] + " ... (%d open deadline windows)" % wait.count("[day ")
                line += "   (if the history ended now: %s; still waiting for: %s)" % (r["if_history_ends_now"], wait)
            print(line)
            if a.why and r["result"] in ("TRUE", "FALSE") and (r["decided_at"] or a.closed):
                cells = why_cells(layer, r)
                r["why"] = [{k3: v3 for k3, v3 in c.items() if k3 not in ("first_state", "name", "args")} for c in cells]
                if cells:
                    print("      why %s: the smallest set of atom values over time that decides it (every one is needed):" % r["result"])
                    for c in cells:
                        print("        %-28s %-6s %s .. %s%s" % (c["atom"], c["value"], c["from"], c["to"], "   (not entailed: counts as false in validation mode)" if c["assumed"] else ""))
                        if a.explain and not c["assumed"]:
                            at = ("atom", c["name"], c["args"])
                            for l in layer.justify(at, c["first_state"], "T" if c["value"] == "true" else "F"): print("            " + l)
            # witnesses: where each atom of the grounded formula holds
            ats = sorted(atoms_of(r["ground"]), key=repr)
            if len(res) <= 12 or r["result"] != "TRUE":
                for at in ats:
                    w = layer.atom_windows(at)
                    parts = []
                    if w["T"]: parts.append("true " + ", ".join(layer.fmt_range(x, y) for x, y in w["T"]))
                    if w["F"]: parts.append("false " + ", ".join(layer.fmt_range(x, y) for x, y in w["F"]))
                    print("      %s: %s" % (show(at), "; ".join(parts) if parts else "never entailed (UNKNOWN)"))
                    if a.explain:
                        for truth in ("T", "F"):
                            if w[truth]:
                                i0 = w[truth][0][0]
                                lines = layer.justify(at, i0, truth)
                                if lines:
                                    print("        why %s at %s:" % ("true" if truth == "T" else "false", axis.fmt(layer.windows[i0][0])))
                                    for l in lines: print("          " + l)
        allres.append({"name": name, "formula": text, "results": [{k2: v for k2, v in r.items() if k2 not in ("ground", "ctx")} for r in res]})
    print("\nKonclude calls: %s, total reasoning time %.2f s" % (k.calls, t_reason))
    layer.close_live()
    if a.json:
        with open(a.json, "w", encoding="utf-8") as fo:
            json.dump({"states": [{"from": axis.fmt(w[0]), "to": axis.fmt(w[1]), "facts": len(layer.state_facts[i]), "inconsistent": sorted(layer.bad[i])} for i, w in enumerate(layer.windows)],
                       "stats": layer.stats, "calls": k.calls, "formulas": allres, "derived": getattr(layer, "derived", [])}, fo, indent=1)
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except ValueError as e:
        print("Error: %s" % e, file=sys.stderr); sys.exit(2)
