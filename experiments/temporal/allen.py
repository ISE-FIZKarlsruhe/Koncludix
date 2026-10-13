"""Allen's interval algebra: the 13 relations, the composition table (derived by brute force, not typed in), and path consistency.

Intervals are (start, end) with start < end. The composition table is computed from all triples of intervals over 6 points: three intervals have at most
6 distinct end points, so every qualitative configuration occurs, and the table is exact by construction."""
import itertools

REL = ["before", "meets", "overlaps", "starts", "during", "finishes", "equals",
       "after", "metBy", "overlappedBy", "startedBy", "contains", "finishedBy"]
IDX = {r: i for i, r in enumerate(REL)}
INV = {"before": "after", "meets": "metBy", "overlaps": "overlappedBy", "starts": "startedBy", "during": "contains", "finishes": "finishedBy", "equals": "equals"}
INV.update({v: k for k, v in list(INV.items())})
FULL = (1 << 13) - 1


def relation(i, j):
    """the Allen relation r with r(i, j)"""
    a, b = i
    c, d = j
    assert a < b and c < d
    if b < c: return "before"
    if b == c: return "meets"
    if d < a: return "after"
    if d == a: return "metBy"
    if a == c and b == d: return "equals"
    if a == c: return "starts" if b < d else "startedBy"
    if b == d: return "finishes" if a > c else "finishedBy"
    if c < a and b < d: return "during"
    if a < c and d < b: return "contains"
    if a < c < b < d: return "overlaps"
    if c < a < d < b: return "overlappedBy"
    raise AssertionError((i, j))


def bit(r): return 1 << IDX[r]
def mask(rels): return sum(bit(r) for r in rels)
def rels_of(m): return [r for r in REL if m & bit(r)]
def inverse_mask(m):
    out = 0
    for r in rels_of(m): out |= bit(INV[r])
    return out


def _build_table():
    pts = range(6)
    ivs = [(a, b) for a in pts for b in pts if a < b]
    table = {}
    for x, y, z in itertools.product(ivs, repeat=3):
        key = (IDX[relation(x, y)], IDX[relation(y, z)])
        table[key] = table.get(key, 0) | bit(relation(x, z))
    return table


_TABLE = _build_table()
assert len(_TABLE) == 169


def compose(m1, m2):
    out = 0
    for i, r1 in enumerate(REL):
        if m1 & (1 << i):
            for j in range(13):
                if m2 & (1 << j):
                    out |= _TABLE[(i, j)]
    return out


def path_consistency(n, constraints):
    """constraints: dict {(i, j): mask} for i < j (missing = all relations). Returns (consistent, refined constraint matrix or None, conflicting pair or None).
    Path consistency is sound (an empty constraint proves inconsistency) but not complete for general networks."""
    C = [[FULL] * n for _ in range(n)]
    for i in range(n): C[i][i] = bit("equals")
    for (i, j), m in constraints.items():
        C[i][j] &= m
        C[j][i] &= inverse_mask(m)
        if C[i][j] == 0: return False, None, (i, j)
    changed = True
    while changed:
        changed = False
        for i in range(n):
            for j in range(n):
                if i == j: continue
                for k in range(n):
                    if k == i or k == j: continue
                    new = C[i][j] & compose(C[i][k], C[k][j])
                    if new != C[i][j]:
                        if new == 0: return False, None, (i, j)
                        C[i][j] = new; C[j][i] = inverse_mask(new)
                        changed = True
    return True, C, None


if __name__ == "__main__":
    # sanity: every pair of random intervals has exactly one relation, inverse is consistent, a few entries of the table are the textbook ones
    import random
    rnd = random.Random(1)
    for _ in range(20000):
        i = tuple(sorted(rnd.sample(range(50), 2))); j = tuple(sorted(rnd.sample(range(50), 2)))
        assert INV[relation(i, j)] == relation(j, i)
    assert compose(bit("before"), bit("before")) == bit("before")
    assert compose(bit("meets"), bit("meets")) == bit("before")
    assert compose(bit("during"), bit("contains")) == FULL
    assert compose(bit("equals"), bit("overlaps")) == bit("overlaps")
    assert compose(bit("starts"), bit("starts")) == bit("starts")
    print("composition table entries:", len(_TABLE), "; entries equal to the full set:", sum(1 for v in _TABLE.values() if v == FULL), "(textbook: 3 of 169 with all 13... checking)")
    ok, C, bad = path_consistency(3, {(0, 1): bit("before"), (1, 2): bit("before"), (0, 2): bit("after")})
    print("cycle a<b<c, c<a detected as inconsistent:", not ok)
