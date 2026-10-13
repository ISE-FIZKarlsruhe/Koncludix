#!/bin/bash
# Time demo on OWL2Bench DL-1: who is head (chair) of a department, how that changes over the years, and what the temporal layer answers.
#   usage:  bash demo/dl1_time_demo.sh [PATH_TO_KONCLUDE]       (about 3 minutes; the layer starts its own Konclude in live mode)
#   file:   DL1=/path/to/OWL2DL-1_Functional.owl bash demo/dl1_time_demo.sh
# The dates are invented for the demo. In DL-1 the head of department U0C0D0 is one fixed fact; here that fact is taken out of the
# static ontology and given a time, together with two earlier heads, exactly as real data would be migrated.
set -u
HERE="$(cd "$(dirname "$0")/.." && pwd)"
K="${1:-${KONCLUDE_BIN:-$HOME/kon_all2/Release/Konclude}}"
DL1="${DL1:-/mnt/d/downloads/OWL2DL-1_Functional.owl}"
W="$(mktemp -d /tmp/dl1time.XXXX)"
export KONCLUDE_SUPERVISED=1
[ -x "$K" ] || { echo "No Konclude found at $K"; exit 1; }
[ -f "$DL1" ] || { echo "OWL2Bench DL-1 not found at $DL1: set DL1=..."; exit 1; }
T="python3 $HERE/tools/temporal/tlayer.py --konclude $K --tbox $W/DL1_static.ofn --live"
step() { echo; echo "=============================================================================="; echo "$1"; echo "=============================================================================="; }
show() { grep -a -v "^Reasoning:\|^Konclude calls\|^Trace:\|^Live mode\|^$"; }

# the static ontology: DL-1 without the fixed fact "U0C0D0FP4 is head of U0C0D0" (it becomes a timed fact below)
grep -a -v "^ObjectPropertyAssertion(owl2bench:isHeadOf owl2bench:U0C0D0FP4 owl2bench:U0C0D0)" "$DL1" > "$W/DL1_static.ofn"

cat > "$W/heads.tl" <<'EOF'
# the head of department U0C0D0 over the years (invented dates)
isHeadOf(U0C0D0FP0, U0C0D0)   [2015-01-01, 2018-08-31]
isHeadOf(U0C0D0FP1, U0C0D0)   [2018-09-01, 2022-12-31]
isHeadOf(U0C0D0FP4, U0C0D0)   [2023-01-01, inf)
EOF

step "1. The snapshots: one new snapshot whenever a fact starts or ends (nothing else is needed)"
$T --facts "$W/heads.tl" --now 2026-01-01 --timeline --formula "Chair(?x)" 2>&1 | sed -n '/^Snapshots/,/^$/p'

step "2. Who is a chair? Konclude infers it in each snapshot: DL-1 says Chair = someone who is head of a department (nobody wrote 'Chair')"
echo "Question: at some time, is somebody a chair?  (F = at some time later; the lines under each answer are the periods from the snapshots)"
$T --facts "$W/heads.tl" --now 2026-01-01 --formula "F Chair(?x)" 2>&1 | show | grep -a -v "U0C0D0:\|^      Chair(U0C0D0)\|^  ?x=U0C0D0  "

step "3. The career history of professor FP1: when was he a chair?"
$T --facts "$W/heads.tl" --now 2026-01-01 --formula "F Chair(U0C0D0FP1)" 2>&1 | show

step "4. A question about the past: standing on 2020-06-01, who is the chair?"
$T --facts "$W/heads.tl" --now 2026-01-01 --at 2020-06-01 --formula "Chair(?x)" 2>&1 | show | grep -a "TRUE\|\[q1\]"

step "5. A data mistake: FP1 (a man in DL-1) is recorded as a woman for the month of February 2020"
cat > "$W/mistake.tl" <<'EOF'
isHeadOf(U0C0D0FP0, U0C0D0)   [2015-01-01, 2018-08-31]
isHeadOf(U0C0D0FP1, U0C0D0)   [2018-09-01, 2022-12-31]
isHeadOf(U0C0D0FP4, U0C0D0)   [2023-01-01, inf)
Woman(U0C0D0FP1)              [2020-02-01, 2020-02-29]
EOF
$T --facts "$W/mistake.tl" --now 2026-01-01 --timeline --formula "Chair(?x)" 2>&1 | sed -n '/^Snapshots/,/^\[q1\]/p' | grep -a -v "^\[q1\]" | cut -c1-200 | head -16

step "6. Two changes on the SAME day: use a finer clock. Here the time is in minutes (09:00 = 540, 17:00 = 1020) instead of days"
cat > "$W/sameday.tl" <<'EOF'
# one working day, time in minutes since midnight: FP0 is head until 12:00, FP1 from 12:01 to 17:00, FP4 from 17:01 on
isHeadOf(U0C0D0FP0, U0C0D0)   [540, 720]
isHeadOf(U0C0D0FP1, U0C0D0)   [721, 1020]
isHeadOf(U0C0D0FP4, U0C0D0)   [1021, inf)
EOF
$T --facts "$W/sameday.tl" --now 1200 --timeline --formula "Chair(?x)" 2>&1 | sed -n '/^Snapshots/,/^$/p'

echo; echo "Files written: $W"
