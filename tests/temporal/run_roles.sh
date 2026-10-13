cd /mnt/d/Github/Koncludix/tests/temporal
export KONCLUDE_BIN=~/kon_all2/Release/Konclude
python3 ../../tools/temporal/tlayer.py --tbox roles_tbox.ofn --facts roles_facts.tl --validation --now 2026-10-12 --timeline --why \
  --define "FormerDirector(?x) := O Director(?x) & !Director(?x)" \
  --formula "Staff(Anna)" --formula "O Director(Anna)" --formula "FormerDirector(Anna)" --formula "F Alumni(Anna)" 2>&1 | grep -v "^      Director\|^      Professor\|^      Staff\|^      Former\|^      Alumni"
