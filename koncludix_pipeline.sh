#!/bin/bash

set -e

START_TIME=$(date +%s)

# -----------------------------
# INPUTS
# -----------------------------
BINARY="/mnt/c/Users/Gunjan Singh/Downloads/Experiments/Konclude-v0.7.0-1135-Linux-x64-GCC-Static-Qt5.12.10/Konclude-v0.7.0-1135-Linux-x64-GCC-Static-Qt5.12.10/Binaries/Konclude"

INPUT="/mnt/c/Users/Gunjan Singh/Downloads/Experiments/Konclude-v0.7.0-1135-Linux-x64-GCC-Static-Qt5.12.10/Konclude-v0.7.0-1135-Linux-x64-GCC-Static-Qt5.12.10/Binaries/mse_kg_0804.ttl"

OUTPUT1="output_sparql.ttl"
OUTPUT2="output_realisation.ttl"
FINAL="finaloutput.ttl"

# -----------------------------
# STEP 1: SPARQL PIPELINE
# -----------------------------
echo "[STEP 1] Running SPARQL extraction..."
python3 konclude_sparql.py "$BINARY" "$INPUT" "$OUTPUT1"

# -----------------------------
# STEP 2: REALISATION
# -----------------------------
echo "[STEP 2] Running Konclude realisation..."

"$BINARY" realisation \
    -i "$INPUT" \
    -o "$OUTPUT2" \
    > /dev/null 2>&1

# -----------------------------
# STEP 3: ROBOT MERGE
# -----------------------------
echo "[STEP 3] Merging outputs with ROBOT..."
robot merge \
    --input "$OUTPUT1" \
    --input "$OUTPUT2" \
    --output "$FINAL"

# -----------------------------
# TIMER END
# -----------------------------
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo "----------------------------------"
echo "[DONE] Final output: $FINAL"
echo "[TIME] Total execution time: ${DURATION} seconds"
echo "----------------------------------"