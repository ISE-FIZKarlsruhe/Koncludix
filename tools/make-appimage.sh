#!/usr/bin/env bash
# Builds a single-file Linux AppImage from a Konclude binary that you built yourself.
#
#   tools/make-appimage.sh path/to/Konclude  output-dir  [path/to/appimagetool]
#
# What it does: copies the binary, the Configs and every shared library the binary needs (except the basic C library) into an AppDir,
# then packs the AppDir with appimagetool. Nothing is downloaded. Get appimagetool from https://github.com/AppImage/appimagetool/releases
# if you do not have it (or put it in the environment variable APPIMAGETOOL).
#
# IMPORTANT: the image runs only on systems whose C library (glibc) is at least as new as the one the libraries were taken from.
# The script prints that version at the end. To run on many systems, build the binary on an OLD system (for example Ubuntu 22.04)
# and run this script there.
set -euo pipefail
BIN="${1:?usage: make-appimage.sh path/to/Konclude output-dir [appimagetool]}"
OUT="${2:?usage: make-appimage.sh path/to/Konclude output-dir [appimagetool]}"
TOOL="${3:-${APPIMAGETOOL:-}}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(dirname "$HERE")"
[ -x "$BIN" ] || { echo "binary not found: $BIN"; exit 1; }
[ -n "$TOOL" ] && [ -x "$TOOL" ] || { echo "appimagetool not found (pass it as 3rd argument or set APPIMAGETOOL)"; exit 1; }
mkdir -p "$OUT"
APPDIR="$(mktemp -d)/AppDir"
mkdir -p "$APPDIR/lib" "$APPDIR/Configs"
cp "$BIN" "$APPDIR/Konclude"
cp "$HERE"/appimage/AppRun "$HERE"/appimage/konclude.desktop "$HERE"/appimage/konclude.png "$APPDIR/"
cp "$APPDIR/konclude.png" "$APPDIR/.DirIcon"
chmod +x "$APPDIR/AppRun" "$APPDIR/Konclude"
cp "$REPO"/Configs/*.xml "$APPDIR/Configs/"
# shared libraries: everything except the C library itself, which must come from the machine that runs the image
ldd "$BIN" | awk '/=> \//{print $3}' | grep -v -E '/(libc|libm|libdl|libpthread|librt|libresolv|libutil|libnsl|ld-linux[^/]*)\.so' | while read -r lib; do
  cp -L "$lib" "$APPDIR/lib/"
done
# AppRun sets LD_LIBRARY_PATH to lib/ (see tools/appimage/AppRun)
need="$(for f in "$APPDIR/Konclude" "$APPDIR"/lib/*; do objdump -T "$f" 2>/dev/null | grep -o 'GLIBC_[0-9.]*'; done | sort -Vu | tail -1)"
VERSION="${KONCLUDE_VERSION:-$(date +%d%m%Y)}"
IMG="$OUT/Konclude-fiz-$VERSION-x86_64.AppImage"
ARCH=x86_64 "$TOOL" --appimage-extract-and-run "$APPDIR" "$IMG" >/dev/null
chmod +x "$IMG"
echo "built $IMG ($(du -h "$IMG" | cut -f1))"
echo "needs glibc >= ${need#GLIBC_} on the machine that runs it (libraries bundled: $(ls "$APPDIR/lib" | wc -l))"
