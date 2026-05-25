#!/usr/bin/env sh
# Remove kestrel-hub and kestrel-agent from $PREFIX/bin.
#
# Usage:
#   ./uninstall.sh                # removes from /usr/local/bin
#   PREFIX=~/.local ./uninstall.sh
#
# This script does NOT clear keyring entries or delete kestrel.toml.
# Run `kestrel-hub unenroll` / `kestrel-agent unenroll` first if you
# want a clean wipe.
set -eu

PREFIX="${PREFIX:-/usr/local}"
bindir="${PREFIX}/bin"

sudo=""
if [ ! -w "${bindir}" ] 2>/dev/null && command -v sudo >/dev/null 2>&1; then
    sudo="sudo"
fi

removed=0
for bin in kestrel-hub kestrel-agent; do
    target="${bindir}/${bin}"
    if [ -e "${target}" ]; then
        ${sudo} rm -f "${target}"
        echo ">> removed ${target}"
        removed=$((removed + 1))
    fi
done

if [ "${removed}" -eq 0 ]; then
    echo "nothing to remove from ${bindir}"
fi
