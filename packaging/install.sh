#!/usr/bin/env sh
# Copy kestrel-hub and kestrel-agent into $PREFIX/bin.
#
# Usage:
#   ./install.sh                # installs to /usr/local/bin (sudo if needed)
#   PREFIX=~/.local ./install.sh   # user-local install, no sudo
set -eu

PREFIX="${PREFIX:-/usr/local}"
bindir="${PREFIX}/bin"
here="$(cd "$(dirname "$0")" && pwd)"

if [ ! -x "${here}/bin/kestrel-hub" ] || [ ! -x "${here}/bin/kestrel-agent" ]; then
    echo "error: ${here}/bin/ is missing the kestrel binaries." >&2
    echo "       Run this script from inside the unpacked tarball." >&2
    exit 1
fi

mkdir -p "${bindir}" 2>/dev/null || true

sudo=""
if [ ! -w "${bindir}" ]; then
    if command -v sudo >/dev/null 2>&1; then
        sudo="sudo"
        echo ">> ${bindir} is not writable; using sudo"
    else
        echo "error: ${bindir} is not writable and sudo is unavailable." >&2
        echo "       Re-run with PREFIX=\$HOME/.local to install without root." >&2
        exit 1
    fi
fi

${sudo} install -m 0755 "${here}/bin/kestrel-hub"   "${bindir}/kestrel-hub"
${sudo} install -m 0755 "${here}/bin/kestrel-agent" "${bindir}/kestrel-agent"

echo ">> installed:"
echo "   ${bindir}/kestrel-hub"
echo "   ${bindir}/kestrel-agent"

case ":${PATH}:" in
    *":${bindir}:"*) ;;
    *) echo
       echo "note: ${bindir} is not on your PATH."
       echo "      add it to your shell rc, e.g.:"
       echo "        export PATH=\"${bindir}:\$PATH\""
       ;;
esac
