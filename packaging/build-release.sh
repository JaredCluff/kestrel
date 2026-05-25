#!/usr/bin/env bash
# Build kestrel release binaries and assemble a distributable tar.gz.
#
# Runs on macOS and Linux. Windows users: use packaging/build-release.ps1
# instead — bash on Windows (Git Bash / MSYS) would produce a tarball with
# the wrong path separators and miss the .exe extension.
#
# Usage: ./packaging/build-release.sh [--target <triple>]
#
# Default target is the host triple as reported by rustc. The script writes
# to dist/ at the repo root:
#   dist/kestrel-<version>-<target>/         (staging dir)
#   dist/kestrel-<version>-<target>.tar.gz   (final tarball)
set -euo pipefail

case "$(uname -s)" in
    MINGW*|MSYS*|CYGWIN*)
        echo "error: this script is for macOS/Linux. On Windows run packaging/build-release.ps1." >&2
        exit 1 ;;
esac

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

target=""
while [ $# -gt 0 ]; do
    case "$1" in
        --target) target="$2"; shift 2 ;;
        -h|--help)
            cat <<'EOF'
Usage: ./packaging/build-release.sh [--target <triple>]

Builds release binaries for kestrel-hub + kestrel-agent and stages them
into dist/kestrel-<version>-<target>/, then produces a .tar.gz alongside.

Default target is the host triple (rustc -vV | host).
EOF
            exit 0 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

if ! command -v cargo >/dev/null 2>&1; then
    echo "error: cargo not on PATH. Source ~/.cargo/env or install rustup." >&2
    exit 1
fi

host_triple="$(rustc -vV | awk '/^host:/ {print $2}')"
target="${target:-$host_triple}"

# Read version straight from the hub crate's Cargo.toml. Avoids depending
# on python3 or jq; the format is stable (version = "x.y.z" on its own line).
version="$(awk -F'"' '/^version[[:space:]]*=/ {print $2; exit}' \
    crates/kestrel-hub/Cargo.toml)"
if [ -z "$version" ]; then
    echo "error: could not parse version from crates/kestrel-hub/Cargo.toml" >&2
    exit 1
fi

# Pin openh264 to building from source instead of downloading Cisco's
# binary blob. Builds are then reproducible offline and don't depend on
# Cisco's CDN. nasm must be on PATH (see README "Building from source").
export OPENH264_FROM_SOURCE="${OPENH264_FROM_SOURCE:-1}"

echo ">> building kestrel v${version} for ${target}"

build_args=(--release -p kestrel-hub -p kestrel-agent)
if [ "$target" != "$host_triple" ]; then
    build_args+=(--target "$target")
    artifact_dir="target/${target}/release"
else
    artifact_dir="target/release"
fi

cargo build "${build_args[@]}"

stage="dist/kestrel-${version}-${target}"
rm -rf "$stage"
mkdir -p "$stage/bin"

# `cp` + `chmod` works the same on BSD (mac) and GNU (linux) coreutils;
# avoids the subtle `install` syntax differences between the two.
cp "${artifact_dir}/kestrel-hub"   "$stage/bin/kestrel-hub"
cp "${artifact_dir}/kestrel-agent" "$stage/bin/kestrel-agent"
chmod 0755 "$stage/bin/kestrel-hub" "$stage/bin/kestrel-agent"

cp packaging/install.sh   "$stage/install.sh"
cp packaging/uninstall.sh "$stage/uninstall.sh"
chmod 0755 "$stage/install.sh" "$stage/uninstall.sh"

cp README.md "$stage/README.md"
cp LICENSE   "$stage/LICENSE"

cat > "$stage/VERSION" <<EOF
kestrel ${version}
target  ${target}
built   $(date -u +%Y-%m-%dT%H:%M:%SZ)
commit  $(git rev-parse --short HEAD 2>/dev/null || echo unknown)
EOF

tarball="dist/kestrel-${version}-${target}.tar.gz"
rm -f "$tarball"
# COPYFILE_DISABLE=1 keeps macOS tar from prepending ._ AppleDouble
# entries. Linux tar ignores the env var, so setting it is safe either
# way.
COPYFILE_DISABLE=1 tar -czf "$tarball" -C dist "kestrel-${version}-${target}"

echo
echo ">> done"
echo "   staged:   $stage"
echo "   tarball:  $tarball"
du -h "$tarball" | awk '{print "   size:     " $1}'
