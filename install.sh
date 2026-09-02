#!/bin/sh
# rediscope installer.
#
#   curl -fsSL https://raw.githubusercontent.com/TarasKovalenko/Rediscope/main/install.sh | sh
#
# Environment:
#   REDISCOPE_VERSION   tag to install (default: latest release)
#   REDISCOPE_BIN_DIR   install directory (default: ~/.local/bin, or /usr/local/bin for root)
#   REDISCOPE_REPO      owner/name to install from (default: TarasKovalenko/Rediscope)
set -eu

REPO="${REDISCOPE_REPO:-TarasKovalenko/Rediscope}"
BIN="rediscope"

red() { printf '\033[31m%s\033[0m\n' "$*" >&2; }
dim() { printf '\033[2m%s\033[0m\n' "$*"; }
bold() { printf '\033[1m%s\033[0m\n' "$*"; }
die() { red "error: $*"; exit 1; }

need() { command -v "$1" >/dev/null 2>&1 || die "'$1' is required but not installed"; }

need uname
need tar
if command -v curl >/dev/null 2>&1; then
  fetch() { curl -fsSL "$1"; }
  download() { curl -fsSL --proto '=https' --tlsv1.2 -o "$2" "$1"; }
elif command -v wget >/dev/null 2>&1; then
  fetch() { wget -qO- "$1"; }
  download() { wget -qO "$2" "$1"; }
else
  die "either curl or wget is required"
fi

# ---- target detection ----------------------------------------------------
os="$(uname -s)"
arch="$(uname -m)"
case "$os" in
  Darwin) os_part="apple-darwin" ;;
  Linux)
    # A musl userland needs the statically linked build.
    if ldd /bin/sh 2>&1 | grep -qi musl || [ -f /etc/alpine-release ]; then
      os_part="unknown-linux-musl"
    else
      os_part="unknown-linux-gnu"
    fi
    ;;
  *) die "unsupported operating system: $os (build from source with 'cargo install --git https://github.com/$REPO')" ;;
esac
case "$arch" in
  x86_64|amd64) arch_part="x86_64" ;;
  arm64|aarch64) arch_part="aarch64" ;;
  *) die "unsupported architecture: $arch" ;;
esac
target="${arch_part}-${os_part}"

# ---- version -------------------------------------------------------------
version="${REDISCOPE_VERSION:-}"
if [ -z "$version" ]; then
  version="$(fetch "https://api.github.com/repos/$REPO/releases/latest" \
    | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n1)"
  [ -n "$version" ] || die "could not determine the latest release of $REPO"
fi

# ---- install directory ---------------------------------------------------
if [ -n "${REDISCOPE_BIN_DIR:-}" ]; then
  bin_dir="$REDISCOPE_BIN_DIR"
elif [ "$(id -u)" = "0" ]; then
  bin_dir="/usr/local/bin"
else
  bin_dir="$HOME/.local/bin"
fi

asset="${BIN}-${version}-${target}.tar.gz"
base="https://github.com/$REPO/releases/download/$version"

bold "Installing $BIN $version ($target)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT INT TERM

dim "  downloading $asset"
download "$base/$asset" "$tmp/$asset" || die "no prebuilt binary for $target in release $version"

# ---- checksum ------------------------------------------------------------
if download "$base/SHA256SUMS" "$tmp/SHA256SUMS" 2>/dev/null; then
  expected="$(grep " $asset\$" "$tmp/SHA256SUMS" | awk '{print $1}')"
  if [ -n "$expected" ]; then
    if command -v sha256sum >/dev/null 2>&1; then
      actual="$(sha256sum "$tmp/$asset" | awk '{print $1}')"
    elif command -v shasum >/dev/null 2>&1; then
      actual="$(shasum -a 256 "$tmp/$asset" | awk '{print $1}')"
    else
      actual=""
    fi
    if [ -n "$actual" ]; then
      [ "$actual" = "$expected" ] || die "checksum mismatch for $asset (expected $expected, got $actual)"
      dim "  checksum verified"
    else
      dim "  skipping checksum (no sha256sum/shasum available)"
    fi
  fi
else
  dim "  skipping checksum (SHA256SUMS not published for this release)"
fi

tar -xzf "$tmp/$asset" -C "$tmp"
[ -f "$tmp/$BIN" ] || die "archive did not contain a '$BIN' binary"

mkdir -p "$bin_dir"
# Replacing a running binary fails on some systems; install atomically instead.
chmod +x "$tmp/$BIN"
mv -f "$tmp/$BIN" "$bin_dir/$BIN" 2>/dev/null || {
  rm -f "$bin_dir/$BIN"
  mv "$tmp/$BIN" "$bin_dir/$BIN"
}

bold "Installed $bin_dir/$BIN"
case ":$PATH:" in
  *":$bin_dir:"*) dim "  run: $BIN" ;;
  *)
    printf '\n'
    red "$bin_dir is not on your PATH."
    dim "  add this to your shell profile:"
    printf '\n    export PATH="%s:$PATH"\n\n' "$bin_dir"
    ;;
esac
