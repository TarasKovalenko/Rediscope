#!/bin/sh
# rediscope installer.
#
#   curl -fsSL https://raw.githubusercontent.com/TarasKovalenko/Rediscope/main/install.sh | sh
#
# Environment:
#   REDISCOPE_VERSION   tag to install (default: latest release)
#   REDISCOPE_BIN_DIR   install directory (default: ~/.local/bin, or /usr/local/bin for root)
#   REDISCOPE_VERIFY_PROVENANCE  auto (default) verifies when the release is signed and
#                       gh is installed, and asks before installing one that is not;
#                       1 always requires it, 0 never checks it
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
  MINGW*|MSYS*|CYGWIN*|Windows_NT)
    die "on Windows use the PowerShell installer: irm https://raw.githubusercontent.com/$REPO/main/install.ps1 | iex" ;;
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

# ---- checksum and signed provenance --------------------------------------
download "$base/SHA256SUMS" "$tmp/SHA256SUMS" || die "SHA256SUMS is required"
expected="$(awk -v asset="$asset" '$2 == asset {print $1}' "$tmp/SHA256SUMS")"
[ "${#expected}" = 64 ] || die "missing or ambiguous checksum for $asset"
if command -v sha256sum >/dev/null 2>&1; then
  actual="$(sha256sum "$tmp/$asset" | awk '{print $1}')"
elif command -v shasum >/dev/null 2>&1; then
  actual="$(shasum -a 256 "$tmp/$asset" | awk '{print $1}')"
else
  die "sha256sum or shasum is required"
fi
[ "$actual" = "$expected" ] || die "checksum mismatch for $asset"
dim "  checksum verified"

# Verifying needs the GitHub CLI, and only releases built after provenance was
# introduced carry an attestation to check. `auto` therefore tells three cases
# apart: a signed release (verify, and refuse anything that does not match), an
# unsigned one (say so, and ask), and a missing gh (say so, and ask). A failed
# check on a release that *is* signed is always fatal, in every mode.
verify_provenance() {
  gh attestation verify "$tmp/$asset" --repo "$REPO" \
    --signer-workflow "$REPO/.github/workflows/release.yml" \
    --source-ref "refs/tags/$version" --deny-self-hosted-runners 2>"$tmp/gh.err"
}
# Distinguish "there is nothing to check, or nothing to check it with" from
# "the signature does not match", which is never waved through.
# An unsigned release answers the attestations endpoint with HTTP 404.
unsigned_release() {
  grep -qiE 'HTTP 404|no attestations found' "$tmp/gh.err"
}
cannot_verify() {
  unsigned_release || grep -qiE \
    'gh auth login|authentication|not logged|HTTP 401|HTTP 403|dial tcp|no such host|connection refused|timeout|i/o timeout' \
    "$tmp/gh.err"
}
reason_cannot_verify() {
  if unsigned_release; then
    echo "$version was published before rediscope signed its releases"
  else
    echo "cannot reach GitHub to check this release's build provenance"
  fi
}
# curl | sh leaves no usable stdin, so ask on the terminal itself. Opening it is
# the only reliable test: /dev/tty exists in places where it cannot be read.
has_tty() { { true </dev/tty; } 2>/dev/null; }
confirm() {
  printf '\033[1m%s [y/N] \033[0m' "$1" >/dev/tty
  read -r reply </dev/tty || return 1
  case "$reply" in y|Y|yes|YES) return 0 ;; *) return 1 ;; esac
}
unverified() {
  red "warning: $1"
  red "warning: only the SHA-256 checksum vouches for this download"
  if has_tty; then
    confirm "Install $BIN $version without verified build provenance?" || die "cancelled"
    dim "  continuing without provenance verification"
  else
    red "warning: no terminal to ask on; continuing with the checksum alone"
    dim "  set REDISCOPE_VERIFY_PROVENANCE=1 to make this fatal instead"
  fi
}
case "${REDISCOPE_VERIFY_PROVENANCE:-auto}" in
  auto)
    if ! command -v gh >/dev/null 2>&1; then
      unverified "cannot verify build provenance: the GitHub CLI (gh) is not installed"
    elif verify_provenance; then
      dim "  signed build provenance verified"
    elif cannot_verify; then
      unverified "$(reason_cannot_verify)"
    else
      cat "$tmp/gh.err" >&2
      die "release provenance verification failed; nothing installed"
    fi
    ;;
  1)
    need gh
    verify_provenance || {
      cat "$tmp/gh.err" >&2
      die "release provenance verification failed; nothing installed"
    }
    dim "  signed build provenance verified"
    ;;
  0) dim "  WARNING: provenance verification explicitly disabled" ;;
  *) die "REDISCOPE_VERIFY_PROVENANCE must be auto, 1 or 0" ;;
esac

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
