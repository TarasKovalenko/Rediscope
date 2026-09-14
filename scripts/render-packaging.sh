#!/bin/sh
# Render the package manager manifests under packaging/ for one release.
#
#   scripts/render-packaging.sh VERSION SHA256SUMS [OUT_DIR]
#   scripts/render-packaging.sh --list-assets VERSION
#
# VERSION is the release tag, with or without the leading v (v0.13.0 or
# 0.13.0). SHA256SUMS is the file published with that release. OUT_DIR
# defaults to dist/packaging and gets:
#
#   homebrew/rediscope.rb
#   scoop/rediscope.json
#   winget/TarasKovalenko.Rediscope*.yaml
#   aur/rediscope-bin/PKGBUILD
#
# --list-assets prints the archive names the manifests point at, one per line.
# The release workflow and the CI check both call this script, so what gets
# published is what CI checked.
set -eu

die() { printf 'render-packaging: %s\n' "$*" >&2; exit 1; }

root="$(cd "$(dirname "$0")/.." && pwd)"
templates="$root/packaging"

targets="aarch64-apple-darwin x86_64-apple-darwin
aarch64-unknown-linux-musl x86_64-unknown-linux-musl
aarch64-unknown-linux-gnu x86_64-unknown-linux-gnu
aarch64-pc-windows-msvc x86_64-pc-windows-msvc"

asset_name() {
  case "$2" in
    *-windows-*) printf 'rediscope-%s-%s.zip' "$1" "$2" ;;
    *) printf 'rediscope-%s-%s.tar.gz' "$1" "$2" ;;
  esac
}

normalize_version() {
  version="${1#v}"
  printf '%s' "$version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+([.-][A-Za-z0-9.-]+)?$' \
    || die "not a release version: $1"
  tag="v$version"
}

if [ "${1:-}" = "--list-assets" ]; then
  [ $# -eq 2 ] || die "usage: $0 --list-assets VERSION"
  normalize_version "$2"
  for t in $targets; do
    asset_name "$tag" "$t"
    printf '\n'
  done
  exit 0
fi

[ $# -ge 2 ] && [ $# -le 3 ] || die "usage: $0 VERSION SHA256SUMS [OUT_DIR]"
normalize_version "$1"
sums="$2"
out="${3:-dist/packaging}"
[ -f "$sums" ] || die "no such file: $sums"

# Exactly one 64-digit line per archive, the same rule install.sh applies.
sha_of() {
  name="$(asset_name "$tag" "$1")"
  hash="$(awk -v n="$name" '$2 == n || $2 == "*" n {print $1}' "$sums")"
  [ "$(printf '%s\n' "$hash" | grep -c .)" -eq 1 ] \
    && printf '%s' "$hash" | grep -Eq '^[0-9a-fA-F]{64}$' \
    || die "missing or ambiguous checksum for $name in $sums"
  printf '%s' "$hash" | tr 'A-F' 'a-f'
}

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    die "sha256sum or shasum is required"
  fi
}

upper() { printf '%s' "$1" | tr 'a-f' 'A-F'; }

macos_arm64="$(sha_of aarch64-apple-darwin)"
macos_x64="$(sha_of x86_64-apple-darwin)"
linux_arm64_musl="$(sha_of aarch64-unknown-linux-musl)"
linux_x64_musl="$(sha_of x86_64-unknown-linux-musl)"
linux_arm64_gnu="$(sha_of aarch64-unknown-linux-gnu)"
linux_x64_gnu="$(sha_of x86_64-unknown-linux-gnu)"
windows_arm64="$(sha_of aarch64-pc-windows-msvc)"
windows_x64="$(sha_of x86_64-pc-windows-msvc)"
# The PKGBUILD downloads LICENSE from the tag, which matches this checkout
# when the release workflow runs.
license_sha="$(sha256_file "$root/LICENSE")"
# pacman does not allow a hyphen in pkgver.
pkgver="$(printf '%s' "$version" | tr '-' '_')"

render() {
  mkdir -p "$(dirname "$2")"
  sed \
    -e "s|@VERSION@|$version|g" \
    -e "s|@TAG@|$tag|g" \
    -e "s|@PKGVER@|$pkgver|g" \
    -e "s|@LICENSE_SHA256@|$license_sha|g" \
    -e "s|@SHA256_MACOS_ARM64@|$macos_arm64|g" \
    -e "s|@SHA256_MACOS_X64@|$macos_x64|g" \
    -e "s|@SHA256_LINUX_ARM64_MUSL@|$linux_arm64_musl|g" \
    -e "s|@SHA256_LINUX_X64_MUSL@|$linux_x64_musl|g" \
    -e "s|@SHA256_LINUX_ARM64_GNU@|$linux_arm64_gnu|g" \
    -e "s|@SHA256_LINUX_X64_GNU@|$linux_x64_gnu|g" \
    -e "s|@SHA256_WINDOWS_ARM64_UPPER@|$(upper "$windows_arm64")|g" \
    -e "s|@SHA256_WINDOWS_X64_UPPER@|$(upper "$windows_x64")|g" \
    -e "s|@SHA256_WINDOWS_ARM64@|$windows_arm64|g" \
    -e "s|@SHA256_WINDOWS_X64@|$windows_x64|g" \
    "$1" > "$2"
  if grep -n '@[A-Z0-9_]*@' "$2" >&2; then
    die "unfilled placeholder in $2"
  fi
}

render "$templates/homebrew/rediscope.rb" "$out/homebrew/rediscope.rb"
render "$templates/scoop/rediscope.json" "$out/scoop/rediscope.json"
for f in "$templates"/winget/*.yaml; do
  render "$f" "$out/winget/$(basename "$f")"
done
render "$templates/aur/rediscope-bin/PKGBUILD" "$out/aur/rediscope-bin/PKGBUILD"

printf 'rendered %s manifests into %s\n' "$tag" "$out"
