#!/bin/sh
# Render every manifest from a made-up SHA256SUMS and check that each one
# parses and carries the right checksum for the right archive.
#
#   scripts/check-packaging.sh
#
# Uses ruby, jq and bash when they are installed and skips a check when one is
# missing. Set PACKAGING_REQUIRE_TOOLS=1 (CI does) to fail instead of skipping.
set -eu

root="$(cd "$(dirname "$0")/.." && pwd)"
render="$root/scripts/render-packaging.sh"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT INT TERM

fail() { printf 'check-packaging: %s\n' "$*" >&2; exit 1; }
have() {
  command -v "$1" >/dev/null 2>&1 && return 0
  [ "${PACKAGING_REQUIRE_TOOLS:-0}" = 1 ] && fail "$1 is required"
  printf 'skip: %s not installed\n' "$1"
  return 1
}

version="$(sed -n 's/^version = "\(.*\)"$/\1/p' "$root/Cargo.toml" | head -n 1)"
[ -n "$version" ] || fail "no version in Cargo.toml"
tag="v$version"

# A distinct fake hash per archive: the index padded to 64 digits.
i=0
sh "$render" --list-assets "$tag" | while read -r asset; do
  i=$((i + 1))
  printf '%064d  %s\n' "$i" "$asset"
done > "$work/SHA256SUMS"
printf '%064d  rediscope-dependencies.spdx.json\n' 99 >> "$work/SHA256SUMS"

sh "$render" "$tag" "$work/SHA256SUMS" "$work/out"
out="$work/out"

sum_of() {
  awk -v n="rediscope-$tag-$1" '$2 == n {print $1}' "$work/SHA256SUMS"
}
expect() {
  grep -qF "$2" "$1" || fail "$1 does not contain $2"
}

# Each archive's hash has to sit right after its own URL, not just somewhere
# in the file.
pair_check() {
  file="$1"; asset="$2"; hash="$3"
  awk -v a="$asset" -v h="$hash" '
    index($0, a) { armed = 1; next }
    armed && index($0, h) { found = 1; exit }
    armed && /(url|Url|hash|sha256|Sha256)/ { exit }
    END { exit found ? 0 : 1 }
  ' "$file" || fail "$file: $asset is not followed by its checksum"
}

# ---- Homebrew --------------------------------------------------------------
f="$out/homebrew/rediscope.rb"
for t in aarch64-apple-darwin x86_64-apple-darwin aarch64-unknown-linux-musl x86_64-unknown-linux-musl; do
  pair_check "$f" "rediscope-$tag-$t.tar.gz" "$(sum_of "$t.tar.gz")"
done
expect "$f" "version \"$version\""
if have ruby; then
  ruby -c "$f" >/dev/null || fail "$f is not valid Ruby"
  echo "ok: homebrew formula parses"
fi

# ---- Scoop -----------------------------------------------------------------
f="$out/scoop/rediscope.json"
if have jq; then
  jq -e --arg v "$version" \
    --arg x64 "$(sum_of x86_64-pc-windows-msvc.zip)" \
    --arg arm "$(sum_of aarch64-pc-windows-msvc.zip)" '
      .version == $v
      and .architecture."64bit".hash == $x64
      and (.architecture."64bit".url | endswith("x86_64-pc-windows-msvc.zip"))
      and .architecture.arm64.hash == $arm
      and (.architecture.arm64.url | endswith("aarch64-pc-windows-msvc.zip"))
      and .bin == "rediscope.exe"
      and (.autoupdate.hash.url | endswith("/SHA256SUMS"))
    ' "$f" >/dev/null || fail "$f has the wrong shape or checksums"
  echo "ok: scoop manifest parses and matches"
fi

# ---- winget ----------------------------------------------------------------
f="$out/winget/TarasKovalenko.Rediscope.installer.yaml"
upper() { printf '%s' "$1" | tr 'a-f' 'A-F'; }
pair_check "$f" "rediscope-$tag-x86_64-pc-windows-msvc.zip" "$(upper "$(sum_of x86_64-pc-windows-msvc.zip)")"
pair_check "$f" "rediscope-$tag-aarch64-pc-windows-msvc.zip" "$(upper "$(sum_of aarch64-pc-windows-msvc.zip)")"
if have ruby; then
  for y in "$out"/winget/*.yaml; do
    ruby -ryaml -e '
      doc = YAML.safe_load(File.read(ARGV[0]))
      abort "#{ARGV[0]}: wrong identifier" unless doc["PackageIdentifier"] == "TarasKovalenko.Rediscope"
      abort "#{ARGV[0]}: wrong version" unless doc["PackageVersion"] == ARGV[1]
      abort "#{ARGV[0]}: wrong manifest version" unless doc["ManifestVersion"] == "1.6.0"
    ' "$y" "$version" || fail "$y is not a valid manifest"
  done
  echo "ok: winget manifests parse"
fi

# ---- AUR -------------------------------------------------------------------
f="$out/aur/rediscope-bin/PKGBUILD"
if have bash; then
  bash -n "$f" || fail "$f is not valid bash"
  # Source it in a subshell and compare the arrays makepkg would read.
  bash -c '
    set -eu
    . "$1"
    [ "$pkgver" = "$2" ] || { echo "pkgver $pkgver" >&2; exit 1; }
    [ "${sha256sums_x86_64[0]}" = "$3" ] || { echo "x86_64 checksum" >&2; exit 1; }
    [ "${sha256sums_aarch64[0]}" = "$4" ] || { echo "aarch64 checksum" >&2; exit 1; }
    case "${source_x86_64[0]}" in *x86_64-unknown-linux-gnu.tar.gz) ;; *) exit 1 ;; esac
    case "${source_aarch64[0]}" in *aarch64-unknown-linux-gnu.tar.gz) ;; *) exit 1 ;; esac
    [ "${#sha256sums[0]}" = 64 ]
  ' _ "$f" "$(printf '%s' "$version" | tr '-' '_')" \
    "$(sum_of x86_64-unknown-linux-gnu.tar.gz)" "$(sum_of aarch64-unknown-linux-gnu.tar.gz)" \
    || fail "$f has the wrong version, sources or checksums"
  echo "ok: PKGBUILD parses and matches"
fi

echo "packaging manifests for $tag look right"
