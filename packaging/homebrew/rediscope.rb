# Rendered from packaging/homebrew/rediscope.rb in TarasKovalenko/Rediscope by
# scripts/render-packaging.sh on each release. Change it there, not in the tap.
class Rediscope < Formula
  desc "Terminal UI client for Redis, Valkey, KeyDB and Dragonfly"
  homepage "https://github.com/TarasKovalenko/Rediscope"
  version "@VERSION@"
  license "MIT"

  on_macos do
    on_arm do
      url "https://github.com/TarasKovalenko/Rediscope/releases/download/@TAG@/rediscope-@TAG@-aarch64-apple-darwin.tar.gz"
      sha256 "@SHA256_MACOS_ARM64@"
    end
    on_intel do
      url "https://github.com/TarasKovalenko/Rediscope/releases/download/@TAG@/rediscope-@TAG@-x86_64-apple-darwin.tar.gz"
      sha256 "@SHA256_MACOS_X64@"
    end
  end

  # The musl builds are statically linked, so they don't care which glibc the
  # host has.
  on_linux do
    on_arm do
      url "https://github.com/TarasKovalenko/Rediscope/releases/download/@TAG@/rediscope-@TAG@-aarch64-unknown-linux-musl.tar.gz"
      sha256 "@SHA256_LINUX_ARM64_MUSL@"
    end
    on_intel do
      url "https://github.com/TarasKovalenko/Rediscope/releases/download/@TAG@/rediscope-@TAG@-x86_64-unknown-linux-musl.tar.gz"
      sha256 "@SHA256_LINUX_X64_MUSL@"
    end
  end

  def install
    bin.install "rediscope"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/rediscope --version")
  end
end
