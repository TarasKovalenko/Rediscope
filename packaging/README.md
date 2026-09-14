# Package manager manifests

The files here are templates. `scripts/render-packaging.sh` fills in the
version and the checksums from a release's `SHA256SUMS`, and the release
workflow publishes the result after the GitHub release is up:

| Channel | Template | Published to | Secret |
|---|---|---|---|
| Homebrew | `homebrew/rediscope.rb` | `TarasKovalenko/homebrew-tap`, `Formula/rediscope.rb` | `HOMEBREW_TAP_TOKEN` |
| Scoop | `scoop/rediscope.json` | `TarasKovalenko/scoop-bucket`, `bucket/rediscope.json` | `SCOOP_BUCKET_TOKEN` |
| winget | `winget/*.yaml` | a pull request to `microsoft/winget-pkgs` | `WINGET_TOKEN` |
| AUR | `aur/rediscope-bin/PKGBUILD` | `rediscope-bin` on aur.archlinux.org | `AUR_SSH_PRIVATE_KEY` |
| crates.io | `Cargo.toml` | `rediscope` on crates.io | `CARGO_REGISTRY_TOKEN` |

A job whose secret is missing is skipped with a notice, and the release run
stays green. So you can set these up one at a time, or not at all. Tags with a
suffix, like `v1.0.0-rc.1`, never go to package managers.

To render the manifests yourself:

```sh
gh release download v0.13.0 --pattern SHA256SUMS --dir /tmp/rel
scripts/render-packaging.sh v0.13.0 /tmp/rel/SHA256SUMS dist/packaging
```

`scripts/check-packaging.sh` renders them from a made-up `SHA256SUMS` and
checks each one parses and points the right checksum at the right archive. CI
runs it on every pull request.

## One-time setup

All secrets go in the Rediscope repository under Settings, Secrets and
variables, Actions.

### Homebrew

1. Create a public repository named `homebrew-tap` under `TarasKovalenko`,
   with a README and an empty `Formula/` directory. The name has to start with
   `homebrew-` for `brew tap TarasKovalenko/tap` to find it.
2. Create a fine-grained personal access token limited to that repository,
   with Contents set to read and write.
3. Add it as `HOMEBREW_TAP_TOKEN`.

The next release commits `Formula/rediscope.rb`. Users then run
`brew install TarasKovalenko/tap/rediscope`. Linux users get the static musl
build.

### Scoop

1. Create a public repository named `scoop-bucket` with a `bucket/` directory.
   The Scoop bucket template on GitHub works, but an empty repository is enough.
2. Create a fine-grained token for that repository with Contents read and
   write, and add it as `SCOOP_BUCKET_TOKEN`.

The manifest has `checkver` and `autoupdate`, so Scoop's own tooling
(`checkver.ps1 -Update`, or the Excavator action if the bucket runs it) can
also bump it without this workflow.

### winget

winget-releaser can only update a package that is already in winget-pkgs, so
the first version goes in by hand:

1. Render the manifests for the current release (see above) and copy the three
   files from `dist/packaging/winget/` to
   `manifests/t/TarasKovalenko/Rediscope/<version>/` in a fork of
   `microsoft/winget-pkgs`.
2. Check them with `winget validate --manifest <that directory>` and, on a
   Windows machine, `winget install --manifest <that directory>`.
3. Open the pull request and wait for it to be merged. `wingetcreate submit`
   does steps 1 and 3 for you if you prefer.
4. Fork `microsoft/winget-pkgs` under `TarasKovalenko` if it isn't already.
   The action pushes its branches there.
5. Create a classic personal access token with the `public_repo` scope, which
   is what winget-releaser asks for, and add it as `WINGET_TOKEN`. It opens
   pull requests on a repository you don't own, so a fine-grained token
   limited to your own repositories won't do.

Add the secret only after step 3. Before that the job fails, because the
package doesn't exist yet.

### AUR

1. Create an account on aur.archlinux.org and add an SSH public key to it.
   Use a key made just for this.
2. Check the package name is free: https://aur.archlinux.org/packages/rediscope-bin
   should be a 404. The first push creates the package.
3. Add the private key as `AUR_SSH_PRIVATE_KEY`.
4. Optional: set the repository variables `AUR_COMMIT_USERNAME` and
   `AUR_COMMIT_EMAIL` so the AUR commits carry your name. They default to the
   github-actions bot.

The action builds the package in an Arch container before pushing, which
downloads the archive and checks it against `sha256sums`. It writes `.SRCINFO`
too. If you ever push by hand, regenerate that file on Arch first with
`makepkg --printsrcinfo > .SRCINFO`, since the AUR rejects a push without it.

The PKGBUILD uses the glibc builds, which is what Arch ships. It also fetches
`LICENSE` from the tag, and its checksum comes from the checkout the script
runs in, so render from the tag you are releasing.

### crates.io

`cargo publish --dry-run` passes. `Cargo.toml` excludes the site, screenshots,
packaging files and install scripts, which leaves the sources, tests, README and
license.

1. Log in to crates.io with GitHub and create an API token with the
   `publish-new` and `publish-update` scopes and the crate pattern set to
   `rediscope`.
2. Add it as `CARGO_REGISTRY_TOKEN`.

The job checks that the tag matches the version in `Cargo.toml` before
publishing. A version on crates.io can be yanked but never replaced, so a rerun
of the same tag fails at that step, which is expected.

## Flathub

Not included. Flathub wants a desktop application with an AppStream file and a
build from source inside its sandbox, and a terminal client doesn't fit that
well. `cargo install`, Homebrew on Linux and the AUR cover the same users.
