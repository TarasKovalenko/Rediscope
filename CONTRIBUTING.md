# Contributing to rediscope

Thanks for looking. Bug reports, small fixes and new features are all welcome.
This page covers what a change needs before it can be merged.

## Before you start

For a bug, open an issue with the version, your OS and terminal, the server
(Redis, Valkey, KeyDB or Dragonfly, and its version) and the steps that show it.
For a feature, open an issue first and describe what you want to do with it.
Some ideas clash with the safety rules around production profiles, and it's
better to find that out before the code is written.

Security problems don't go in issues. See [SECURITY.md](SECURITY.md).

## Setting up

You need Rust 1.88 or later. A local `redis-server` is optional but lets you run
the integration suites.

```sh
git clone https://github.com/TarasKovalenko/Rediscope
cd Rediscope
cargo run
```

rediscope keeps its profiles in your normal config directory. While you work on
it, point `REDISCOPE_HOME` at a scratch directory so tests and experiments never
touch your real servers:

```sh
export REDISCOPE_HOME="$(mktemp -d)"
```

## Checks

CI runs these on Linux, macOS and Windows, and a pull request needs all of them
green:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

A bare `cargo test` always works, because every suite that needs a live server
skips itself when its environment variables are missing. To run those too:

```sh
redis-server --port 7799 --daemonize yes --save '' --appendonly no
REDISCOPE_TEST_PORT=7799 cargo test --all-targets
```

The TLS suite needs two more instances and a certificate set. The exact
commands are in `.github/workflows/ci.yml`. The compat job also runs the live
suites against Valkey, KeyDB and Dragonfly; set `REDISCOPE_TEST_FLAVOR` to try
one locally. The [Development](README.md#development) section of the README has
the rest.

## Writing the change

- `src/redis_client.rs` is the only module that talks to Redis, `src/app.rs`
  holds state and key handling, and `src/ui.rs` only draws. Keep it that way.
- New behaviour comes with tests. Screens and key handling are tested in
  `tests/render.rs` style: synthetic key events, then a render at several
  sizes, down to 10×5.
- Nothing that writes may skip the production lock, the typed confirmations or
  the compare-before-write check.
- A new key binding goes in the help screen, the `Ctrl+P` palette and the
  README keybinding tables. A test fails if help and the palette disagree.
- Existing `connections.json` files must keep loading and saving unchanged.
- If the layout changes, regenerate the screenshots with
  `cargo run --example screenshots` against a local Redis 8.

## Commits and pull requests

Commit subjects follow [Conventional Commits](https://www.conventionalcommits.org):
`feat:`, `fix:`, `docs:`, `chore:`, `test:`. The body says why, not what the
diff already shows.

Keep a pull request to one change. Say what it does, how you tested it, and
anything you weren't sure about. The template will ask for this.

Don't bump the version in a pull request. That happens at release time.

## Code of conduct

Everyone taking part is expected to follow the
[code of conduct](CODE_OF_CONDUCT.md).
