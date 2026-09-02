# rediscope

A terminal UI Redis client. Browse the keyspace as a folder tree, read and edit
every value type, manage TTLs, and drop into a raw command console - in one
static binary, no Electron and no Python runtime.

<img width="1721" height="1035" alt="image" src="https://github.com/user-attachments/assets/17b807bb-5ae3-452b-a879-06c9b5b828d9" />

## Install

```sh
curl -fsSL https://raw.githubusercontent.com/TarasKovalenko/Rediscope/main/install.sh | sh
```

The script detects your platform, downloads the matching prebuilt binary from
GitHub Releases, verifies its SHA-256 against the published `SHA256SUMS`, and
installs to `~/.local/bin` (`/usr/local/bin` when run as root).

Pin a version or change the location:

```sh
REDISCOPE_VERSION=v0.1.0 REDISCOPE_BIN_DIR=/usr/local/bin \
  curl -fsSL https://raw.githubusercontent.com/TarasKovalenko/Rediscope/main/install.sh | sh
```

If you'd rather not pipe a script into a shell, download the tarball for your
platform from the [releases page](https://github.com/TarasKovalenko/Rediscope/releases)
and drop the binary on your `PATH`. To build from source instead:

```sh
cargo install --git https://github.com/TarasKovalenko/Rediscope
```

Prebuilt targets: macOS `aarch64` / `x86_64`, Linux `x86_64` and `aarch64`
(both glibc and musl).

## Run

```sh
rediscope                                  # start at the saved-server list
rediscope -H 127.0.0.1 -p 6379 -n 0        # connect immediately
rediscope --url rediss://user@host:6380/2  # or via a URL
```

Pass the password through `REDISCOPE_PASSWORD` rather than `-a`: a flag is
visible to anyone who can run `ps`.

## Features

- **Namespace tree** — keys grouped by `:` into collapsible folders, with a
  per-folder key count and a type badge on every leaf.
- **Safe listing** — `SCAN` in batches, never `KEYS *`, capped at 5,000 keys per
  view. The header tells you when a result was truncated.
- **Bounded value reads** — collections are read through `HSCAN`/`SSCAN` or a
  ranged `LRANGE`/`ZRANGE`, up to 1,000 elements, while still reporting the true
  total ("showing 1000 of 4.2M"). A million-element list will not stall the UI.
- **Edit everything** — strings in a multi-line editor with JSON pretty-printing;
  individual hash fields, list items, set members, sorted-set members and stream
  entries added, edited and deleted in place.
- **TTL management** — view, set, or drop the expiry on any key.
- **Raw command console** (`:`) with history, and a confirmation prompt in front
  of `FLUSHALL`, `FLUSHDB`, `SHUTDOWN` and friends.
- **Saved connections**, stored `0600` in your config dir, with TLS and ACL
  username support. A password of `${SOME_ENV_VAR}` is resolved from the
  environment at connect time, so the file need never hold a literal secret.
- **Non-blocking** — every Redis call runs off the render loop, so the interface
  stays responsive against slow or distant servers.

## Keybindings

Press `?` in the app for this list at any time.

### Server list
| Key | Action |
|---|---|
| `↑` `↓` / `k` `j` | Move |
| `Enter` | Connect |
| `n` / `e` / `d` | New / edit / delete connection |
| `q` | Quit |

### Key browser
| Key | Action |
|---|---|
| `j` `k` `↑` `↓` | Move · `g` / `G` jump to top / bottom |
| `h` `l` `←` `→` | Collapse / expand folder |
| `Enter` | Toggle a folder, or jump into the value pane |
| `Tab` | Switch between the key tree and the value pane |
| `/` | Search by pattern — a bare word becomes `*word*` |
| `Esc` | Clear the search pattern |
| `n` `D` `R` | New key · delete key · rename key |
| `t` | Set or clear TTL |
| `y` | Copy the selected key name to the clipboard (OSC 52) |
| `r` | Refresh |
| `e` | Edit — a string opens the editor, a row opens a form |
| `a` | Add an element to a hash / list / set / zset / stream |
| `x` | Delete the selected element |
| `:` | Raw command console |
| `Ctrl+D` | Switch database |
| `Ctrl+N` | Back to the server list |
| `?` / `q` | Help / quit |

Dialogs: `Esc` cancels, `Enter` confirms, `Ctrl+S` saves in the multi-line
editor, `Tab` moves between fields, `Space` toggles a switch.

## Configuration

Saved connections live in `connections.json` under your platform config dir —
`~/.config/rediscope` on Linux, `~/Library/Application Support/rediscope` on
macOS. Override the directory with `REDISCOPE_HOME`, or print the exact path:

```sh
rediscope --config-path
```

## Development

```sh
cargo test                                    # unit + render tests
redis-server --port 7799 --daemonize yes      # for the integration suite
REDISCOPE_TEST_PORT=7799 cargo test           # exercises a real server
cargo clippy --all-targets -- -D warnings
```

`src/redis_client.rs` is the only module that talks to Redis; `src/app.rs` holds
all state and key handling; `src/ui.rs` only draws. The render tests in
`tests/render.rs` drive the app through synthetic key events and redraw at sizes
down to 10×5, which is what keeps the layout arithmetic honest.

## Releasing

Tag and push — `.github/workflows/release.yml` cross-builds every target,
publishes the tarballs plus `SHA256SUMS`, and that is what `install.sh` reads.

```sh
git tag v0.1.0 && git push origin v0.1.0
```

## Notes

- The console runs whatever you type, exactly like `redis-cli`. The confirmation
  prompt covers the obvious footguns, not every possible one.
- Deleting a list item uses the standard `LSET` + `LREM` sentinel swap, since
  Redis has no delete-by-index. The sentinel is unique per call, so it cannot
  collide with real data.
- Switching database reconnects rather than issuing a bare `SELECT`: the
  connection is multiplexed, and a `SELECT` on it would affect commands that are
  already in flight.

## License

MIT
