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
REDISCOPE_VERSION=v0.2.0 REDISCOPE_BIN_DIR=/usr/local/bin \
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

# TLS against a private CA, and mutual TLS
rediscope -H cache.internal --tls-ca ~/certs/ca.pem
rediscope -H cache.internal --tls-cert ~/certs/client.crt --tls-key ~/certs/client.key
```

Naming any certificate implies `--tls`, so you rarely need the flag itself.

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
- **Server info** (`i`) — `INFO` in tabs: Server (version, uptime, clients and
  key count up top), Memory (a used / `maxmemory` bar), Stats, Key Statistics
  (per-db key and TTL counts, a hit-rate bar, expirations and evictions) and the
  full reply. `/` filters the open section, `y` copies it.
- **Raw command console** (`:`) with history, and a confirmation prompt in front
  of `FLUSHALL`, `FLUSHDB`, `SHUTDOWN` and friends.
- **Connection manager** — add, edit, duplicate (`c`), reorder (`J`/`K`), filter
  (`/`), and test (`T`) saved servers. A test reports round-trip latency, the
  server version and its key count without opening the connection.
- **TLS** — a private CA, mutual TLS with a client certificate and key, or an
  explicit skip-verify for a self-signed dev server. Profiles show `TLS`,
  `no-verify` and `keychain` badges in the list.
- **Password handling** — profiles are stored `0600` in your config dir. A
  password of `${SOME_ENV_VAR}` is resolved from the environment at connect
  time, or the profile can keep its secret in the OS keychain (macOS Keychain,
  Windows Credential Manager, freedesktop Secret Service) so the file holds no
  secret at all.
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
| `c` | Duplicate the selected connection |
| `J` / `K` | Move the connection down / up |
| `T` | Test the connection without opening it |
| `/` | Filter by name or host · `Esc` clears the filter |
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
| `i` | Server info — Server / Memory / Stats / Key Statistics / All, with `/` to filter |
| `:` | Raw command console |
| `Ctrl+D` | Switch database |
| `Ctrl+N` | Back to the server list |
| `?` / `q` | Help / quit |

In server info: `Tab` / `←` `→` / `1`-`5` change section, `/` filters it, `↑` `↓`
`PgUp` `PgDn` `g` `G` scroll, `y` copies the open tab, `r` re-reads `INFO`, `Esc`
clears the filter and then closes.

Dialogs: `Esc` cancels, `Enter` confirms, `Ctrl+S` saves in the multi-line
editor, `Tab` moves between fields, `Space` toggles a switch.

## Connections and secrets

A connection profile holds the server address, database index, optional ACL
username, TLS settings, and how to find its password. The editor is one form
with `Server`, `Authentication` and `TLS` sections; `Tab` moves between fields,
`Space` toggles a switch, and the form scrolls when the terminal is short.

Passwords resolve in one of three ways:

| Setting | Where the secret lives |
|---|---|
| A literal password | In `connections.json`, mode `0600` |
| `${SOME_ENV_VAR}` | In your environment, read at connect time |
| Keychain switch on | In the OS keychain, never in the file |

With the keychain switch on, leaving the password field blank keeps whatever is
already stored, and renaming a profile migrates its entry. Switching it off
removes the entry. If no keychain is available — a headless Linux box with no
Secret Service — the form says so and refuses the switch rather than losing the
password silently.

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

The TLS suite needs two more instances and a certificate set; `.github/workflows/ci.yml`
has the exact `openssl` and `redis-server` invocations. Point it at them with
`REDISCOPE_TLS_PORT`, `REDISCOPE_MTLS_PORT` and `REDISCOPE_CERTS`. Every suite
skips itself when its environment variables are absent, so a bare `cargo test`
always works.

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
- Skip-verify accepts any certificate the server presents, which defeats the
  point of TLS against anything but a local dev server. The form labels it
  unsafe and the list badges it `no-verify`.
- Switching database reconnects rather than issuing a bare `SELECT`: the
  connection is multiplexed, and a `SELECT` on it would affect commands that are
  already in flight.

## License

MIT
