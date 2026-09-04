# rediscope

A terminal UI Redis client. Browse the keyspace as a folder tree, read and edit
every value type, watch TTLs count down, find out which prefix is eating your
RAM, and drop into a raw command console. One static binary, no Electron and no
Python runtime.

Main view             |  Connections view | Server Info
:-------------------------:|:-------------------------:|:-------------------------:
<img width="1721" height="1035" alt="image" src="https://github.com/user-attachments/assets/17b807bb-5ae3-452b-a879-06c9b5b828d9" />  |  <img width="1721" height="1035" alt="image" src="https://github.com/user-attachments/assets/a0e80e28-d436-4e67-a464-155dd8563b7d" /> | <img width="1721" height="1035" alt="image" src="https://github.com/user-attachments/assets/5d071494-37bc-44e5-b8d1-c32f691b2208" />

**Contents:** [Install](#install) · [Quick start](#quick-start) ·
[Features](#features) · [Keybindings](#keybindings) ·
[Command line](#command-line) · [Connections and secrets](#connections-and-secrets) ·
[Configuration](#configuration) · [Troubleshooting](#troubleshooting) ·
[Development](#development)

## Install

macOS and Linux:

```sh
curl -fsSL https://raw.githubusercontent.com/TarasKovalenko/Rediscope/main/install.sh | sh
```

Windows (PowerShell):

```powershell
irm https://raw.githubusercontent.com/TarasKovalenko/Rediscope/main/install.ps1 | iex
```

Either script detects your platform, downloads the matching prebuilt binary from
GitHub Releases, verifies its SHA-256 against the published `SHA256SUMS`, and
installs to `~/.local/bin` (`/usr/local/bin` when run as root), or on Windows to
`%LOCALAPPDATA%\Programs\rediscope\bin`, adding it to your user `PATH`.

Pin a version or change the location:

```sh
REDISCOPE_VERSION=v0.7.0 REDISCOPE_BIN_DIR=/usr/local/bin \
  curl -fsSL https://raw.githubusercontent.com/TarasKovalenko/Rediscope/main/install.sh | sh
```

```powershell
$env:REDISCOPE_VERSION = 'v0.7.0'
$env:REDISCOPE_BIN_DIR = 'C:\tools\bin'
irm https://raw.githubusercontent.com/TarasKovalenko/Rediscope/main/install.ps1 | iex
```

If you'd rather not pipe a script into a shell, download the archive for your
platform from the [releases page](https://github.com/TarasKovalenko/Rediscope/releases)
and drop the binary on your `PATH`. To build from source instead:

```sh
cargo install --git https://github.com/TarasKovalenko/Rediscope
```

Prebuilt targets: macOS `aarch64` / `x86_64`, Linux `x86_64` and `aarch64`
(both glibc and musl), Windows `x86_64` and `aarch64` (MSVC). Unix builds ship
as `.tar.gz`, Windows builds as `.zip`.

## Quick start

Connect to a local server and look around:

```sh
rediscope -H 127.0.0.1 -p 6379
```

Your first minute, in order:

1. The key tree fills on the left. Keys split on `:`, so `user:42:profile` sits
   under `user` → `42`. Move with `j` / `k`, open a folder with `Enter` or `l`.
2. Selecting a key loads its value on the right, with its type and TTL in the
   header. `Tab` moves focus into the value pane and back.
3. Press `/` and type `session` to filter. A bare word becomes `*session*`;
   write your own glob if you want something exact. `Esc` clears it.
4. Press `e` to edit. A string opens a full editor (`Ctrl+S` saves), a hash
   field or list item opens a small form.
5. Press `i` for server info, `M` to see which prefix holds the memory, `:` for
   a `redis-cli`-style console.
6. Press `?` at any time for the full key list, `q` to quit.

Run it with no arguments to start at the saved-server list instead, where `n`
adds a connection you can reuse:

```sh
rediscope
```

## Features

### Browsing

- **Namespace tree.** Keys grouped by `:` into collapsible folders, with a
  per-folder key count and a type badge on every leaf.
- **Safe listing.** `SCAN` in batches, never `KEYS *`, capped at 5,000 keys per
  view. The header says so when a result was truncated, so you know to narrow
  the pattern rather than trusting a short list.
- **Bounded value reads.** Collections are read through `HSCAN`/`SSCAN` or a
  ranged `LRANGE`/`ZRANGE`, up to 1,000 elements, while still reporting the true
  total ("showing 1000 of 4.2M"). A million-element list will not stall the UI.
- **Live TTLs.** Expiries count down in place, and a key leaves the tree the
  second it expires, so nothing stale sits in the view between scans.
- **Search.** `/` filters by glob against the server, not just what's on screen.
- **Search inside values.** `F` reads the values of the keys on screen and
  keeps the ones containing your text, case-insensitively, across every type —
  for when you know what is in the value but not what the key is called.
- **Marks and bulk actions.** `m` marks a key, or every key under a folder.
  `D` then deletes the marked set (pipelined `UNLINK`, not one round trip per
  key) and `t` sets or clears their TTLs together. `u` clears the marks.
- **Session memory.** Each profile remembers its database, search pattern, open
  folders and selected key, and reopens where you left it.

### Editing

- **All six types.** Create a string, hash, list, set, sorted set or stream with
  `n`, then add elements with `a`: a field and value for a hash, a value for a
  list (`RPUSH`), a member for a set (`SADD`), a member and score for a sorted
  set (`ZADD`), a field and value for a stream (`XADD *`).
- **Strings in a real editor.** Multi-line, with `Ctrl+S` to save and `Esc` to
  back out.
- **Rows edited in place.** `e` on a hash field, list item, set member,
  sorted-set member or stream entry opens a form; `x` deletes the selected one.
- **Rename, delete, TTL.** `R` renames, `D` deletes after a confirmation, `t`
  sets an expiry in seconds or clears it when left blank.
- **JSON values.** A string holding JSON is shown indented and syntax-coloured
  with a `json` badge. The editor opens it pretty-printed, `Ctrl+F` reformats,
  and `Ctrl+S` refuses to save a document that no longer parses. Key order is
  preserved, and a value stored on one line is written back minified.
- **Copy a key** (`C`). Anywhere: another name, another database, or another
  saved server. `DUMP` + `RESTORE` carries the type and the remaining TTL, so a
  sorted set arrives as a sorted set.
- **Export and import** (`w` / `I`). Write the marked keys — or everything on
  screen — to a JSON file of `DUMP` payloads, and restore them here or on
  another server, optionally overwriting what is already there.
- **RedisJSON and RedisTimeSeries.** A `ReJSON-RL` document opens in the JSON
  editor and saves through `JSON.SET`; a time series lists its samples, and `a`
  appends one.
- **Lua** (`L`). An editor, `ctrl+s` to `EVAL`. The marked keys arrive as
  `KEYS[1..]`, so a script says what it touches.
- **JSON and XML inside collections.** Structured elements of a hash, list, set,
  sorted set or stream get a formatted preview below the collection. `PgUp` /
  `PgDn` scrolls that preview while the arrow keys keep moving between elements.

### Diagnosing

- **Server info** (`i`). Ten tabs. The first four read `INFO`: Server (version, uptime, clients
  and key count up top), Memory (a used / `maxmemory` bar), Stats, Key
  Statistics (per-db key and TTL counts, a hit-rate bar, expirations and
  evictions). The rest come from a single diagnostics fetch alongside it:
  **Slowlog** (slowest first, `x` resets it), **Clients** (`CLIENT LIST` sorted
  by idle time, `x` disconnects the selected one), **Config** (every running
  parameter, `e` edits one through `CONFIG SET`), **Latency** (a live ping
  sample plus `LATENCY LATEST`), **Cluster** (`CLUSTER INFO` and the loaded
  modules), and the full raw reply. `/` filters the open section, `y` copies
  it, `r` re-reads. Anything a managed provider refuses simply says so in its
  tab.
- **Namespace memory** (`M`). Which key prefix is holding the RAM. A background
  `SCAN` counts every key and measures an evenly spaced sample with
  `MEMORY USAGE`, so a multi-million-key server answers in seconds instead of
  hours. The table fills in as the scan runs. Prefixes are ranked by estimated
  size with a share bar, `1` `2` `3` regroup by one, two or three name segments
  without rescanning, and the header always says how much of the keyspace the
  estimate is based on. Past 5,000 distinct prefixes the tail is pooled into
  `(other prefixes)` so a `user:<id>` scheme cannot explode the table. `t`
  switches the same scan to the biggest individual keys it measured, with the
  `OBJECT FREQ` counter beside each one where the server keeps one.
- **Pub/Sub** (`P`). Subscribe to channel patterns and watch messages arrive,
  `w` publishes one, `f` follows the tail, `y` copies the feed.
- **Keyspace events** (`N`). The same feed pointed at
  `__keyevent@<db>__:*`, so you can watch keys being written, expired and
  evicted live. Needs `notify-keyspace-events` set on the server.
- **Consumer groups** (`S`, on a stream). Every group with its pending count
  and lag, the consumers behind it, and the entries none of them have acked.
  `n` creates a group, `d` destroys one, `a` acks an entry and `c` claims one
  for another consumer — enough to unstick a queue whose worker died.
- **Search** (`Q`). Pick a RediSearch index and run a query; the reply opens in
  a scrollable pane.
- **Raw command console** (`:`). Anything `redis-cli` takes, with history that
  survives a restart (500 commands), `Ctrl+R` reverse search, and `Tab`
  completion of command names and of keys already on screen. `FLUSHALL`,
  `FLUSHDB`, `SHUTDOWN`, `DEBUG`, `SCRIPT`, `RESET` and `SWAPDB` ask first.
  Commands carrying a password (`AUTH`, `HELLO ... AUTH`,
  `CONFIG SET requirepass`) are never written to the history file.

### Connecting

- **Connection manager.** Add, edit, duplicate (`c`), reorder (`J`/`K`), filter
  (`/`), and test (`T`) saved servers. A test reports round-trip latency, the
  server version and its key count without opening the connection.
- **TLS.** A private CA, mutual TLS with a client certificate and key, or an
  explicit skip-verify for a self-signed dev server. Profiles show `TLS`,
  `no-verify` and `keychain` badges in the list.
- **Password handling.** Profiles are stored `0600` in your config dir (on
  Windows, under `%APPDATA%`, which is already per-user). A password of
  `${SOME_ENV_VAR}` is resolved from the environment at connect time, or the
  profile can keep its secret in the OS keychain (macOS Keychain, Windows
  Credential Manager, freedesktop Secret Service) so the file holds no secret at
  all.
- **Read-only profiles.** A profile marked read-only refuses every write —
  edits, deletes, TTLs, bulk actions, imports, and the writing commands in the
  console, which are identified from the server's own command table rather than
  a guess. The title bar says `READ-ONLY` while such a session is open.
- **SSH tunnels.** Give a profile a jump host and rediscope runs
  `ssh -N -L …` for the life of the connection, then connects through the local
  port. It uses your system ssh, so your agent, `~/.ssh/config` and
  `known_hosts` all apply; the tunnel dies with the connection.
- **Database switching.** `Ctrl+D` picks another index and reconnects.
- **ACL usernames.** Redis 6+ `user` / `password` pairs, per profile or via
  `-u`.

### Comfort

- **Colour themes** (`p`). Preview and choose Redis, Dracula, Catppuccin Mocha,
  Nord, Gruvbox Dark, or Tokyo Night. Arrow keys preview live, `Enter` saves the
  choice for the next run, `Esc` puts the old one back.
- **Clipboard over OSC 52.** `y` copies the key name, the open info tab or the
  memory report straight through SSH and tmux, with no system clipboard tool
  installed.
- **Non-blocking.** Every Redis call runs off the render loop, so the interface
  stays responsive against slow or distant servers.
- **Tiny terminals.** The layout is tested down to 10×5, so a split pane still
  renders something usable.

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
| `p` | Preview and choose a colour theme |
| `?` / `q` | Help / quit |

### Key browser
| Key | Action |
|---|---|
| `j` `k` `↑` `↓` | Move · `PgUp` `PgDn` jump 10 · `g` `G` (`Home` `End`) top / bottom |
| `h` `l` `←` `→` | Collapse / expand folder |
| `Enter` / `Space` | Toggle a folder, or jump into the value pane |
| `Tab` | Switch between the key tree and the value pane |
| `/` | Search by pattern. A bare word becomes `*word*` |
| `Esc` | Clear the search pattern |
| `n` `D` `R` | New key · delete key · rename key |
| `t` | Set or clear TTL |
| `y` | Copy the selected key name to the clipboard |
| `m` / `u` | Mark the key or folder · clear every mark |
| `F` | Find keys whose value contains some text |
| `C` | Copy the key to another name, database or server |
| `w` / `I` | Export the marked keys to a file · import a file back |
| `L` | Run a Lua script (marked keys become `KEYS[1..]`) |
| `r` | Refresh keys and the open value |
| `e` | Edit. A string opens the editor, a row opens a form |
| `a` | Add an element to a hash / list / set / zset / stream |
| `x` | Delete the selected element |
| `PgUp` `PgDn` | Scroll the selected JSON or XML preview |
| `i` | Server info |
| `M` | Namespace memory report |
| `P` / `N` | Pub/sub feed · keyspace event feed |
| `S` | Consumer groups of the selected stream |
| `Q` | Run a RediSearch query |
| `p` | Colour theme picker |
| `:` | Raw command console |
| `Ctrl+D` | Switch database (reconnects) |
| `Ctrl+N` | Back to the server list |
| `?` / `q` | Help / quit |

### Server info (`i`)
| Key | Action |
|---|---|
| `Tab` `←` `→` `h` `l` / `1`-`9` `0` | Change section |
| `↑` `↓` `j` `k` `PgUp` `PgDn` `g` `G` | Scroll |
| `/` | Filter the open section |
| `e` | Edit the selected parameter (Config tab) |
| `x` | Disconnect the selected client, or reset the slow log |
| `y` | Copy the open tab |
| `r` | Re-read everything |
| `Esc` / `q` | Clear the filter, then close |

### Namespace memory (`M`)
| Key | Action |
|---|---|
| `1` `2` `3` | Group by one, two or three name segments |
| `t` | Switch between prefixes and the biggest individual keys |
| `↑` `↓` `j` `k` / `g` | Scroll · back to the top |
| `r` | Rescan |
| `y` | Copy the report |
| `Esc` / `q` | Cancel the scan and close |

### Pub/Sub and keyspace events (`P` / `N`)
| Key | Action |
|---|---|
| `s` | Change what the feed is subscribed to |
| `w` | Publish a message |
| `f` | Follow the newest message · `↑` `↓` `PgUp` `PgDn` scroll back |
| `c` / `y` | Clear the feed · copy it |
| `Esc` / `q` | Stop the subscription and close |

### Consumer groups (`S`)
| Key | Action |
|---|---|
| `↑` `↓` `j` `k` | Move · `Tab` switches between groups and pending entries |
| `n` / `d` | Create a group · destroy the selected one |
| `a` / `c` | Ack the selected pending entry · claim it for another consumer |
| `r` | Refresh · `Esc` closes |

### Console (`:`)
| Key | Action |
|---|---|
| `Enter` | Run the command |
| `↑` `↓` | Walk the history |
| `Ctrl+R` | Reverse search. Again steps further back, `Enter` accepts, `Esc` restores the line you were typing |
| `Tab` | Complete a command name, or after it a key name from the tree |
| `Esc` | Close the console |

### Editor and dialogs
| Key | Action |
|---|---|
| `Ctrl+S` | Save (validates JSON first) |
| `Ctrl+F` | Reformat JSON |
| `Tab` / `↑` `↓` | Move between form fields |
| `Space` | Toggle a switch · `←` `→` picks a choice |
| `Enter` | Confirm |
| `Esc` | Cancel |

### Theme picker (`p`)
`↑` `↓` (or `j` `k`) previews a theme immediately, `Enter` saves it, `Esc`
restores the previous one.

## Command line

```sh
rediscope                                  # start at the saved-server list
rediscope -H 127.0.0.1 -p 6379 -n 0        # connect immediately
rediscope --url rediss://user@host:6380/2  # or via a URL

# TLS against a private CA, and mutual TLS
rediscope -H cache.internal --tls-ca ~/certs/ca.pem
rediscope -H cache.internal --tls-cert ~/certs/client.crt --tls-key ~/certs/client.key
```

| Flag | Meaning |
|---|---|
| `-H`, `--host` | Redis host. Given, rediscope connects straight away and skips the server list |
| `-p`, `--port` | Port (default `6379`) |
| `-n`, `--db` | Database index (default `0`) |
| `-u`, `--username` | ACL username (Redis 6+) |
| `-a`, `--password` | Password. Prefer `REDISCOPE_PASSWORD` |
| `--url` | `redis://` or `rediss://` URL. Overrides the other flags |
| `--tls` | Connect over TLS |
| `--tls-ca FILE` | PEM root certificate for a private CA |
| `--tls-cert FILE` | PEM client certificate (needs `--tls-key`) |
| `--tls-key FILE` | PEM client key (needs `--tls-cert`) |
| `--tls-insecure` | Accept any server certificate. Dev servers only |
| `--profile NAME` | Open a saved profile directly, by name |
| `--read-only` | Refuse every write for this session |
| `--ssh HOST` | Reach the server through `ssh -L` on this jump host |
| `--ssh-user`, `--ssh-port`, `--ssh-key` | Details for `--ssh` |
| `--config-path` | Print the connections file path and exit |
| `-V`, `--version` | Version |

Naming any certificate implies `--tls`, so you rarely need the flag itself.

### Scripting

The same binary answers without opening the TUI, so it can be used from a
script or a CI job. Every subcommand takes the connection flags above, or
`--profile` to reuse a saved one:

```sh
rediscope --profile prod keys --pattern 'session:*' --json
rediscope --profile prod info --json | jq .Memory.used_memory
rediscope --profile prod mem-report --depth 2 --json
rediscope --profile prod export --pattern 'user:*' --out users.json
rediscope -H localhost import --file users.json --replace
```

| Subcommand | What it prints |
|---|---|
| `keys` | One line per key: type, TTL and name. `--json` for objects |
| `info` | The raw `INFO` reply, or `--json` for sections as objects |
| `mem-report` | The namespace estimate, including the biggest keys under `--json` |
| `export` | `DUMP` payloads and TTLs as JSON, to `--out` or stdout |
| `import` | Restores such a file; `--replace` overwrites existing keys |

A read-only profile refuses `import`, the same as it does in the UI.

| Environment variable | Meaning |
|---|---|
| `REDISCOPE_PASSWORD` | Password, instead of `-a`. A flag is visible to anyone who can run `ps` |
| `REDISCOPE_HOME` | Config directory, overriding the platform default |

## Connections and secrets

A connection profile holds the server address, database index, optional ACL
username, TLS settings, a read-only switch, an optional SSH jump host, and how
to find its password. The editor is one form with `Server`, `Authentication`,
`TLS` and `SSH tunnel` sections; `Tab` moves between fields, `Space` toggles a
switch, and the form scrolls when the terminal is short.

Passwords resolve in one of three ways:

| Setting | Where the secret lives |
|---|---|
| A literal password | In `connections.json`, mode `0600` (Windows: `%APPDATA%` ACL) |
| `${SOME_ENV_VAR}` | In your environment, read at connect time |
| Keychain switch on | In the OS keychain, never in the file |

With the keychain switch on, leaving the password field blank keeps whatever is
already stored, and renaming a profile migrates its entry. Switching it off
removes the entry. If no keychain is available, say a headless Linux box with no
Secret Service, the form says so and refuses the switch rather than losing the
password silently.

## Configuration

Saved connections live in `connections.json` under your platform config dir, and
the console keeps its history beside it in `history` (mode `0600`, 500 commands).
That's `~/.config/rediscope` on Linux, `~/Library/Application Support/rediscope`
on macOS, `%APPDATA%\rediscope` on Windows. Override the directory with
`REDISCOPE_HOME`, or print the exact path:

```sh
rediscope --config-path
```

The same file keeps your theme, so the colours come back on the next run, and
one entry per profile recording where you left it — database, search pattern,
open folders and selected key.

The file is written atomically: a scratch file renamed over the old one, with
the previous version kept as `connections.json.bak`, so an interrupted save
cannot truncate it. A file that exists but does not parse is moved aside as
`connections.json.bad-<timestamp>` rather than replaced, and a file that cannot
be read at all disables saving for the session instead of overwriting profiles
that are still on disk.

## Troubleshooting

**The interface renders as garbage on Windows.** Use
[Windows Terminal](https://aka.ms/terminal), or any ConPTY-based terminal on
Windows 10 1809+. The legacy `conhost` console window cannot draw it.

**`y` copies nothing.** Copying uses the OSC 52 escape, which your terminal has
to allow. iTerm2, WezTerm, Kitty, Alacritty and Windows Terminal do by default;
tmux needs `set -g allow-passthrough on`, and some terminals hide the setting
under "allow clipboard access".

**The key list looks short.** A view is capped at 5,000 keys and the header says
when it was truncated. Narrow it with `/`.

**A collection shows fewer elements than it has.** Reads stop at 1,000 elements
on purpose; the header carries the real total.

**The memory report says it sampled a small share.** That's the honest basis for
the estimate on a big keyspace, not an error. Let it run longer, or read the
numbers as the ranking they are.

**Writes are refused with "this connection is read-only".** The profile has its
read-only switch on (the title bar says `READ-ONLY`). Turn it off in the profile
editor, or connect without `--read-only`.

**The SSH tunnel times out.** rediscope runs the system `ssh` in batch mode, so
it never waits at a password prompt. Check that `ssh <host>` works on its own,
with a key your agent already holds.

**The keyspace feed stays empty.** Redis publishes those events only when
`notify-keyspace-events` is configured — `CONFIG SET notify-keyspace-events KEA`
turns everything on for a test.

**Keys on a cluster come back as `MOVED` errors.** rediscope talks to the node
you point it at; it does not follow slot redirects yet. Point it at the node
that owns the keys, or use the Cluster tab of the info pane (`i`) to see the
topology.

**The keychain switch refuses to turn on.** No Secret Service is running, which
is normal on a headless Linux box. Use `${SOME_ENV_VAR}` for that profile.

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

Tag and push. `.github/workflows/release.yml` cross-builds every target,
publishes the tarballs and Windows zips plus `SHA256SUMS`, and that is what
`install.sh` and `install.ps1` read.

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
