# Security policy

## Supported versions

Security fixes target the latest published release. Older releases do not receive
backports. There is no long-term-support branch or guaranteed response SLA.
Upgrade to the latest release before investigating a suspected fixed issue.

## Reporting a vulnerability

Use GitHub's private vulnerability reporting page when it is available:
https://github.com/TarasKovalenko/Rediscope/security/advisories/new

If private reporting is unavailable, open an issue asking the maintainer for a
private contact channel, without exploit details, credentials, keys, or data.
Do not post sensitive reports in public issues. Include the Rediscope version,
OS, Redis version/topology, minimal reproduction against disposable data, and
expected versus actual behavior in the private report. Remove credentials and
customer data from all attachments.

## Security boundaries

Rediscope is a local client running with the user's permissions. Production
leases and typed confirmations protect against mistakes; Redis ACLs enforce
access. Local audit files can be changed by the same OS user. Collect them into
your organization's secured logging service when you need independent retention
or attribution. No telemetry or automatic remote log upload is enabled.

The conflict-safe editor requires EVAL and the underlying read/write commands.
Member renames require Redis 7+ for ACL preflight. Explicit raw commands, imports,
and Lua scripts are intentional operations and do not inherit editor conflict
checks. An unknown write outcome must be investigated before retrying.

## Release verification

New release workflows produce signed GitHub build provenance and an SPDX
inventory of the locked Cargo dependencies, including dependencies for other
platforms. This inventory is not a claim that every listed crate is in every
binary, or that system libraries/toolchains are fully inventoried.

Installers require matching SHA-256 checksums and verify provenance using GitHub
CLI, constrained to this repository's release workflow and the requested tag.
A release that carries an attestation which fails to verify is refused outright,
under every setting of `REDISCOPE_VERIFY_PROVENANCE`. The default, `auto`, also
covers the cases where there is nothing to check or nothing to check it with —
a release published before provenance existed, a missing GitHub CLI, an
unauthenticated CLI, or an unreachable GitHub. Those warn and ask at the
terminal, and where no terminal exists they warn and continue on the checksum
alone; `REDISCOPE_VERIFY_PROVENANCE=1` makes them fatal, `0` skips the check
entirely. Distinguishing them relies on the GitHub CLI's reported error, so
treat `auto` as a convenience and `1` as the setting for an environment that
must not install an unattested binary. No release is signed until its workflow
actually runs successfully.

The release workflow audits Cargo.lock before building. A separate security
workflow checks pull requests, main, and weekly RustSec updates. GitHub Actions
are pinned to commits and Dependabot proposes dependency/action updates.
