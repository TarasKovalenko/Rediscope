//! The non-interactive half: subcommands that print to stdout instead of
//! opening the TUI, so the same binary can be used from a script or a CI job.

use anyhow::{Context, Result};

use crate::config::{Connection, Store};
use crate::memory::{Rollup, human_bytes};
use crate::redis_client::{Client, KEY_LIMIT, MemoryScan};
use crate::transfer::{self, Format};

/// Resolve which server to talk to: an explicit `--profile`, otherwise the
/// profile the connection flags describe.
pub fn resolve(profile: Option<&str>, flags: Option<Connection>) -> Result<Connection> {
    if let Some(name) = profile {
        let (store, _) = Store::load();
        return store
            .connections
            .iter()
            .find(|c| c.name == name)
            .cloned()
            .with_context(|| format!("no saved profile called '{name}'"));
    }
    flags.context("name a server with --host, --socket or --url, or a saved profile with --profile")
}

/// `rediscope keys` — the keyspace as one line per key.
pub async fn keys(conn: Connection, pattern: &str, json: bool) -> Result<()> {
    let client = Client::connect(conn).await?;
    let report = client.scan_report(pattern, KEY_LIMIT).await?;
    for warning in &report.warnings {
        eprintln!("warning: PARTIAL RESULTS: {warning}");
    }
    let (keys, truncated) = (report.keys, report.truncated);
    if json {
        let rows: Vec<serde_json::Value> = keys
            .iter()
            .map(|k| serde_json::json!({ "key": k.name, "type": k.kind.name(), "ttl": k.ttl }))
            .collect();
        println!("{}", serde_json::to_string_pretty(&rows)?);
    } else {
        for k in &keys {
            let ttl = if k.ttl < 0 {
                "-".to_string()
            } else {
                format!("{}s", k.ttl)
            };
            println!("{:<10} {:>8}  {}", k.kind.name(), ttl, k.name);
        }
    }
    if truncated {
        eprintln!("warning: stopped at {KEY_LIMIT} keys — narrow the pattern");
    }
    Ok(())
}

/// `rediscope export` — the keys matching a pattern, in any export format.
pub async fn export(
    conn: Connection,
    pattern: &str,
    out: &str,
    format: Format,
    replace: bool,
) -> Result<()> {
    anyhow::ensure!(
        !replace || format == Format::Commands,
        "--replace only applies to --format commands, where it writes DEL before each key"
    );
    let client = Client::connect(conn).await?;
    let report = client.scan_report(pattern, KEY_LIMIT).await?;
    for warning in &report.warnings {
        eprintln!("warning: PARTIAL RESULTS: {warning}");
    }
    let (keys, truncated) = (report.keys, report.truncated);
    let names: Vec<String> = keys.into_iter().map(|k| k.name).collect();
    let exported = if out == "-" {
        let stdout = std::io::BufWriter::new(std::io::stdout());
        client.export_to(&names, format, replace, stdout).await?.0
    } else {
        // Written beside the file and renamed over it once complete, so a
        // failed export leaves an existing file untouched.
        let (pending, file) =
            transfer::PendingFile::create(out).with_context(|| format!("cannot write {out}"))?;
        let (report, writer) = client
            .export_to(&names, format, replace, std::io::BufWriter::new(file))
            .await?;
        drop(writer);
        pending
            .commit()
            .with_context(|| format!("cannot write {out}"))?;
        eprintln!(
            "exported {} key(s) to {out} as {}",
            report.written,
            format.label()
        );
        report
    };
    for skipped in &exported.skipped {
        eprintln!("warning: skipped {skipped}");
    }
    if truncated {
        eprintln!("warning: stopped at {KEY_LIMIT} keys — narrow the pattern");
    }
    Ok(())
}

/// `rediscope import` — write an export back to a server.
pub async fn import(conn: Connection, file: &str, replace: bool) -> Result<()> {
    import_confirmed(conn, file, replace, None, None).await
}

/// `rediscope import` with the production flags. The file's format is read
/// from its content: DUMP payloads, JSON, JSON Lines, CSV or commands.
pub async fn import_confirmed(
    conn: Connection,
    file: &str,
    replace: bool,
    unlock: Option<&str>,
    confirmation: Option<&str>,
) -> Result<()> {
    let client = Client::connect(conn).await?;
    if client.production() {
        anyhow::ensure!(
            confirmation == Some(client.conn.name.as_str()),
            "Production import requires --confirm-production with the exact profile name"
        );
        client.unlock_writes(unlock.unwrap_or_default())?;
    } else {
        anyhow::ensure!(
            unlock.is_none() && confirmation.is_none(),
            "Production confirmation flags require a production profile"
        );
    }
    if client.read_only() {
        anyhow::bail!("'{}' is a read-only profile", client.conn.name);
    }
    let bytes = std::fs::read(file).with_context(|| format!("cannot read {file}"))?;
    let parsed = transfer::parse(&bytes).context("not a rediscope export")?;
    if replace && parsed.format() == Format::Commands {
        eprintln!("warning: --replace does not apply to a commands file; it runs as written");
    }
    let report = client.import_parsed(&parsed, replace).await?;
    eprintln!("{}", transfer::import_summary(parsed.format(), &report));
    Ok(())
}

/// `rediscope info` — the `INFO` reply, raw or as JSON sections.
pub async fn info(conn: Connection, json: bool) -> Result<()> {
    let client = Client::connect(conn).await?;
    let info = client.info().await?;
    if json {
        let sections: serde_json::Map<String, serde_json::Value> = info
            .sections
            .iter()
            .map(|s| {
                let fields: serde_json::Map<String, serde_json::Value> = s
                    .fields
                    .iter()
                    .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                    .collect();
                (s.name.clone(), serde_json::Value::Object(fields))
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&sections)?);
    } else {
        print!("{}", info.raw);
    }
    Ok(())
}

/// `rediscope mem-report` — the namespace memory report, run to completion.
pub async fn mem_report(conn: Connection, depth: usize, json: bool) -> Result<()> {
    let client = Client::connect(conn).await?;
    let dbsize = client.dbsize().await.unwrap_or(0);
    // The same sampling stride the TUI uses, so both give the same answer.
    let stride = (dbsize / 20_000).max(1);
    let mut scan = MemoryScan::default();
    let mut rollup = Rollup::with_separator(client.conn.key_separator());
    while !client.memory_batch(&mut scan, stride, &mut rollup).await? {}

    let rows = rollup.rows(depth.clamp(1, crate::memory::DEPTH_MAX));
    if json {
        let out = serde_json::json!({
            "scanned": rollup.scanned(),
            "sampled": rollup.sampled(),
            "estimated_bytes": rollup.total_bytes(),
            "prefixes": rows
                .iter()
                .map(|r| serde_json::json!({
                    "prefix": r.prefix,
                    "keys": r.keys,
                    "estimated_bytes": r.est_bytes,
                    "share_percent": r.share,
                }))
                .collect::<Vec<_>>(),
            "biggest_keys": rollup
                .top_keys()
                .iter()
                .map(|k| serde_json::json!({
                    "key": k.key,
                    "bytes": k.bytes,
                    "freq": k.freq,
                }))
                .collect::<Vec<_>>(),
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
        return Ok(());
    }
    println!(
        "{} keys, {} measured, {} estimated in total",
        rollup.scanned(),
        rollup.sampled(),
        human_bytes(rollup.total_bytes())
    );
    for row in rows {
        println!(
            "{:<40} {:>10} {:>12} {:>6.1}%",
            row.prefix,
            row.keys,
            human_bytes(row.est_bytes),
            row.share
        );
    }
    Ok(())
}
