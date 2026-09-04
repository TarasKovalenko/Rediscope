//! The non-interactive half: subcommands that print to stdout instead of
//! opening the TUI, so the same binary can be used from a script or a CI job.

use anyhow::{Context, Result};

use crate::config::{Connection, Store};
use crate::memory::{Rollup, human_bytes};
use crate::redis_client::{Client, ExportEntry, KEY_LIMIT, MemoryScan};

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
    flags.context("name a server with --host/--url, or a saved profile with --profile")
}

/// `rediscope keys` — the keyspace as one line per key.
pub async fn keys(conn: Connection, pattern: &str, json: bool) -> Result<()> {
    let client = Client::connect(conn).await?;
    let (keys, truncated) = client.scan_keys(pattern, KEY_LIMIT).await?;
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

/// `rediscope export` — DUMP payloads and TTLs, as the JSON the import reads.
pub async fn export(conn: Connection, pattern: &str, out: &str) -> Result<()> {
    let client = Client::connect(conn).await?;
    let (keys, truncated) = client.scan_keys(pattern, KEY_LIMIT).await?;
    let names: Vec<String> = keys.into_iter().map(|k| k.name).collect();
    let entries = client.export_keys(&names).await?;
    let text = serde_json::to_string_pretty(&entries)?;
    if out == "-" {
        println!("{text}");
    } else {
        std::fs::write(out, text).with_context(|| format!("cannot write {out}"))?;
        eprintln!("exported {} key(s) to {out}", entries.len());
    }
    if truncated {
        eprintln!("warning: stopped at {KEY_LIMIT} keys — narrow the pattern");
    }
    Ok(())
}

/// `rediscope import` — write an export back to a server.
pub async fn import(conn: Connection, file: &str, replace: bool) -> Result<()> {
    let client = Client::connect(conn).await?;
    if client.read_only() {
        anyhow::bail!("'{}' is a read-only profile", client.conn.name);
    }
    let text = std::fs::read_to_string(file).with_context(|| format!("cannot read {file}"))?;
    let entries: Vec<ExportEntry> =
        serde_json::from_str(&text).context("not a rediscope export")?;
    let written = client.import_entries(&entries, replace).await?;
    eprintln!("imported {written} key(s)");
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
    let mut rollup = Rollup::default();
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
