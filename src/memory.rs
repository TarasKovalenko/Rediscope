//! Namespace memory rollup: which key prefix is holding the RAM.
//!
//! Measuring every key on a large server is not an option, so the scan counts
//! every key it sees and calls `MEMORY USAGE` on an evenly spaced sample. Each
//! prefix is then estimated from its own sample. The report always shows how
//! much of the keyspace was actually measured, because an extrapolation that
//! hides its basis is worse than no number at all.

use std::collections::HashMap;

/// Keys are bucketed this deep and rolled up for shallower views, so changing
/// the depth on screen never costs another scan.
pub const DEPTH_MAX: usize = 3;

/// A `user:1234:profile` scheme would otherwise allocate a bucket per user.
/// Prefixes past this limit are counted together under [`OTHER`].
pub const MAX_PREFIXES: usize = 5_000;

/// Label for everything that arrived after the bucket limit.
pub const OTHER: &str = "(other prefixes)";

#[derive(Debug, Default, Clone, Copy)]
struct Bucket {
    keys: u64,
    sampled: u64,
    bytes: u64,
}

impl Bucket {
    fn mean(self) -> Option<f64> {
        (self.sampled > 0).then(|| self.bytes as f64 / self.sampled as f64)
    }
}

/// One line of the report.
#[derive(Debug, Clone, PartialEq)]
pub struct PrefixRow {
    pub prefix: String,
    pub keys: u64,
    pub est_bytes: u64,
    /// Percentage of the estimated total.
    pub share: f64,
}

#[derive(Debug, Default, Clone)]
pub struct Rollup {
    buckets: HashMap<String, Bucket>,
    other: Bucket,
    scanned: u64,
    sampled: u64,
    bytes: u64,
}

impl Rollup {
    /// Record that `key` exists.
    pub fn count(&mut self, key: &str) {
        self.scanned += 1;
        let prefix = prefix_of(key, DEPTH_MAX);
        if let Some(bucket) = self.buckets.get_mut(&prefix) {
            bucket.keys += 1;
        } else if self.buckets.len() < MAX_PREFIXES {
            self.buckets.insert(
                prefix,
                Bucket {
                    keys: 1,
                    ..Bucket::default()
                },
            );
        } else {
            self.other.keys += 1;
        }
    }

    /// Record that `key`, already counted, measured `bytes`.
    pub fn measure(&mut self, key: &str, bytes: u64) {
        self.sampled += 1;
        self.bytes += bytes;
        let prefix = prefix_of(key, DEPTH_MAX);
        let bucket = match self.buckets.get_mut(&prefix) {
            Some(bucket) => bucket,
            None => &mut self.other,
        };
        bucket.sampled += 1;
        bucket.bytes += bytes;
    }

    pub fn scanned(&self) -> u64 {
        self.scanned
    }

    pub fn sampled(&self) -> u64 {
        self.sampled
    }

    /// Every prefix at `depth`, biggest estimate first.
    pub fn rows(&self, depth: usize) -> Vec<PrefixRow> {
        // The overall mean stands in for prefixes the sample never reached.
        let overall = if self.sampled > 0 {
            self.bytes as f64 / self.sampled as f64
        } else {
            0.0
        };

        let mut merged: HashMap<String, Bucket> = HashMap::new();
        for (prefix, bucket) in &self.buckets {
            let entry = merged.entry(prefix_of(prefix, depth)).or_default();
            entry.keys += bucket.keys;
            entry.sampled += bucket.sampled;
            entry.bytes += bucket.bytes;
        }
        if self.other.keys > 0 {
            merged.insert(OTHER.to_string(), self.other);
        }

        let mut rows: Vec<PrefixRow> = merged
            .into_iter()
            .map(|(prefix, bucket)| {
                let mean = bucket.mean().unwrap_or(overall);
                PrefixRow {
                    prefix,
                    keys: bucket.keys,
                    est_bytes: (bucket.keys as f64 * mean).round() as u64,
                    share: 0.0,
                }
            })
            .collect();

        let total: u64 = rows.iter().map(|r| r.est_bytes).sum();
        if total > 0 {
            for row in &mut rows {
                row.share = row.est_bytes as f64 * 100.0 / total as f64;
            }
        }
        rows.sort_by(|a, b| {
            b.est_bytes
                .cmp(&a.est_bytes)
                .then_with(|| a.prefix.cmp(&b.prefix))
        });
        rows
    }

    /// Estimated size of the whole keyspace.
    pub fn total_bytes(&self) -> u64 {
        self.rows(1).iter().map(|r| r.est_bytes).sum()
    }
}

/// The first `depth` colon-separated segments of `key`, keeping the trailing
/// colon so a prefix reads as one. A key with fewer segments is its own
/// prefix, since there is nothing left to group it with.
fn prefix_of(key: &str, depth: usize) -> String {
    let mut out = String::with_capacity(key.len());
    for (i, segment) in key.split(':').enumerate() {
        if i == depth {
            // Truncated, so the trailing colon says "and everything below".
            out.push(':');
            return out;
        }
        if i > 0 {
            out.push(':');
        }
        out.push_str(segment);
    }
    // Fewer segments than asked for: the whole key, unchanged.
    out
}

/// Bytes as a person would say them: three significant-ish digits and a unit.
pub fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["KB", "MB", "GB", "TB", "PB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64 / 1024.0;
    for unit in UNITS {
        if value < 1024.0 {
            return format!("{value:.1} {unit}");
        }
        value /= 1024.0;
    }
    format!("{value:.1} EB")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Count `keys` under `prefix:n`, measuring the first `sampled` of them at
    /// `bytes` each.
    fn seed(r: &mut Rollup, prefix: &str, keys: u64, sampled: u64, bytes: u64) {
        for i in 0..keys {
            let key = format!("{prefix}:{i}");
            r.count(&key);
            if i < sampled {
                r.measure(&key, bytes);
            }
        }
    }

    #[test]
    fn keys_group_under_their_first_segment() {
        let mut r = Rollup::default();
        seed(&mut r, "session:web", 4, 4, 100);
        seed(&mut r, "cache:page", 2, 2, 50);
        let rows = r.rows(1);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].prefix, "session:");
        assert_eq!(rows[0].keys, 4);
        assert_eq!(rows[1].prefix, "cache:");
    }

    #[test]
    fn a_deeper_view_splits_the_same_scan_without_rescanning() {
        let mut r = Rollup::default();
        seed(&mut r, "session:web", 2, 2, 100);
        seed(&mut r, "session:api", 2, 2, 100);
        assert_eq!(r.rows(1).len(), 1, "one prefix at depth 1");
        let deep = r.rows(2);
        assert_eq!(deep.len(), 2);
        assert!(deep.iter().any(|row| row.prefix == "session:web:"));
    }

    #[test]
    fn a_key_without_a_separator_is_its_own_prefix() {
        let mut r = Rollup::default();
        r.count("mykey");
        r.measure("mykey", 64);
        assert_eq!(r.rows(1)[0].prefix, "mykey");
    }

    #[test]
    fn the_sample_is_scaled_up_to_the_whole_prefix() {
        let mut r = Rollup::default();
        // 100 keys, 4 of them measured at 50 bytes: 100 * 50.
        seed(&mut r, "session:web", 100, 4, 50);
        assert_eq!(r.rows(1)[0].est_bytes, 5_000);
    }

    #[test]
    fn a_prefix_the_sample_missed_falls_back_to_the_overall_average() {
        let mut r = Rollup::default();
        seed(&mut r, "session:web", 10, 10, 200);
        // Never sampled, so its own mean is unknown.
        seed(&mut r, "cache:page", 5, 0, 0);
        let cache = r
            .rows(1)
            .into_iter()
            .find(|row| row.prefix == "cache:")
            .unwrap();
        assert_eq!(cache.est_bytes, 1_000, "5 keys at the 200-byte average");
    }

    #[test]
    fn the_biggest_prefix_comes_first_and_the_shares_add_up() {
        let mut r = Rollup::default();
        seed(&mut r, "small:a", 1, 1, 100);
        seed(&mut r, "big:a", 3, 3, 100);
        let rows = r.rows(1);
        assert_eq!(rows[0].prefix, "big:");
        assert!((rows[0].share - 75.0).abs() < 0.01, "{}", rows[0].share);
        assert!((rows[1].share - 25.0).abs() < 0.01, "{}", rows[1].share);
    }

    #[test]
    fn a_scheme_with_a_prefix_per_key_does_not_grow_without_limit() {
        let mut r = Rollup::default();
        for i in 0..(MAX_PREFIXES + 100) {
            let key = format!("user:{i}:profile");
            r.count(&key);
            r.measure(&key, 10);
        }
        assert!(r.rows(3).len() <= MAX_PREFIXES + 1, "plus the overflow row");
        let other = r.rows(3).into_iter().find(|row| row.prefix == OTHER);
        assert_eq!(other.unwrap().keys, 100);
    }

    #[test]
    fn totals_track_what_was_scanned_and_what_was_measured() {
        let mut r = Rollup::default();
        seed(&mut r, "a:x", 10, 3, 100);
        assert_eq!(r.scanned(), 10);
        assert_eq!(r.sampled(), 3);
    }

    #[test]
    fn sizes_read_the_way_a_person_says_them() {
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(999), "999 B");
        assert_eq!(human_bytes(1_024), "1.0 KB");
        assert_eq!(human_bytes(1_536), "1.5 KB");
        assert_eq!(human_bytes(5 * 1024 * 1024), "5.0 MB");
        assert_eq!(human_bytes(3 * 1024 * 1024 * 1024), "3.0 GB");
    }
}
