//! Compare the value the user saw and apply an edit in one Redis operation.
use super::*;

#[derive(Clone, Debug)]
pub struct EditTarget {
    pub key: String,
    pub kind: KeyType,
    /// Hash field, list index, or set/sorted-set member; empty for documents.
    pub selector: String,
    /// What the user was shown: the stored text, or its decoded form.
    pub original: String,
    /// Set when the value was shown through a codec. The edit is encoded the
    /// same way, and the conflict check compares the exact stored bytes.
    pub decoded: Option<crate::codec::Decoding>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EditOutcome {
    Saved,
    Conflict { current: Option<String> },
}

// Arguments are kept out of audit events. No automatic retries of EVAL.
const SAVE: &str = r#"
local k, kind, selector, expected, value, score, force = KEYS[1], ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6]
local actualtype = redis.call('TYPE', k).ok
if actualtype ~= kind then return {0, false} end
local current
if kind == 'string' then current = redis.call('GET', k)
elseif kind == 'ReJSON-RL' then current = redis.call('JSON.GET', k, '.')
elseif kind == 'hash' then current = redis.call('HGET', k, selector)
elseif kind == 'list' then current = redis.call('LINDEX', k, selector)
elseif kind == 'set' then if redis.call('SISMEMBER', k, selector) == 1 then current = selector end
elseif kind == 'zset' then current = redis.call('ZSCORE', k, selector)
elseif kind == 'vectorset' then
    -- VGETATTR answers nil both for no attributes and for no element; VEMB
    -- tells them apart.
    if redis.call('VEMB', k, selector) then current = redis.call('VGETATTR', k, selector) or '' end
else return redis.error_reply('Unsupported edit type') end
local equal = current == expected
if kind == 'zset' and current then equal = tonumber(current) == tonumber(expected) end
if not current or (force ~= '1' and not equal) then return {0, current or false} end
if kind == 'string' then redis.call('SET', k, value, 'KEEPTTL')
elseif kind == 'ReJSON-RL' then redis.call('JSON.SET', k, '$', value)
elseif kind == 'hash' then redis.call('HSET', k, selector, value)
elseif kind == 'list' then redis.call('LSET', k, selector, value)
elseif kind == 'vectorset' then redis.call('VSETATTR', k, selector, value)
elseif kind == 'set' or kind == 'zset' then
    if selector ~= value then
        local exists
        if kind == 'set' then exists = redis.call('SISMEMBER', k, value) == 1
        else exists = redis.call('ZSCORE', k, value) ~= false end
        if exists then return redis.error_reply('Destination member already exists; choose another member name') end
        -- Validate both writes before either executes; Lua does not roll back.
        if not redis.acl_check_cmd then return redis.error_reply('Member rename requires Redis 7+ ACL preflight') end
        if kind == 'set' then
            if not redis.acl_check_cmd('SADD', k, value) or not redis.acl_check_cmd('SREM', k, selector) then return redis.error_reply('Member rename denied by ACL') end
        else
            if not redis.acl_check_cmd('ZADD', k, score, value) or not redis.acl_check_cmd('ZREM', k, selector) then return redis.error_reply('Member rename denied by ACL') end
        end
    end
    -- Add before removing the last old member so Redis retains the key and TTL.
    if kind == 'set' then
        redis.call('SADD', k, value)
        if selector ~= value then redis.call('SREM', k, selector) end
    else
        redis.call('ZADD', k, score, value)
        if selector ~= value then redis.call('ZREM', k, selector) end
    end
end
return {1, false}
"#;
impl Client {
    pub async fn save_edit(
        &self,
        target: &EditTarget,
        values: &[String],
        overwrite: bool,
    ) -> Result<EditOutcome> {
        anyhow::ensure!(
            !self.read_only(),
            "Read-only profile or production write lease expired"
        );
        // A binary value is shown as a hex dump. Saving one would store the
        // dump text over the bytes it describes, so the edit stops here.
        anyhow::ensure!(
            !crate::redis_client::is_hex_dump(&target.original),
            "This value is binary and is shown as a hex dump. Editing it would overwrite the bytes with their own description."
        );
        anyhow::ensure!(
            !crate::redis_client::is_hex_dump(&target.selector),
            "This element's name is binary and is shown as a hex dump; it cannot be edited by name."
        );
        let value = values.first().context("Missing edit value")?;
        let score = values.get(1).map(String::as_str).unwrap_or("");
        if target.kind == KeyType::ZSet {
            let score: f64 = score.parse().context("Invalid sorted-set score")?;
            anyhow::ensure!(!score.is_nan(), "Score cannot be NaN");
        }
        if target.kind == KeyType::Json
            || (target.kind == KeyType::VectorSet && !value.is_empty())
            || (target.kind == KeyType::String && crate::json::mode(&target.original).is_json())
        {
            crate::json::check(value).map_err(anyhow::Error::msg)?;
        }
        let kind = if target.kind == KeyType::Json {
            "ReJSON-RL"
        } else {
            target.kind.name()
        };
        // Through a codec, the draft is encoded and the check is made against
        // the bytes that were read, not the text they decoded to.
        let (expected, stored) = match &target.decoded {
            None => (
                target.original.as_bytes().to_vec(),
                value.as_bytes().to_vec(),
            ),
            Some(decoded) => {
                anyhow::ensure!(
                    matches!(target.kind, KeyType::String | KeyType::Hash | KeyType::List),
                    "Only string values, hash values and list items can be saved through a codec"
                );
                if let Some(reason) = &decoded.read_only {
                    anyhow::bail!(
                        "This value cannot be saved through {}: {reason}",
                        decoded.codec.name()
                    );
                }
                // Nothing edited: write back exactly what was read, rather than
                // re-encoding it into different bytes (a new gzip header, say).
                let stored = if *value == target.original {
                    decoded.raw.clone()
                } else {
                    let codec = decoded.codec.clone();
                    let text = value.clone();
                    let original = target.original.clone();
                    tokio::task::spawn_blocking(move || encode_checked(&codec, &text, &original))
                        .await
                        .context("encoding the value failed")??
                };
                (decoded.raw.clone(), stored)
            }
        };
        let mut c = self.mgr.clone();
        let (saved, current): (i64, Option<Vec<u8>>) = redis::cmd("EVAL")
            .arg(SAVE)
            .arg(1)
            .arg(decode_key(&target.key))
            .arg(kind)
            .arg(&target.selector)
            .arg(expected)
            .arg(stored)
            .arg(score)
            .arg(if overwrite { "1" } else { "0" })
            .query_async(&mut c)
            .await?;
        // What is stored now, in the same form the user was shown.
        let current = match (current, &target.decoded) {
            (None, _) => None,
            (Some(bytes), None) => Some(decode_value(bytes)),
            (Some(bytes), Some(decoded)) => {
                let view = View::Codec(decoded.codec.clone());
                let shown = tokio::task::spawn_blocking(move || crate::codec::show(bytes, &view))
                    .await
                    .context("decoding the current value failed")?;
                Some(match shown {
                    Shown::Plain(text) | Shown::Failed { text, .. } => text,
                    Shown::Decoded { text, .. } => text,
                })
            }
        };
        Ok(if saved == 1 {
            EditOutcome::Saved
        } else {
            EditOutcome::Conflict { current }
        })
    }
}

/// Encode an edit. A custom codec's two programs are trusted only as far as
/// they agree with each other: what `decode` reads back from the bytes about
/// to be stored must survive another `encode` and `decode` unchanged. The
/// check is on the text, not the bytes, so an encoder that never writes the
/// same bytes twice (encryption with a fresh salt, a timestamp) passes, and so
/// does a decoder that prints its own canonical form (a trailing newline,
/// reordered fields). A mismatched pair fails, and nothing is saved.
///
/// The edit itself must also survive: an encoder that ignores its input, or
/// always writes the same thing, would otherwise agree with itself while
/// storing something other than the draft. `original` is the decoded text the
/// edit started from.
fn encode_checked(codec: &crate::codec::Codec, text: &str, original: &str) -> Result<Vec<u8>> {
    let stored = crate::codec::encode(codec, text)?;
    if let crate::codec::Codec::Custom(custom) = codec {
        let name = &custom.name;
        let context = || format!("checking what '{name}' encoded");
        anyhow::ensure!(
            !stored.is_empty() || text.is_empty(),
            "the '{name}' codec's encode command printed nothing for a non-empty draft, so nothing was saved"
        );
        let (back, read_only) = crate::codec::decode(codec, &stored).with_context(context)?;
        if let Some(reason) = read_only {
            anyhow::bail!("reading back what '{name}' encoded failed: {reason}; nothing was saved");
        }
        // Compared with the original as the codec itself would store it, so an
        // encoder that writes the same bytes whatever it is given is caught
        // even when those bytes are not the original's.
        let original_again = crate::codec::encode(codec, original)
            .and_then(|bytes| crate::codec::decode(codec, &bytes))
            .with_context(context)?
            .0;
        anyhow::ensure!(
            back != original && back != original_again,
            "the '{name}' codec reads its encoding of this edit back the same as the original, so the edit would be lost; nothing was saved"
        );
        let again = crate::codec::encode(codec, &back).with_context(context)?;
        let (back_again, _) = crate::codec::decode(codec, &again).with_context(context)?;
        anyhow::ensure!(
            back_again == back,
            "the '{name}' codec's decode and encode commands do not agree: text read back from its own encoding changes when encoded and decoded again, so nothing was saved"
        );
    }
    Ok(stored)
}
