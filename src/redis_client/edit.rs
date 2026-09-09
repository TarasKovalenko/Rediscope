//! Compare the value the user saw and apply an edit in one Redis operation.
use super::*;

#[derive(Clone, Debug)]
pub struct EditTarget {
    pub key: String,
    pub kind: KeyType,
    /// Hash field, list index, or set/sorted-set member; empty for documents.
    pub selector: String,
    pub original: String,
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
else return redis.error_reply('Unsupported edit type') end
local equal = current == expected
if kind == 'zset' and current then equal = tonumber(current) == tonumber(expected) end
if not current or (force ~= '1' and not equal) then return {0, current or false} end
if kind == 'string' then redis.call('SET', k, value, 'KEEPTTL')
elseif kind == 'ReJSON-RL' then redis.call('JSON.SET', k, '$', value)
elseif kind == 'hash' then redis.call('HSET', k, selector, value)
elseif kind == 'list' then redis.call('LSET', k, selector, value)
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
        let value = values.first().context("Missing edit value")?;
        let score = values.get(1).map(String::as_str).unwrap_or("");
        if target.kind == KeyType::ZSet {
            let score: f64 = score.parse().context("Invalid sorted-set score")?;
            anyhow::ensure!(!score.is_nan(), "Score cannot be NaN");
        }
        if target.kind == KeyType::Json
            || (target.kind == KeyType::String && crate::json::mode(&target.original).is_json())
        {
            crate::json::check(value).map_err(anyhow::Error::msg)?;
        }
        let kind = if target.kind == KeyType::Json {
            "ReJSON-RL"
        } else {
            target.kind.name()
        };
        let mut c = self.mgr.clone();
        let (saved, current): (i64, Option<String>) = redis::cmd("EVAL")
            .arg(SAVE)
            .arg(1)
            .arg(&target.key)
            .arg(kind)
            .arg(&target.selector)
            .arg(&target.original)
            .arg(value)
            .arg(score)
            .arg(if overwrite { "1" } else { "0" })
            .query_async(&mut c)
            .await?;
        Ok(if saved == 1 {
            EditOutcome::Saved
        } else {
            EditOutcome::Conflict { current }
        })
    }
}
