//! Value codecs against a real redis-server: compressed and packed values are
//! read decoded, edits are encoded the same way, and the conflict check still
//! compares the exact stored bytes.
//!
//! Skipped unless REDISCOPE_TEST_PORT is set:
//!   redis-server --port 7799 --daemonize yes
//!   REDISCOPE_TEST_PORT=7799 cargo test --test codecs

mod common;

use redis::AsyncCommands;
use rediscope::codec::{self, Builtin, Codec, View};
use rediscope::config::Connection;
use rediscope::redis_client::{Client, EditOutcome, EditTarget, KeyType, KeyValue};

fn port() -> Option<u16> {
    common::isolate_config();
    std::env::var("REDISCOPE_TEST_PORT").ok()?.parse().ok()
}

/// A rediscope client plus a plain redis connection for writing raw bytes,
/// both on database `db`.
async fn clients(db: i64) -> Option<(Client, redis::aio::MultiplexedConnection)> {
    let port = port()?;
    let client = Client::connect(Connection {
        name: "codecs".into(),
        host: "127.0.0.1".into(),
        port,
        db,
        ..Default::default()
    })
    .await
    .expect("connect");
    let raw = redis::Client::open(format!("redis://127.0.0.1:{port}/{db}"))
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    Some((client, raw))
}

macro_rules! clients {
    ($db:expr) => {
        match clients($db).await {
            Some(pair) => pair,
            None => return,
        }
    };
}

async fn clear(raw: &mut redis::aio::MultiplexedConnection, prefix: &str) {
    let keys: Vec<Vec<u8>> = raw.keys(format!("{prefix}*")).await.unwrap();
    for k in keys {
        let _: i64 = raw.del(k).await.unwrap();
    }
}

fn gzip() -> Codec {
    Codec::Builtin(Builtin::Gzip)
}

const DOC: &str = r#"{"user":"ada","plan":"pro"}"#;

#[tokio::test]
async fn plain_reads_are_unchanged_and_auto_decodes_gzip() {
    let (c, mut raw) = clients!(0);
    clear(&mut raw, "codec:auto:").await;
    let stored = codec::encode(&gzip(), DOC).unwrap();
    let _: () = raw.set("codec:auto:doc", &stored).await.unwrap();
    let _: () = raw.set("codec:auto:text", "hello").await.unwrap();

    // The long-standing read still shows the stored bytes as a hex dump.
    let KeyValue::Str(shown) = c
        .read_value("codec:auto:doc", KeyType::String)
        .await
        .unwrap()
    else {
        panic!("expected a plain string");
    };
    assert!(rediscope::redis_client::is_hex_dump(&shown), "{shown}");

    // Auto recognises the gzip header and shows the document.
    let (value, notice) = c
        .read_value_as("codec:auto:doc", KeyType::String, &View::Auto)
        .await
        .unwrap();
    assert_eq!(notice, None);
    let KeyValue::Decoded { text, decoding } = value else {
        panic!("expected a decoded string, got {value:?}");
    };
    assert_eq!(text, DOC);
    assert_eq!(decoding.codec, gzip());
    assert_eq!(decoding.raw, stored);

    // Text is never touched by auto.
    let (value, _) = c
        .read_value_as("codec:auto:text", KeyType::String, &View::Auto)
        .await
        .unwrap();
    assert!(
        matches!(value, KeyValue::Str(ref s) if s == "hello"),
        "{value:?}"
    );
    clear(&mut raw, "codec:auto:").await;
}

#[tokio::test]
async fn a_gzip_edit_is_stored_compressed_and_keeps_the_ttl() {
    let (c, mut raw) = clients!(0);
    clear(&mut raw, "codec:edit:").await;
    let key = "codec:edit:doc";
    let stored = codec::encode(&gzip(), DOC).unwrap();
    let _: () = raw.set_ex(key, &stored, 600).await.unwrap();

    let (KeyValue::Decoded { text, decoding }, _) = c
        .read_value_as(key, KeyType::String, &View::Auto)
        .await
        .unwrap()
    else {
        panic!("expected decoded");
    };
    let target = EditTarget {
        key: key.into(),
        kind: KeyType::String,
        selector: String::new(),
        original: text,
        decoded: Some(decoding),
    };
    let draft = r#"{"user":"ada","plan":"team"}"#;
    assert_eq!(
        c.save_edit(&target, &[draft.into()], false).await.unwrap(),
        EditOutcome::Saved
    );

    let now: Vec<u8> = raw.get(key).await.unwrap();
    assert_eq!(codec::detect(&now), Some(Builtin::Gzip), "still gzip");
    assert_eq!(codec::decode(&gzip(), &now).unwrap().0, draft);
    let ttl: i64 = raw.ttl(key).await.unwrap();
    assert!(ttl > 500, "TTL preserved, got {ttl}");

    // A save against the bytes read before the first edit is a conflict, and
    // the current value comes back decoded for the three-way view.
    match c.save_edit(&target, &["{}".into()], false).await.unwrap() {
        EditOutcome::Conflict { current } => assert_eq!(current.as_deref(), Some(draft)),
        other => panic!("expected a conflict, got {other:?}"),
    }
    let still: Vec<u8> = raw.get(key).await.unwrap();
    assert_eq!(still, now, "a conflict writes nothing");

    // Overwrite after the conflict goes through, still compressed.
    assert_eq!(
        c.save_edit(&target, &["{}".into()], true).await.unwrap(),
        EditOutcome::Saved
    );
    let after: Vec<u8> = raw.get(key).await.unwrap();
    assert_eq!(codec::decode(&gzip(), &after).unwrap().0, "{}");
    clear(&mut raw, "codec:edit:").await;
}

#[tokio::test]
async fn collection_elements_decode_and_hash_and_list_values_save_back() {
    let (c, mut raw) = clients!(0);
    clear(&mut raw, "codec:rows:").await;
    let zstd = Codec::Builtin(Builtin::Zstd);
    let packed = codec::encode(&Codec::Builtin(Builtin::MsgPack), r#"{"id":7}"#).unwrap();

    let _: () = raw
        .hset("codec:rows:h", "doc", codec::encode(&zstd, DOC).unwrap())
        .await
        .unwrap();
    let _: () = raw.hset("codec:rows:h", "plain", "text").await.unwrap();
    let _: () = raw
        .rpush("codec:rows:l", codec::encode(&gzip(), "first").unwrap())
        .await
        .unwrap();
    let _: () = raw.sadd("codec:rows:s", &packed).await.unwrap();
    let _: String = redis::cmd("XADD")
        .arg("codec:rows:x")
        .arg("*")
        .arg("payload")
        .arg(codec::encode(&gzip(), "event").unwrap())
        .query_async(&mut raw)
        .await
        .unwrap();

    // Hash: the compressed field decodes, the text one is untouched.
    let (KeyValue::Rows { rows, .. }, _) = c
        .read_value_as("codec:rows:h", KeyType::Hash, &View::Auto)
        .await
        .unwrap()
    else {
        panic!("expected rows");
    };
    let doc = rows.iter().find(|r| r.id == "doc").unwrap();
    assert_eq!(doc.cells, ["doc", DOC]);
    let decoding = doc.decoding.clone().expect("decoded");
    assert_eq!(decoding.codec, zstd);
    let plain = rows.iter().find(|r| r.id == "plain").unwrap();
    assert_eq!(plain.cells, ["plain", "text"]);
    assert!(plain.decoding.is_none());

    let target = EditTarget {
        key: "codec:rows:h".into(),
        kind: KeyType::Hash,
        selector: "doc".into(),
        original: DOC.into(),
        decoded: Some(decoding),
    };
    assert_eq!(
        c.save_edit(&target, &["changed".into()], false)
            .await
            .unwrap(),
        EditOutcome::Saved
    );
    let now: Vec<u8> = raw.hget("codec:rows:h", "doc").await.unwrap();
    assert_eq!(codec::decode(&zstd, &now).unwrap().0, "changed");

    // List item through gzip.
    let (KeyValue::Rows { rows, .. }, _) = c
        .read_value_as("codec:rows:l", KeyType::List, &View::Auto)
        .await
        .unwrap()
    else {
        panic!("expected rows");
    };
    assert_eq!(rows[0].cells, ["0", "first"]);
    let target = EditTarget {
        key: "codec:rows:l".into(),
        kind: KeyType::List,
        selector: "0".into(),
        original: "first".into(),
        decoded: rows[0].decoding.clone(),
    };
    assert_eq!(
        c.save_edit(&target, &["second".into()], false)
            .await
            .unwrap(),
        EditOutcome::Saved
    );
    let now: Vec<u8> = raw.lindex("codec:rows:l", 0).await.unwrap();
    assert_eq!(codec::decode(&gzip(), &now).unwrap().0, "second");

    // Set member shown as JSON, but members are addresses: not saved through
    // a codec.
    let (KeyValue::Rows { rows, .. }, _) = c
        .read_value_as("codec:rows:s", KeyType::Set, &View::Auto)
        .await
        .unwrap()
    else {
        panic!("expected rows");
    };
    assert_eq!(rows[0].cells, [r#"{"id":7}"#]);
    let target = EditTarget {
        key: "codec:rows:s".into(),
        kind: KeyType::Set,
        selector: rows[0].id.clone(),
        original: rows[0].id.clone(),
        decoded: rows[0].decoding.clone(),
    };
    assert!(c.save_edit(&target, &["x".into()], true).await.is_err());
    let members: Vec<Vec<u8>> = raw.smembers("codec:rows:s").await.unwrap();
    assert_eq!(members, [packed]);

    // Stream field values decode in place.
    let (KeyValue::Rows { rows, .. }, _) = c
        .read_value_as("codec:rows:x", KeyType::Stream, &View::Auto)
        .await
        .unwrap()
    else {
        panic!("expected rows");
    };
    assert_eq!(rows[0].cells[1], "payload=event");
    clear(&mut raw, "codec:rows:").await;
}

#[tokio::test]
async fn a_chosen_codec_that_does_not_fit_says_so_and_shows_the_value() {
    let (c, mut raw) = clients!(0);
    clear(&mut raw, "codec:fail:").await;
    let _: () = raw.set("codec:fail:s", "plain text").await.unwrap();
    let _: () = raw.rpush("codec:fail:l", &["a", "b"]).await.unwrap();

    let (value, notice) = c
        .read_value_as("codec:fail:s", KeyType::String, &View::Codec(gzip()))
        .await
        .unwrap();
    assert!(
        matches!(value, KeyValue::Str(ref s) if s == "plain text"),
        "{value:?}"
    );
    assert!(notice.unwrap().contains("gzip"));

    let (value, notice) = c
        .read_value_as("codec:fail:l", KeyType::List, &View::Codec(gzip()))
        .await
        .unwrap();
    let KeyValue::Rows { rows, .. } = value else {
        panic!("expected rows");
    };
    assert_eq!(rows[1].cells, ["1", "b"]);
    assert!(notice.unwrap().starts_with("2 element(s)"));
    clear(&mut raw, "codec:fail:").await;
}

#[tokio::test]
async fn hex_view_edits_binary_values_byte_for_byte() {
    let (c, mut raw) = clients!(0);
    clear(&mut raw, "codec:hex:").await;
    let key = "codec:hex:bin";
    let _: () = raw.set(key, &[0x80u8, 0xfe, 0x00, 0x41][..]).await.unwrap();

    let hex = View::Codec(Codec::Builtin(Builtin::Hex));
    let (KeyValue::Decoded { text, decoding }, _) =
        c.read_value_as(key, KeyType::String, &hex).await.unwrap()
    else {
        panic!("expected decoded");
    };
    assert_eq!(text, "80 fe 00 41");
    let target = EditTarget {
        key: key.into(),
        kind: KeyType::String,
        selector: String::new(),
        original: text,
        decoded: Some(decoding),
    };
    assert_eq!(
        c.save_edit(&target, &["ff 00\n01".into()], false)
            .await
            .unwrap(),
        EditOutcome::Saved
    );
    let now: Vec<u8> = raw.get(key).await.unwrap();
    assert_eq!(now, [0xff, 0x00, 0x01]);

    // Malformed hex never reaches the server.
    assert!(c.save_edit(&target, &["zz".into()], true).await.is_err());
    let still: Vec<u8> = raw.get(key).await.unwrap();
    assert_eq!(still, [0xff, 0x00, 0x01]);
    clear(&mut raw, "codec:hex:").await;
}

#[tokio::test]
async fn read_only_decodings_refuse_to_save() {
    let (c, mut raw) = clients!(0);
    clear(&mut raw, "codec:ro:").await;
    let key = "codec:ro:packed";
    // A float32 has no exact JSON form, so this cannot be saved back.
    let mut packed = Vec::new();
    rmpv::encode::write_value(
        &mut packed,
        &rmpv::Value::Map(vec![(rmpv::Value::from("f"), rmpv::Value::F32(1.5))]),
    )
    .unwrap();
    let _: () = raw.set(key, &packed).await.unwrap();

    let (KeyValue::Decoded { text, decoding }, _) = c
        .read_value_as(key, KeyType::String, &View::Auto)
        .await
        .unwrap()
    else {
        panic!("expected decoded");
    };
    assert!(decoding.read_only.is_some());
    let target = EditTarget {
        key: key.into(),
        kind: KeyType::String,
        selector: String::new(),
        original: text,
        decoded: Some(decoding),
    };
    assert!(
        c.save_edit(&target, &[r#"{"f":2}"#.into()], true)
            .await
            .is_err()
    );
    let still: Vec<u8> = raw.get(key).await.unwrap();
    assert_eq!(still, packed);
    clear(&mut raw, "codec:ro:").await;
}

#[tokio::test]
async fn value_search_looks_inside_compressed_values() {
    let (c, mut raw) = clients!(0);
    clear(&mut raw, "codec:grep:").await;
    let _: () = raw
        .set(
            "codec:grep:z",
            codec::encode(&gzip(), "needle in gzip").unwrap(),
        )
        .await
        .unwrap();
    let _: () = raw.set("codec:grep:t", "hay").await.unwrap();
    let (hits, _) = c.grep_values("codec:grep:*", "NEEDLE", 100).await.unwrap();
    assert_eq!(
        hits.iter().map(|k| k.name.as_str()).collect::<Vec<_>>(),
        ["codec:grep:z"]
    );
    clear(&mut raw, "codec:grep:").await;
}

#[cfg(unix)]
#[tokio::test]
async fn custom_codecs_view_and_save_through_their_programs() {
    let (c, mut raw) = clients!(0);
    clear(&mut raw, "codec:custom:").await;
    let key = "codec:custom:rot";
    let _: () = raw.set(key, "Uryyb").await.unwrap();
    let tr = vec![
        "sh".to_string(),
        "-c".into(),
        "tr 'A-Za-z' 'N-ZA-Mn-za-m'".into(),
    ];
    let view = View::Codec(Codec::Custom(codec::CustomCodec {
        name: "rot13".into(),
        decode: tr.clone(),
        encode: tr,
        timeout_secs: None,
    }));
    let (KeyValue::Decoded { text, decoding }, _) =
        c.read_value_as(key, KeyType::String, &view).await.unwrap()
    else {
        panic!("expected decoded");
    };
    assert_eq!(text, "Hello");
    let target = EditTarget {
        key: key.into(),
        kind: KeyType::String,
        selector: String::new(),
        original: text,
        decoded: Some(decoding),
    };
    assert_eq!(
        c.save_edit(&target, &["World".into()], false)
            .await
            .unwrap(),
        EditOutcome::Saved
    );
    let now: String = raw.get(key).await.unwrap();
    assert_eq!(now, "Jbeyq");
    clear(&mut raw, "codec:custom:").await;
}

#[tokio::test]
async fn an_unchanged_save_writes_back_the_exact_bytes() {
    let (c, mut raw) = clients!(0);
    clear(&mut raw, "codec:same:").await;
    let key = "codec:same:doc";
    let stored = codec::encode(&gzip(), DOC).unwrap();
    let _: () = raw.set(key, &stored).await.unwrap();
    let (KeyValue::Decoded { text, decoding }, _) = c
        .read_value_as(key, KeyType::String, &View::Auto)
        .await
        .unwrap()
    else {
        panic!("expected decoded");
    };
    let target = EditTarget {
        key: key.into(),
        kind: KeyType::String,
        selector: String::new(),
        original: text.clone(),
        decoded: Some(decoding),
    };
    assert_eq!(
        c.save_edit(&target, &[text], false).await.unwrap(),
        EditOutcome::Saved
    );
    let now: Vec<u8> = raw.get(key).await.unwrap();
    assert_eq!(now, stored, "nothing edited, nothing re-encoded");
    clear(&mut raw, "codec:same:").await;
}

#[tokio::test]
async fn edits_reach_keys_whose_names_hold_backslashes_and_raw_bytes() {
    let (c, mut raw) = clients!(0);
    clear(&mut raw, "codec:esc").await;
    // Stored names: `codec:esc\a` (one backslash) and one with a raw 0xff.
    let names: [&[u8]; 2] = [b"codec:esc\\a", b"codec:esc:\xff"];
    for name in names {
        let _: () = raw.set(name, "before").await.unwrap();
    }
    let (keys, _) = c.scan_keys("codec:esc*", 100).await.unwrap();
    assert_eq!(keys.len(), 2);
    for key in keys {
        let target = EditTarget {
            key: key.name.clone(),
            kind: KeyType::String,
            selector: String::new(),
            original: "before".into(),
            decoded: None,
        };
        assert_eq!(
            c.save_edit(&target, &["after".into()], false)
                .await
                .unwrap(),
            EditOutcome::Saved,
            "{}",
            key.name
        );
        let now: String = raw
            .get(rediscope::redis_client::decode_key(&key.name))
            .await
            .unwrap();
        assert_eq!(now, "after", "{}", key.name);
    }
    clear(&mut raw, "codec:esc").await;
}

#[cfg(unix)]
#[tokio::test]
async fn custom_codecs_that_print_their_own_canonical_form_can_still_save() {
    let (c, mut raw) = clients!(0);
    clear(&mut raw, "codec:canon:").await;
    let key = "codec:canon:v";
    let _: () = raw.set(key, "abc").await.unwrap();
    // A line-oriented decoder that adds a newline, and an encoder that
    // strips it: like protoc or jq, the text form is not byte-identical.
    let view = View::Codec(Codec::Custom(codec::CustomCodec {
        name: "lines".into(),
        decode: vec!["sh".into(), "-c".into(), "cat; echo".into()],
        encode: vec!["tr".into(), "-d".into(), "\\n".into()],
        timeout_secs: None,
    }));
    let (KeyValue::Decoded { text, decoding }, _) =
        c.read_value_as(key, KeyType::String, &view).await.unwrap()
    else {
        panic!("expected decoded");
    };
    assert_eq!(text, "abc\n");
    let target = EditTarget {
        key: key.into(),
        kind: KeyType::String,
        selector: String::new(),
        original: text,
        decoded: Some(decoding),
    };
    // Typed without the decoder's newline: still saved.
    assert_eq!(
        c.save_edit(&target, &["xyz".into()], false).await.unwrap(),
        EditOutcome::Saved
    );
    let now: String = raw.get(key).await.unwrap();
    assert_eq!(now, "xyz");
    clear(&mut raw, "codec:canon:").await;
}

#[cfg(unix)]
#[tokio::test]
async fn a_custom_codec_whose_programs_disagree_saves_nothing() {
    let (c, mut raw) = clients!(0);
    clear(&mut raw, "codec:mismatch:").await;
    let key = "codec:mismatch:v";
    let _: () = raw.set(key, "Uryyb").await.unwrap();
    // Decodes with rot13 but "encodes" by passing text through untouched.
    let view = View::Codec(Codec::Custom(codec::CustomCodec {
        name: "broken".into(),
        decode: vec![
            "sh".into(),
            "-c".into(),
            "tr 'A-Za-z' 'N-ZA-Mn-za-m'".into(),
        ],
        encode: vec!["cat".into()],
        timeout_secs: None,
    }));
    let (KeyValue::Decoded { text, decoding }, _) =
        c.read_value_as(key, KeyType::String, &view).await.unwrap()
    else {
        panic!("expected decoded");
    };
    let target = EditTarget {
        key: key.into(),
        kind: KeyType::String,
        selector: String::new(),
        original: text,
        decoded: Some(decoding),
    };
    let err = c
        .save_edit(&target, &["World".into()], false)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("do not agree"), "{err}");
    let now: String = raw.get(key).await.unwrap();
    assert_eq!(now, "Uryyb");
    clear(&mut raw, "codec:mismatch:").await;
}

#[cfg(unix)]
#[tokio::test]
async fn custom_encoders_that_never_repeat_their_bytes_can_save() {
    let (c, mut raw) = clients!(0);
    clear(&mut raw, "codec:salted:").await;
    let key = "codec:salted:v";
    // Like encryption with a fresh salt: every encode writes different
    // bytes ("<nonce>:<text>"), and decode strips the nonce.
    let encode = vec![
        "sh".to_string(),
        "-c".into(),
        "printf '%s:' \"$$-$(date +%N)\"; cat".into(),
    ];
    let decode = vec![
        "sh".to_string(),
        "-c".into(),
        "v=$(cat); printf %s \"${v#*:}\"".into(),
    ];
    let _: () = raw.set(key, "1:hello").await.unwrap();
    let view = View::Codec(Codec::Custom(codec::CustomCodec {
        name: "salted".into(),
        decode,
        encode,
        timeout_secs: None,
    }));
    let (KeyValue::Decoded { text, decoding }, _) =
        c.read_value_as(key, KeyType::String, &view).await.unwrap()
    else {
        panic!("expected decoded");
    };
    assert_eq!(text, "hello");
    let target = EditTarget {
        key: key.into(),
        kind: KeyType::String,
        selector: String::new(),
        original: text,
        decoded: Some(decoding),
    };
    assert_eq!(
        c.save_edit(&target, &["world".into()], false)
            .await
            .unwrap(),
        EditOutcome::Saved
    );
    let now: String = raw.get(key).await.unwrap();
    assert!(now.ends_with(":world"), "{now}");
    clear(&mut raw, "codec:salted:").await;
}

#[cfg(unix)]
#[tokio::test]
async fn a_custom_encoder_that_ignores_its_input_saves_nothing() {
    let (c, mut raw) = clients!(0);
    clear(&mut raw, "codec:deaf:").await;
    let key = "codec:deaf:v";
    let _: () = raw.set(key, "abc").await.unwrap();
    for (name, encode) in [
        // Prints nothing at all.
        ("silent", vec!["true".to_string()]),
        // Always prints the stored value, whatever it is given.
        (
            "constant",
            vec![
                "sh".to_string(),
                "-c".into(),
                "cat >/dev/null; printf abc".into(),
            ],
        ),
        // Always prints something else, whatever it is given.
        (
            "other constant",
            vec![
                "sh".to_string(),
                "-c".into(),
                "cat >/dev/null; printf zzz".into(),
            ],
        ),
    ] {
        let view = View::Codec(Codec::Custom(codec::CustomCodec {
            name: name.into(),
            decode: vec!["cat".into()],
            encode,
            timeout_secs: None,
        }));
        let (KeyValue::Decoded { text, decoding }, _) =
            c.read_value_as(key, KeyType::String, &view).await.unwrap()
        else {
            panic!("expected decoded");
        };
        let target = EditTarget {
            key: key.into(),
            kind: KeyType::String,
            selector: String::new(),
            original: text,
            decoded: Some(decoding),
        };
        assert!(
            c.save_edit(&target, &["xyz".into()], false).await.is_err(),
            "{name} saved"
        );
        let now: String = raw.get(key).await.unwrap();
        assert_eq!(now, "abc", "{name} changed the value");
    }
    clear(&mut raw, "codec:deaf:").await;
}
