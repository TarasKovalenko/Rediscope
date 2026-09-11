//! P0 safety checks. CI supplies a disposable standalone Redis via REDISCOPE_TEST_PORT.
mod common;

use common::Flavor;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use rediscope::{
    app::{App, Modal, Msg, Screen},
    config::{Connection, Environment, Store},
    redis_client::{Client, EditOutcome, EditTarget, KeyInfo, KeyType, KeyValue},
};

fn profile() -> Option<Connection> {
    common::isolate_config();
    Some(Connection {
        name: "p0-production".into(),
        host: "127.0.0.1".into(),
        port: std::env::var("REDISCOPE_TEST_PORT").ok()?.parse().ok()?,
        db: 0,
        environment: Environment::Production,
        ..Default::default()
    })
}
fn press(app: &mut App, code: KeyCode) {
    app.on_key(KeyEvent::new(code, KeyModifiers::NONE));
}
fn type_text(app: &mut App, text: &str) {
    for c in text.chars() {
        press(app, KeyCode::Char(c));
    }
}
fn target(key: &str, kind: KeyType, selector: &str, original: &str) -> EditTarget {
    EditTarget {
        key: key.into(),
        kind,
        selector: selector.into(),
        original: original.into(),
        decoded: None,
    }
}

#[tokio::test]
async fn locked_transport_ui_cli_and_audit() {
    let Some(p) = profile() else {
        return;
    };
    let c = Client::connect(p.clone()).await.unwrap();
    assert!(c.read_only());
    assert!(c.set_string("p0:locked", "secret-value").await.is_err());
    assert!(c.execute_raw("SET p0:locked secret-value").await.is_err());
    assert!(c.delete_keys(&["p0:locked".into()]).await.is_err());
    assert!(
        c.eval(
            "return redis.call('SET', KEYS[1], 'secret-value')",
            &["p0:locked".into()],
            &[]
        )
        .await
        .is_err()
    );
    assert!(c.unlock_writes("wrong").is_err());
    c.unlock_writes(&p.name).unwrap();
    assert!(!c.clone().read_only());
    c.set_string("p0:locked", "secret-value").await.unwrap();
    c.clone().lock_writes().unwrap();
    assert!(c.set_ttl("p0:locked", Some(1)).await.is_err());
    assert!(Client::connect(p.clone()).await.unwrap().read_only());
    let mut hard = p.clone();
    hard.read_only = true;
    assert!(
        Client::connect(hard)
            .await
            .unwrap()
            .unlock_writes(&p.name)
            .is_err()
    );

    // The UI can unlock; ordinary console writes must still require typed confirmation.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
    let mut app = App::new(Store::default(), tx);
    app.screen = Screen::Browser;
    app.client = Some(c.clone());
    app.on_key(KeyEvent::new(KeyCode::Char('w'), KeyModifiers::CONTROL));
    assert!(matches!(app.modal, Some(Modal::Form { .. })));
    type_text(&mut app, &p.name);
    press(&mut app, KeyCode::Enter);
    assert!(!c.read_only());
    app.current = Some(KeyInfo {
        name: "p0:locked".into(),
        kind: KeyType::String,
        ttl: -1,
    });
    press(&mut app, KeyCode::Char('D'));
    press(&mut app, KeyCode::Enter);
    assert!(matches!(app.modal, Some(Modal::Form { .. })));
    press(&mut app, KeyCode::Enter);
    assert!(matches!(
        &app.modal,
        Some(Modal::Form { error: Some(_), .. })
    ));
    assert_eq!(
        c.execute_raw("EXISTS p0:locked").await.unwrap(),
        "(integer) 1"
    );
    press(&mut app, KeyCode::Esc);
    press(&mut app, KeyCode::Char(':'));
    type_text(&mut app, "SET p0:console another-secret");
    press(&mut app, KeyCode::Enter);
    assert!(matches!(app.modal, Some(Modal::Form { .. })));
    press(&mut app, KeyCode::Esc);
    assert_eq!(
        c.execute_raw("EXISTS p0:console").await.unwrap(),
        "(integer) 0"
    );
    // A confirmed delete reaches Redis exactly once.
    press(&mut app, KeyCode::Char('D'));
    press(&mut app, KeyCode::Enter);
    type_text(&mut app, &p.name);
    press(&mut app, KeyCode::Enter);
    let msg = tokio::time::timeout(std::time::Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(msg, Msg::Mutated(Ok(_))));
    assert_eq!(
        c.execute_raw("EXISTS p0:locked").await.unwrap(),
        "(integer) 0"
    );
    app.on_key(KeyEvent::new(KeyCode::Char('w'), KeyModifiers::CONTROL));
    assert!(c.read_only());

    let file =
        std::env::temp_dir().join(format!("rediscope-p0-import-{}.json", std::process::id()));
    std::fs::write(&file, "[]").unwrap();
    let filename = file.to_str().unwrap();
    assert!(
        rediscope::headless::import(p.clone(), filename, false)
            .await
            .is_err()
    );
    assert!(
        rediscope::headless::import_confirmed(
            p.clone(),
            filename,
            false,
            Some(&p.name),
            Some("wrong")
        )
        .await
        .is_err()
    );
    rediscope::headless::import_confirmed(p.clone(), filename, false, Some(&p.name), Some(&p.name))
        .await
        .unwrap();
    // Exercise actual CLI flags and fail-closed read-only override.
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_rediscope"))
        .args([
            "-H",
            "127.0.0.1",
            "-p",
            &p.port.to_string(),
            "--environment",
            "production",
            "--read-only",
            "import",
            "--file",
            filename,
            "--unlock-production",
            "127.0.0.1",
            "--confirm-production",
            "127.0.0.1",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    std::fs::remove_file(file).unwrap();

    let audit = std::env::var_os("REDISCOPE_AUDIT_FILE")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| rediscope::config::config_file().with_file_name("audit.jsonl"));
    let log = std::fs::read_to_string(audit).unwrap();
    assert!(
        !log.contains("secret-value")
            && !log.contains("another-secret")
            && !log.contains("p0:locked")
    );
    let events: Vec<serde_json::Value> = log
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    assert!(
        events
            .iter()
            .any(|e| e["profile"] == p.name && e["outcome"] == "denied")
    );
    assert!(events.iter().any(|e| e["profile"] == p.name
        && e["action"] == "WRITE_UNLOCK"
        && e["outcome"] == "success"));
}

#[tokio::test]
async fn atomic_edits_detect_conflicts_and_preserve_ttls() {
    let Some(mut p) = profile() else {
        return;
    };
    p.environment = Environment::Development;
    let c = Client::connect(p).await.unwrap();
    c.set_string("p0:edit:string", "original").await.unwrap();
    c.set_ttl("p0:edit:string", Some(60)).await.unwrap();
    let t = target("p0:edit:string", KeyType::String, "", "original");
    c.set_string(&t.key, "concurrent").await.unwrap();
    assert!(c.key_info(&t.key).await.unwrap().ttl > 0);
    assert_eq!(
        c.save_edit(&t, &["draft".into()], false).await.unwrap(),
        EditOutcome::Conflict {
            current: Some("concurrent".into())
        }
    );
    assert_eq!(
        c.save_edit(&t, &["draft".into()], true).await.unwrap(),
        EditOutcome::Saved
    );
    assert!(c.key_info(&t.key).await.unwrap().ttl > 0);
    // Simultaneous writers sharing one baseline: exactly one commits.
    let t = target(&t.key, KeyType::String, "", "draft");
    let a = vec!["first".into()];
    let b = vec!["second".into()];
    let (a, b) = tokio::join!(c.save_edit(&t, &a, false), c.save_edit(&t, &b, false));
    let outcomes = [a.unwrap(), b.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|o| **o == EditOutcome::Saved)
            .count(),
        1
    );
    c.delete_key(&t.key).await.unwrap();
    assert_eq!(
        c.save_edit(&t, &["draft".into()], true).await.unwrap(),
        EditOutcome::Conflict { current: None }
    );
    c.hash_set(&t.key, "field", "newtype").await.unwrap();
    assert_eq!(
        c.save_edit(&t, &["draft".into()], true).await.unwrap(),
        EditOutcome::Conflict { current: None }
    );
    c.delete_key(&t.key).await.unwrap();

    c.hash_set("p0:edit:hash", "field", "old").await.unwrap();
    c.set_ttl("p0:edit:hash", Some(60)).await.unwrap();
    let h = target("p0:edit:hash", KeyType::Hash, "field", "old");
    c.hash_set(&h.key, "field", "other").await.unwrap();
    assert!(matches!(
        c.save_edit(&h, &["draft".into()], false).await.unwrap(),
        EditOutcome::Conflict { .. }
    ));
    assert_eq!(
        c.save_edit(&h, &["draft".into()], true).await.unwrap(),
        EditOutcome::Saved
    );
    assert!(c.key_info(&h.key).await.unwrap().ttl > 0);

    c.list_push("p0:edit:list", "old").await.unwrap();
    let l = target("p0:edit:list", KeyType::List, "0", "old");
    c.list_set(&l.key, 0, "other").await.unwrap();
    assert!(matches!(
        c.save_edit(&l, &["draft".into()], false).await.unwrap(),
        EditOutcome::Conflict { .. }
    ));

    for (kind, key) in [
        (KeyType::Set, "p0:edit:set"),
        (KeyType::ZSet, "p0:edit:zset"),
    ] {
        if kind == KeyType::Set {
            c.set_add(key, "old").await.unwrap();
        } else {
            c.zset_add(key, "old", 1.25).await.unwrap();
        }
        c.set_ttl(key, Some(60)).await.unwrap();
        let t = target(
            key,
            kind,
            "old",
            if kind == KeyType::Set { "old" } else { "1.25" },
        );
        // Renaming a member is an add plus a remove, and the script checks both
        // against the ACL with redis.acl_check_cmd before writing either. That
        // Lua function arrived in Redis 7; KeyDB (Redis 6 based) and Dragonfly
        // do not have it, so there the rename has to be refused untouched.
        if common::skip_on(
            &[Flavor::KeyDb, Flavor::Dragonfly],
            "no redis.acl_check_cmd in Lua, member rename is refused",
        ) {
            let err = c
                .save_edit(&t, &["new".into(), "2.5".into()], false)
                .await
                .unwrap_err();
            assert!(err.to_string().contains("Redis 7+"), "{err:#}");
            let (check, member, absent) = if kind == KeyType::Set {
                ("SISMEMBER", "(integer) 1", "(integer) 0")
            } else {
                ("ZSCORE", "1.25", "(nil)")
            };
            assert_eq!(
                c.execute_raw(&format!("{check} {key} old")).await.unwrap(),
                member,
                "the old member is still there"
            );
            assert_eq!(
                c.execute_raw(&format!("{check} {key} new")).await.unwrap(),
                absent,
                "and the new one was never added"
            );
            assert!(c.key_info(key).await.unwrap().ttl > 0);
            // A new score for the same member is a single ZADD and needs no
            // preflight, so that edit still goes through.
            if kind == KeyType::ZSet {
                assert_eq!(
                    c.save_edit(&t, &["old".into(), "2.5".into()], false)
                        .await
                        .unwrap(),
                    EditOutcome::Saved
                );
                assert_eq!(
                    c.execute_raw(&format!("ZSCORE {key} old")).await.unwrap(),
                    "2.5"
                );
            }
            continue;
        }
        assert_eq!(
            c.save_edit(&t, &["new".into(), "2.5".into()], false)
                .await
                .unwrap(),
            EditOutcome::Saved
        );
        assert!(
            c.key_info(key).await.unwrap().ttl > 0,
            "last-member rename must preserve TTL"
        );
        assert!(matches!(
            c.save_edit(&t, &["third".into(), "3".into()], false)
                .await
                .unwrap(),
            EditOutcome::Conflict { .. }
        ));
    }
    c.delete_keys(&[h.key, l.key, "p0:edit:set".into(), "p0:edit:zset".into()])
        .await
        .unwrap();
}

#[test]
fn conflict_ui_retains_draft_and_requires_explicit_overwrite() {
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let mut app = App::new(Store::default(), tx);
    app.screen = Screen::Browser;
    app.current = Some(KeyInfo {
        name: "key".into(),
        kind: KeyType::String,
        ttl: -1,
    });
    app.value = Some(KeyValue::Str("old".into()));
    app.modal = Some(Modal::EditConflict {
        target: target("key", KeyType::String, "", "old"),
        values: vec!["draft".into()],
        current: Some("new".into()),
        error: None,
    });
    press(&mut app, KeyCode::Char('e'));
    let Some(Modal::Editor { textarea, .. }) = &app.modal else {
        panic!("draft editor")
    };
    assert_eq!(textarea.lines().join("\n"), "draft");
    app.modal = Some(Modal::EditConflict {
        target: target("key", KeyType::String, "", "old"),
        values: vec!["draft".into()],
        current: Some("new".into()),
        error: None,
    });
    press(&mut app, KeyCode::Char('o'));
    press(&mut app, KeyCode::Enter);
    assert!(matches!(
        app.modal,
        Some(Modal::Form { error: Some(_), .. })
    ));
}

/// Adding an element still writes through the plain add path; editing an
/// existing one always goes through the conflict-checked save.
#[tokio::test]
async fn add_forms_write_and_editing_a_row_goes_through_conflict_detection() {
    let Some(mut p) = profile() else {
        return;
    };
    p.environment = Environment::Development;
    let c = Client::connect(p).await.unwrap();
    c.delete_keys(&["p0:add:hash".into(), "p0:add:zset".into()])
        .await
        .unwrap();
    c.hash_set("p0:add:hash", "existing", "value")
        .await
        .unwrap();
    c.zset_add("p0:add:zset", "existing", 1.0).await.unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
    let mut app = App::new(Store::default(), tx);
    app.screen = Screen::Browser;
    app.client = Some(c.clone());

    for (key, kind, fields) in [
        ("p0:add:hash", KeyType::Hash, vec!["added", "value"]),
        ("p0:add:zset", KeyType::ZSet, vec!["added", "2.5"]),
    ] {
        app.current = Some(KeyInfo {
            name: key.into(),
            kind,
            ttl: -1,
        });
        press(&mut app, KeyCode::Char('a'));
        assert!(matches!(app.modal, Some(Modal::Form { .. })), "{key}");
        for (i, text) in fields.iter().enumerate() {
            if i > 0 {
                press(&mut app, KeyCode::Tab);
            }
            type_text(&mut app, text);
        }
        press(&mut app, KeyCode::Enter);
        let msg = tokio::time::timeout(std::time::Duration::from_secs(3), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(msg, Msg::Mutated(Ok(_))), "{key}");
    }
    assert_eq!(
        c.execute_raw("HGET p0:add:hash added").await.unwrap(),
        "value"
    );
    assert_eq!(
        c.execute_raw("ZSCORE p0:add:zset added").await.unwrap(),
        "2.5"
    );

    // `e` on a row must produce a conflict-checked save, not a blind overwrite.
    app.current = Some(KeyInfo {
        name: "p0:add:hash".into(),
        kind: KeyType::Hash,
        ttl: -1,
    });
    app.on_msg(Msg::Value {
        info: app.current.clone().unwrap(),
        value: KeyValue::Rows {
            headers: vec!["field", "value"],
            rows: vec![rediscope::redis_client::Row {
                id: "existing".into(),
                cells: vec!["existing".into(), "value".into()],
                decoding: None,
                ttl: None,
            }],
            total: 1,
        },
    });
    press(&mut app, KeyCode::Char('e'));
    assert!(matches!(app.modal, Some(Modal::Form { .. })));
    c.hash_set("p0:add:hash", "existing", "changed by someone else")
        .await
        .unwrap();
    type_text(&mut app, "my draft");
    press(&mut app, KeyCode::Enter);
    let msg = tokio::time::timeout(std::time::Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap();
    app.on_msg(msg);
    assert!(
        matches!(app.modal, Some(Modal::EditConflict { .. })),
        "a changed field must open the conflict view"
    );
    assert_eq!(
        c.execute_raw("HGET p0:add:hash existing").await.unwrap(),
        "changed by someone else"
    );
    c.delete_keys(&["p0:add:hash".into(), "p0:add:zset".into()])
        .await
        .unwrap();
}

/// Scrolling the key tree must not fire a read per row, and a read that comes
/// back for a key the cursor has already left must not land on screen.
#[tokio::test]
async fn scrolling_coalesces_value_reads_and_drops_stale_replies() {
    let Some(mut p) = profile() else {
        return;
    };
    p.environment = Environment::Development;
    let c = Client::connect(p).await.unwrap();
    // Flat names keep the tree free of folder rows, so every Down lands on a key.
    let names: Vec<String> = (0..4).map(|i| format!("p0scroll{i}")).collect();
    for name in &names {
        c.set_string(name, "value").await.unwrap();
    }

    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
    let mut app = App::new(Store::default(), tx);
    app.screen = Screen::Browser;
    app.client = Some(c.clone());
    app.on_msg(Msg::Keys {
        keys: names
            .iter()
            .map(|name| KeyInfo {
                name: name.clone(),
                kind: KeyType::String,
                ttl: -1,
            })
            .collect(),
        truncated: false,
        warnings: vec![],
        dbsize: names.len() as u64,
        pattern: "p0scroll*".into(),
    });

    // Three rows of travel, one pending read: the deadline keeps moving out.
    for _ in 0..3 {
        press(&mut app, KeyCode::Down);
        assert!(
            app.value_deadline().is_some(),
            "a moved cursor must schedule a read, not issue one"
        );
        assert!(app.value.is_none(), "no value may load mid-scroll");
    }
    let resting = app.current.clone().expect("a key under the cursor");
    app.on_value_deadline();
    assert!(app.value_deadline().is_none());

    // A reply for a key we scrolled past arrives late and is ignored.
    app.on_msg(Msg::Value {
        info: KeyInfo {
            name: names[0].clone(),
            kind: KeyType::String,
            ttl: -1,
        },
        value: KeyValue::Str("stale".into()),
    });
    assert_eq!(app.current.as_ref().unwrap().name, resting.name);
    assert!(app.value.is_none(), "a stale reply must not reach the pane");

    // The reply we actually asked for does land.
    app.on_msg(Msg::Value {
        info: resting.clone(),
        value: KeyValue::Str("current".into()),
    });
    assert_eq!(app.current.as_ref().unwrap().name, resting.name);
    assert!(matches!(app.value, Some(KeyValue::Str(ref t)) if t == "current"));

    c.delete_keys(&names).await.unwrap();
}
