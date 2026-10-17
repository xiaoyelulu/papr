//! WebDAV transport: compressed incremental chunks and a conditional root.
//! A strong ETag is required for existing files: unsupported concurrency
//! protection fails closed instead of overwriting another device's upload.
use crate::db::{
    self,
    webdav::{self as replication, Record},
};
use crate::error::{AppError, AppResult};
use reqwest::{header, Client, StatusCode};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::Mutex;
use url::Url;

mod chunks;
pub(crate) use chunks::schema as chunk_schema;

type Db = Mutex<Connection>;
const FILE: &str = "papr-library-v1.json";
const LEGACY_MAX_BYTES: usize = 128 * 1024 * 1024;
// Serialize connect/disconnect/run in this process, including the scheduler.
static SYNC_LOCK: Mutex<()> = Mutex::const_new(());

#[derive(Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    version: u32,
    records: Vec<Record>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    pub connected: bool,
    pub url: Option<String>,
    pub username: Option<String>,
    pub last_sync: Option<String>,
    pub last_error: Option<&'static str>,
    pub conflicts: usize,
}

struct Credentials {
    url: Url,
    user: String,
    password: String,
}

fn base_url(input: &str) -> AppResult<Url> {
    let mut url = Url::parse(input.trim()).map_err(|_| AppError::code("webdavInvalidUrl"))?;
    if !matches!(url.scheme(), "https" | "http")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(AppError::code("webdavInvalidUrl"));
    }
    if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path()));
    }
    Ok(url)
}

fn credentials(conn: &Connection) -> AppResult<Option<Credentials>> {
    let Some(url) = db::get_setting(conn, "webdav_url")?.filter(|u| !u.is_empty()) else {
        return Ok(None);
    };
    Ok(Some(Credentials {
        url: base_url(&url)?,
        user: db::get_setting(conn, "webdav_user")?.unwrap_or_default(),
        password: db::get_setting(conn, "webdav_password")?.unwrap_or_default(),
    }))
}

pub async fn status(db: &Db) -> AppResult<Status> {
    let conn = db.lock().await;
    let c = credentials(&conn)?;
    Ok(Status {
        connected: c.is_some(),
        url: c.as_ref().map(|c| c.url.to_string()),
        username: c.map(|c| c.user),
        last_sync: db::get_setting(&conn, "webdav_last_sync")?,
        conflicts: replication::conflicts(&conn)?.len(),
        last_error: db::get_setting(&conn, "webdav_last_error")?
            .filter(|s| !s.is_empty())
            .map(|s| {
                if s == "webdavNeedsResolution" {
                    "webdavNeedsResolution"
                } else {
                    "webdavSyncFailed"
                }
            }),
    })
}

fn request(
    http: &Client,
    c: &Credentials,
    method: reqwest::Method,
    url: Url,
) -> reqwest::RequestBuilder {
    http.request(method, url)
        .basic_auth(&c.user, Some(&c.password))
        .timeout(Duration::from_secs(120))
}

fn http_error(status: StatusCode) -> AppError {
    match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => AppError::code("webdavAuthFailed"),
        _ => AppError::other(format!("WebDAV: HTTP {status}")),
    }
}

// Strip the request URL from transport errors: some deployments put private
// tokens in their WebDAV path. Server response bodies are never logged.
fn network_error(e: reqwest::Error) -> AppError {
    AppError::Http(e.without_url())
}

/// Create only the final directory; the configured parent must already exist.
async fn ensure_collection(http: &Client, c: &Credentials) -> AppResult<()> {
    let resp = request(
        http,
        c,
        reqwest::Method::from_bytes(b"MKCOL").unwrap(),
        c.url.clone(),
    )
    .send()
    .await
    .map_err(network_error)?;
    if resp.status().is_success() || resp.status() == StatusCode::METHOD_NOT_ALLOWED {
        Ok(())
    } else {
        Err(http_error(resp.status()))
    }
}

async fn download(http: &Client, c: &Credentials) -> AppResult<(Root, Option<String>)> {
    let mut resp = request(http, c, reqwest::Method::GET, c.url.join(FILE)?)
        .header(header::CACHE_CONTROL, "no-cache")
        .send()
        .await
        .map_err(network_error)?;
    if resp.status() == StatusCode::NOT_FOUND {
        return Ok((Root::Manifest(chunks::Manifest::empty()), None));
    }
    if !resp.status().is_success() {
        return Err(http_error(resp.status()));
    }
    let etag = resp
        .headers()
        .get(header::ETAG)
        .and_then(|v| v.to_str().ok())
        .filter(|v| v.starts_with('"') && v.ends_with('"'))
        .ok_or_else(|| AppError::code("webdavEtagRequired"))?
        .to_string();
    if resp
        .content_length()
        .is_some_and(|n| n > LEGACY_MAX_BYTES as u64)
    {
        return Err(AppError::code("webdavTooLarge"));
    }
    let mut body = Vec::new();
    while let Some(chunk) = resp.chunk().await.map_err(network_error)? {
        if body.len() + chunk.len() > LEGACY_MAX_BYTES {
            return Err(AppError::code("webdavTooLarge"));
        }
        body.extend_from_slice(&chunk);
    }
    #[derive(Deserialize)]
    struct Version {
        version: u32,
    }
    let version: Version =
        serde_json::from_slice(&body).map_err(|_| AppError::code("webdavInvalidData"))?;
    let root = if version.version == 6 {
        if body.len() > 16 * 1024 * 1024 {
            return Err(AppError::code("webdavTooLarge"));
        }
        let manifest: chunks::Manifest =
            serde_json::from_slice(&body).map_err(|_| AppError::code("webdavInvalidData"))?;
        manifest.validate()?;
        Root::Manifest(manifest)
    } else if (1..=5).contains(&version.version) {
        let mut snapshot: Snapshot =
            serde_json::from_slice(&body).map_err(|_| AppError::code("webdavInvalidData"))?;
        if snapshot.version < 4 {
            replication::upgrade_legacy_tags(&mut snapshot.records)?;
        }
        Root::Legacy(snapshot)
    } else {
        return Err(AppError::code("webdavVersion"));
    };
    Ok((root, Some(etag)))
}

enum Root {
    Legacy(Snapshot),
    Manifest(chunks::Manifest),
}

async fn fetch_chunk(http: &Client, c: &Credentials, p: &chunks::Part) -> AppResult<Vec<u8>> {
    let mut resp = request(http, c, reqwest::Method::GET, c.url.join(&chunks::path(p))?)
        .header(header::ACCEPT_ENCODING, "identity")
        .send()
        .await
        .map_err(network_error)?;
    if !resp.status().is_success() {
        return Err(http_error(resp.status()));
    }
    if resp.content_length().is_some_and(|n| n != p.bytes as u64) {
        return Err(AppError::code("webdavInvalidData"));
    }
    let mut body = Vec::with_capacity(p.bytes);
    while let Some(b) = resp.chunk().await.map_err(network_error)? {
        if body.len() + b.len() > p.bytes {
            return Err(AppError::code("webdavInvalidData"));
        }
        body.extend_from_slice(&b);
    }
    Ok(body)
}

/// Verify read and write access using a disposable file. Never upload or merge
/// library data merely because the user saves connection settings.
pub async fn connect(
    db: &Db,
    http: &Client,
    url: &str,
    user: &str,
    password: &str,
) -> AppResult<()> {
    let _guard = SYNC_LOCK.lock().await;
    {
        let conn = db.lock().await;
        if db::is_freshrss_connected(&conn) {
            return Err(AppError::code("syncProviderConflict"));
        }
    }
    let c = Credentials {
        url: base_url(url)?,
        user: user.trim().into(),
        password: password.into(),
    };
    ensure_collection(http, &c).await?;
    download(http, &c).await?;
    let nonce: String = {
        let conn = db.lock().await;
        replication::probe_id(&conn)?
    };
    let probe = c.url.join(&format!(".papr-test-{nonce}"))?;
    let resp = request(http, &c, reqwest::Method::PUT, probe.clone())
        .header(header::IF_NONE_MATCH, "*")
        .body("papr")
        .send()
        .await
        .map_err(network_error)?;
    if !resp.status().is_success() {
        return Err(http_error(resp.status()));
    }
    let resp = request(http, &c, reqwest::Method::DELETE, probe)
        .send()
        .await
        .map_err(network_error)?;
    if !resp.status().is_success() {
        return Err(http_error(resp.status()));
    }
    let mut conn = db.lock().await;
    let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    if db::is_freshrss_connected(&tx) {
        return Err(AppError::code("syncProviderConflict"));
    }
    if db::get_setting(&tx, "webdav_url")?.as_deref() != Some(c.url.as_str())
        || db::get_setting(&tx, "webdav_user")?.as_deref() != Some(c.user.as_str())
    {
        tx.execute("DELETE FROM webdav_baseline", [])?;
        tx.execute("DELETE FROM webdav_conflicts", [])?;
        tx.execute("DELETE FROM webdav_observed", [])?;
        tx.execute("DELETE FROM webdav_incoming", [])?;
        tx.execute("DELETE FROM webdav_incoming_chunks", [])?;
        tx.execute("DELETE FROM webdav_objects", [])?;
        tx.execute("DELETE FROM webdav_buckets", [])?;
    }
    for (key, value) in [
        ("webdav_url", c.url.as_str()),
        ("webdav_user", c.user.as_str()),
        ("webdav_password", password),
        ("webdav_last_sync", ""),
        ("webdav_last_error", ""),
    ] {
        db::set_setting(&tx, key, value)?;
    }
    tx.commit()?;
    Ok(())
}

pub async fn disconnect(db: &Db) -> AppResult<()> {
    let _guard = SYNC_LOCK.lock().await;
    let mut conn = db.lock().await;
    let tx = conn.transaction()?;
    for key in [
        "webdav_url",
        "webdav_user",
        "webdav_password",
        "webdav_last_sync",
        "webdav_last_error",
    ] {
        db::set_setting(&tx, key, "")?;
    }
    tx.commit()?;
    Ok(())
}

pub async fn conflicts(db: &Db) -> AppResult<Vec<replication::Conflict>> {
    replication::conflicts(&*db.lock().await)
}
pub async fn resolve(db: &Db, kind: &str, key: &str, choice: &str) -> AppResult<()> {
    let _guard = SYNC_LOCK.lock().await;
    replication::resolve_conflict(&mut *db.lock().await, kind, key, choice)
}

pub async fn run_if_connected(db: &Db, http: &Client) -> AppResult<bool> {
    if !status(db).await?.connected {
        return Ok(false);
    }
    sync_now(db, http).await.map(|_| true)
}

pub async fn sync_now(db: &Db, http: &Client) -> AppResult<usize> {
    let _guard = SYNC_LOCK.lock().await;
    let c = {
        let conn = db.lock().await;
        credentials(&conn)?.ok_or_else(|| AppError::code("webdavNotConnected"))?
    };
    let result = sync_inner(db, http, &c).await;
    let conn = db.lock().await;
    match &result {
        Ok(_) => {
            db::set_setting(&conn, "webdav_last_sync", &chrono::Utc::now().to_rfc3339())?;
            db::set_setting(&conn, "webdav_last_error", "")?;
        }
        Err(error) => {
            db::set_setting(
                &conn,
                "webdav_last_error",
                if matches!(error, AppError::Coded("webdavNeedsResolution")) {
                    "webdavNeedsResolution"
                } else {
                    "webdavSyncFailed"
                },
            )?;
        }
    }
    result
}

async fn sync_inner(db: &Db, http: &Client, c: &Credentials) -> AppResult<usize> {
    let mut total = 0;
    for _ in 0..3 {
        let (remote, etag) = download(http, c).await?;
        match &remote {
            Root::Legacy(snapshot) => chunks::stage_legacy(&mut *db.lock().await, snapshot)?,
            Root::Manifest(manifest) => {
                for p in &manifest.chunks {
                    let present = chunks::available(&*db.lock().await, p)?;
                    if !present {
                        let body = fetch_chunk(http, c, p).await?;
                        chunks::cache(&*db.lock().await, p, &body)?;
                    }
                }
                chunks::stage(&mut *db.lock().await, manifest)?;
            }
        }
        let outgoing = {
            let mut conn = db.lock().await;
            total += replication::merge_staged(&mut conn)?;
            chunks::export(&mut conn)?
        };
        let remote_parts = match &remote {
            Root::Manifest(m) => m
                .chunks
                .iter()
                .map(|p| p.hash.as_str())
                .collect::<std::collections::BTreeSet<_>>(),
            _ => Default::default(),
        };
        if etag.is_some() && matches!(&remote,Root::Manifest(m) if *m==outgoing) {
            chunks::acknowledge(&*db.lock().await)?;
            chunks::prune(&*db.lock().await)?;
            return Ok(total);
        }
        // Publish immutable objects first. A partial upload cannot expose an
        // incomplete snapshot; the old conditional root remains authoritative.
        for p in &outgoing.chunks {
            if remote_parts.contains(p.hash.as_str()) {
                continue;
            }
            let body = chunks::cached(&*db.lock().await, p)?
                .ok_or_else(|| AppError::code("webdavInvalidData"))?;
            let resp = request(http, c, reqwest::Method::PUT, c.url.join(&chunks::path(p))?)
                .header(header::IF_NONE_MATCH, "*")
                .header(header::CONTENT_TYPE, "application/gzip")
                .body(body)
                .send()
                .await
                .map_err(network_error)?;
            if resp.status() == StatusCode::PRECONDITION_FAILED {
                // Existing content-addressed objects are reusable only after
                // verifying their actual bytes (also resumes interrupted runs).
                let body = fetch_chunk(http, c, p).await?;
                chunks::cache(&*db.lock().await, p, &body)?;
            } else if !resp.status().is_success() {
                return Err(http_error(resp.status()));
            }
        }
        let body =
            serde_json::to_vec(&outgoing).map_err(|_| AppError::code("webdavInvalidData"))?;
        if body.len() > 16 * 1024 * 1024 {
            return Err(AppError::code("webdavTooLarge"));
        }
        let mut req = request(http, c, reqwest::Method::PUT, c.url.join(FILE)?)
            .header(header::CONTENT_TYPE, "application/json");
        req = if let Some(etag) = etag {
            req.header(header::IF_MATCH, etag)
        } else {
            req.header(header::IF_NONE_MATCH, "*")
        };
        let resp = req.body(body).send().await.map_err(network_error)?;
        if matches!(
            resp.status(),
            StatusCode::PRECONDITION_FAILED | StatusCode::CONFLICT
        ) {
            continue;
        }
        if !resp.status().is_success() {
            return Err(http_error(resp.status()));
        }
        chunks::acknowledge(&*db.lock().await)?;
        chunks::prune(&*db.lock().await)?;
        return Ok(total);
    }
    Err(AppError::code("webdavConflict"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;
    use std::sync::{Arc, Mutex as StdMutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[derive(Default)]
    struct Server {
        body: Option<Vec<u8>>,
        objects: std::collections::BTreeMap<String, Vec<u8>>,
        chunk_gets: usize,
        chunk_puts: usize,
        revision: u64,
        conflict: bool,
        missing_etag: bool,
        deny: bool,
        fail_upload: bool,
        fail_root: bool,
        conditional_writes: usize,
    }
    async fn server() -> (String, Arc<StdMutex<Server>>, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/papr/", listener.local_addr().unwrap());
        let state = Arc::new(StdMutex::new(Server::default()));
        let shared = state.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let end = loop {
                    let mut buf = [0; 4096];
                    let n = socket.read(&mut buf).await.unwrap();
                    if n == 0 {
                        break None;
                    }
                    bytes.extend_from_slice(&buf[..n]);
                    if let Some(p) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                        break Some(p + 4);
                    }
                };
                let Some(end) = end else {
                    continue;
                };
                let headers = String::from_utf8_lossy(&bytes[..end]).to_string();
                let length = headers
                    .lines()
                    .find_map(|l| {
                        l.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .and_then(|v| v.trim().parse::<usize>().ok())
                    })
                    .unwrap_or(0);
                while bytes.len() < end + length {
                    let mut buf = [0; 4096];
                    let n = socket.read(&mut buf).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    bytes.extend_from_slice(&buf[..n]);
                }
                let (code, etag, body) = {
                    let mut s = shared.lock().unwrap();
                    let method = headers.split_whitespace().next().unwrap();
                    let path = headers.split_whitespace().nth(1).unwrap().to_string();
                    let object = path.contains("papr-chunk-v6-");
                    let probe = headers.lines().next().unwrap().contains(".papr-test-");
                    if s.deny {
                        (401, None, Vec::new())
                    } else if method == "MKCOL" {
                        (405, None, Vec::new())
                    } else if probe {
                        (if method == "PUT" { 201 } else { 204 }, None, Vec::new())
                    } else if object {
                        if method == "GET" {
                            s.chunk_gets += 1;
                            match s.objects.get(&path) {
                                Some(b) => (200, None, b.clone()),
                                None => (404, None, Vec::new()),
                            }
                        } else if method == "PUT" {
                            assert!(headers.to_ascii_lowercase().contains("if-none-match: *"));
                            s.chunk_puts += 1;
                            if s.fail_upload || s.fail_root {
                                (500, None, Vec::new())
                            } else if s.objects.contains_key(&path) {
                                (412, None, Vec::new())
                            } else {
                                s.objects.insert(path, bytes[end..end + length].to_vec());
                                (201, None, Vec::new())
                            }
                        } else {
                            (400, None, Vec::new())
                        }
                    } else if method == "GET" {
                        match s.body.clone() {
                            Some(body) => (
                                200,
                                if s.missing_etag {
                                    None
                                } else {
                                    Some(format!("\"{}\"", s.revision))
                                },
                                body,
                            ),
                            None => (404, None, Vec::new()),
                        }
                    } else if method == "PUT" {
                        let lower = headers.to_ascii_lowercase();
                        s.conditional_writes += 1;
                        let expected = if s.body.is_some() {
                            format!("if-match: \"{}\"", s.revision)
                        } else {
                            "if-none-match: *".into()
                        };
                        assert!(lower.contains(&expected), "Missing condition: {headers}");
                        if s.fail_upload || s.fail_root {
                            (500, None, Vec::new())
                        } else if s.conflict {
                            s.conflict = false;
                            s.revision += 1;
                            (412, None, Vec::new())
                        } else {
                            s.body = Some(bytes[end..end + length].to_vec());
                            s.revision += 1;
                            (201, None, Vec::new())
                        }
                    } else {
                        (400, None, Vec::new())
                    }
                };
                let etag = etag.map(|s| format!("ETag: {s}\r\n")).unwrap_or_default();
                let response = format!(
                    "HTTP/1.1 {code} Test\r\n{etag}Content-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
                socket.write_all(&body).await.unwrap();
            }
        });
        (url, state, task)
    }
    fn database() -> Db {
        Mutex::new(db::open(std::path::Path::new(":memory:")).unwrap())
    }
    fn client() -> Client {
        Client::builder().no_proxy().build().unwrap()
    }
    async fn seed(db: &Db) {
        let conn = db.lock().await;
        conn.execute(
            "INSERT INTO feeds(feed_url,title) VALUES('https://example.com/rss','Feed')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO articles(feed_id,guid,title) VALUES(1,'one','Article')",
            [],
        )
        .unwrap();
        db::set_read(&conn, 1, true).unwrap();
    }
    #[tokio::test]
    async fn two_devices_merge_over_http_and_retry_conditional_conflicts() {
        let (url, server, task) = server().await;
        let http = client();
        let a = database();
        let b = database();
        seed(&a).await;
        connect(&a, &http, &url, "alice", "SECRET").await.unwrap();
        assert!(
            server.lock().unwrap().body.is_none(),
            "Connecting must not upload the library"
        );
        sync_now(&a, &http).await.unwrap();
        connect(&b, &http, &url, "alice", "SECRET").await.unwrap();
        db::set_setting(&*b.lock().await, "ai_summary_prompt", "retry prompt").unwrap();
        server.lock().unwrap().conflict = true;
        sync_now(&b, &http).await.unwrap();
        {
            let conn = b.lock().await;
            assert!(db::get_article(&conn, 1).unwrap().is_read);
            db::set_starred(&conn, 1, true).unwrap();
        }
        sync_now(&b, &http).await.unwrap();
        sync_now(&a, &http).await.unwrap();
        assert!(db::get_article(&*a.lock().await, 1).unwrap().is_starred);
        assert!(status(&a).await.unwrap().last_sync.is_some());
        assert!(
            !String::from_utf8(server.lock().unwrap().body.clone().unwrap())
                .unwrap()
                .contains("SECRET")
        );
        assert_eq!(server.lock().unwrap().conditional_writes, 4);
        sync_now(&a, &http).await.unwrap();
        assert_eq!(
            server.lock().unwrap().conditional_writes,
            4,
            "Idle sync must not upload again"
        );
        disconnect(&a).await.unwrap();
        assert!(!status(&a).await.unwrap().connected);
        assert!(server.lock().unwrap().body.is_some());
        task.abort();
    }
    #[tokio::test]
    async fn ai_preferences_sync_over_http_with_conflict_review_and_no_secrets() {
        let (url, server, task) = server().await;
        let http = client();
        let a = database();
        let b = database();
        {
            let c = a.lock().await;
            db::configure_ai_selection(&c, "codex", Some("shared-model"), None, Some("high"))
                .unwrap();
            db::set_setting(&c, "ai_summary_prompt", "Shared prompt").unwrap();
            db::set_setting(&c, "ai_api_key", "SECRET_AI_KEY").unwrap();
            db::set_setting(&c, "ai_base_url", "https://SECRET@example.com/v1").unwrap();
        }
        connect(&a, &http, &url, "a", "SECRET_DAV").await.unwrap();
        sync_now(&a, &http).await.unwrap();
        connect(&b, &http, &url, "a", "SECRET_DAV").await.unwrap();
        sync_now(&b, &http).await.unwrap();
        {
            let c = b.lock().await;
            assert_eq!(
                db::get_setting(&c, "ai_provider").unwrap().as_deref(),
                Some("codex")
            );
            assert_eq!(
                db::get_setting(&c, "ai_model").unwrap().as_deref(),
                Some("shared-model")
            );
            assert_eq!(
                db::get_setting(&c, "ai_reasoning_effort_codex")
                    .unwrap()
                    .as_deref(),
                Some("high")
            );
            assert_eq!(
                db::get_setting(&c, "ai_summary_prompt").unwrap().as_deref(),
                Some("Shared prompt")
            );
            assert!(db::get_setting(&c, "ai_api_key").unwrap().is_none());
            db::set_setting(&c, "ai_summary_prompt", "Device B prompt").unwrap();
        }
        db::set_setting(&*a.lock().await, "ai_summary_prompt", "Device A prompt").unwrap();
        sync_now(&a, &http).await.unwrap();
        let before = server.lock().unwrap().body.clone();
        assert!(matches!(
            sync_now(&b, &http).await,
            Err(AppError::Coded("webdavNeedsResolution"))
        ));
        assert_eq!(server.lock().unwrap().body, before);
        let c = conflicts(&b).await.unwrap().remove(0);
        assert_eq!(c.kind, "ai_prompt");
        resolve(&b, "ai_prompt", "summary", "local").await.unwrap();
        sync_now(&b, &http).await.unwrap();
        sync_now(&a, &http).await.unwrap();
        assert_eq!(
            db::get_setting(&*a.lock().await, "ai_summary_prompt")
                .unwrap()
                .as_deref(),
            Some("Device B prompt")
        );
        let body = String::from_utf8(server.lock().unwrap().body.clone().unwrap()).unwrap();
        assert!(!body.contains("SECRET"));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&body).unwrap()["version"],
            6
        );
        task.abort();
    }
    #[tokio::test]
    async fn conflicting_progress_pauses_before_upload_and_resolution_converges() {
        let (url, server, task) = server().await;
        let http = client();
        let a = database();
        let b = database();
        seed(&a).await;
        connect(&a, &http, &url, "a", "p").await.unwrap();
        sync_now(&a, &http).await.unwrap();
        connect(&b, &http, &url, "a", "p").await.unwrap();
        sync_now(&b, &http).await.unwrap();
        replication::set_progress(&*a.lock().await, 1, 0.2).unwrap();
        sync_now(&a, &http).await.unwrap();
        replication::set_progress(&*b.lock().await, 1, 0.8).unwrap();
        let before = server.lock().unwrap().body.clone();
        let writes = server.lock().unwrap().conditional_writes;
        assert!(matches!(
            sync_now(&b, &http).await,
            Err(AppError::Coded("webdavNeedsResolution"))
        ));
        assert_eq!(status(&b).await.unwrap().conflicts, 1);
        assert_eq!(server.lock().unwrap().body, before);
        assert_eq!(server.lock().unwrap().conditional_writes, writes);
        let conflict = conflicts(&b).await.unwrap().remove(0);
        resolve(&b, &conflict.kind, &conflict.key, "local")
            .await
            .unwrap();
        sync_now(&b, &http).await.unwrap();
        sync_now(&a, &http).await.unwrap();
        assert_eq!(status(&b).await.unwrap().conflicts, 0);
        assert_eq!(
            db::get_article(&*a.lock().await, 1)
                .unwrap()
                .reading_progress,
            0.8
        );
        assert_eq!(
            replication::records(&*a.lock().await).unwrap(),
            replication::records(&*b.lock().await).unwrap()
        );
        task.abort();
    }

    #[tokio::test]
    async fn refuses_unsafe_or_malformed_remote_and_keeps_local_library() {
        let (url, server, task) = server().await;
        let http = client();
        let a = database();
        seed(&a).await;
        connect(&a, &http, &url, "alice", "secret").await.unwrap();
        {
            let mut s = server.lock().unwrap();
            s.body = Some(b"not json".to_vec());
            s.missing_etag = true;
        }
        assert!(matches!(
            sync_now(&a, &http).await,
            Err(AppError::Coded("webdavEtagRequired"))
        ));
        server.lock().unwrap().missing_etag = false;
        assert!(matches!(
            sync_now(&a, &http).await,
            Err(AppError::Coded("webdavInvalidData"))
        ));
        server.lock().unwrap().body = Some(br#"{"version":999,"records":[]}"#.to_vec());
        assert!(matches!(
            sync_now(&a, &http).await,
            Err(AppError::Coded("webdavVersion"))
        ));
        assert!(db::get_article(&*a.lock().await, 1).unwrap().is_read);
        assert!(status(&a).await.unwrap().last_error.is_some());
        task.abort();
    }
    #[tokio::test]
    async fn failed_auth_does_not_save_credentials_and_failed_upload_can_retry() {
        let (url, server, task) = server().await;
        let http = client();
        let a = database();
        seed(&a).await;
        server.lock().unwrap().deny = true;
        assert!(matches!(
            connect(&a, &http, &url, "a", "p").await,
            Err(AppError::Coded("webdavAuthFailed"))
        ));
        assert!(!status(&a).await.unwrap().connected);
        server.lock().unwrap().deny = false;
        connect(&a, &http, &url, "a", "p").await.unwrap();
        server.lock().unwrap().fail_upload = true;
        assert!(sync_now(&a, &http).await.is_err());
        server.lock().unwrap().fail_upload = false;
        sync_now(&a, &http).await.unwrap();
        assert!(status(&a).await.unwrap().last_error.is_none());
        task.abort();
    }
    #[tokio::test]
    async fn migrates_legacy_and_only_transfers_changed_state_chunks() {
        let (url, server, task) = server().await;
        let http = client();
        let a = database();
        let b = database();
        seed(&a).await;
        {
            let c = a.lock().await;
            c.execute(
                "UPDATE articles SET body_text=?1 WHERE id=1",
                ["article body ".repeat(100_000)],
            )
            .unwrap();
            let mut s = server.lock().unwrap();
            s.body = Some(
                serde_json::to_vec(&Snapshot {
                    version: 5,
                    records: replication::records(&c).unwrap(),
                })
                .unwrap(),
            );
            s.revision = 1;
        }
        connect(&a, &http, &url, "u", "p").await.unwrap();
        sync_now(&a, &http).await.unwrap();
        let old: chunks::Manifest =
            serde_json::from_slice(server.lock().unwrap().body.as_ref().unwrap()).unwrap();
        assert_eq!(old.version, 6);
        connect(&b, &http, &url, "u", "p").await.unwrap();
        sync_now(&b, &http).await.unwrap();
        let (puts, gets) = {
            let s = server.lock().unwrap();
            (s.chunk_puts, s.chunk_gets)
        };
        sync_now(&a, &http).await.unwrap();
        sync_now(&b, &http).await.unwrap();
        {
            let s = server.lock().unwrap();
            assert_eq!((s.chunk_puts, s.chunk_gets), (puts, gets));
        }
        replication::set_progress(&*a.lock().await, 1, 0.73).unwrap();
        sync_now(&a, &http).await.unwrap();
        let new: chunks::Manifest =
            serde_json::from_slice(server.lock().unwrap().body.as_ref().unwrap()).unwrap();
        let article_hashes = |m: &chunks::Manifest| {
            m.chunks
                .iter()
                .filter(|p| p.kind == "article")
                .map(|p| p.hash.clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(article_hashes(&old), article_hashes(&new));
        assert_eq!(server.lock().unwrap().chunk_puts, puts + 1);
        sync_now(&b, &http).await.unwrap();
        assert_eq!(server.lock().unwrap().chunk_gets, gets + 1);
        assert_eq!(
            db::get_article(&*b.lock().await, 1)
                .unwrap()
                .reading_progress,
            0.73
        );
        task.abort();
    }

    #[tokio::test]
    async fn corrupt_or_missing_chunks_do_not_partially_import() {
        let (url, server, task) = server().await;
        let http = client();
        let a = database();
        let b = database();
        seed(&a).await;
        connect(&a, &http, &url, "u", "p").await.unwrap();
        sync_now(&a, &http).await.unwrap();
        connect(&b, &http, &url, "u", "p").await.unwrap();
        let (path, bytes) = {
            let s = server.lock().unwrap();
            let (p, b) = s.objects.iter().next_back().unwrap();
            (p.clone(), b.clone())
        };
        server
            .lock()
            .unwrap()
            .objects
            .insert(path.clone(), vec![b'x'; bytes.len()]);
        assert!(matches!(
            sync_now(&b, &http).await,
            Err(AppError::Coded("webdavInvalidData"))
        ));
        assert_eq!(
            b.lock()
                .await
                .query_row("SELECT count(*) FROM feeds", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
        server.lock().unwrap().objects.remove(&path);
        assert!(sync_now(&b, &http).await.is_err());
        assert_eq!(
            b.lock()
                .await
                .query_row("SELECT count(*) FROM feeds", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
        server.lock().unwrap().objects.insert(path, bytes);
        sync_now(&b, &http).await.unwrap();
        assert_eq!(
            db::get_article(&*b.lock().await, 1).unwrap().title,
            "Article"
        );
        task.abort();
    }

    #[tokio::test]
    async fn manifest_compare_and_swap_failure_keeps_old_snapshot_and_resumes() {
        let (url, server, task) = server().await;
        let http = client();
        let a = database();
        let b = database();
        seed(&a).await;
        connect(&a, &http, &url, "u", "p").await.unwrap();
        sync_now(&a, &http).await.unwrap();
        let before = server.lock().unwrap().body.clone();
        replication::set_progress(&*a.lock().await, 1, 0.5).unwrap();
        // The root fails after all immutable chunks have been published.
        server.lock().unwrap().fail_root = true;
        assert!(sync_now(&a, &http).await.is_err());
        assert_eq!(server.lock().unwrap().body, before);
        connect(&b, &http, &url, "u", "p").await.unwrap();
        sync_now(&b, &http).await.unwrap();
        assert_eq!(
            db::get_article(&*b.lock().await, 1)
                .unwrap()
                .reading_progress,
            0.0
        );
        server.lock().unwrap().fail_root = false;
        sync_now(&a, &http).await.unwrap();
        sync_now(&b, &http).await.unwrap();
        assert_eq!(
            db::get_article(&*b.lock().await, 1)
                .unwrap()
                .reading_progress,
            0.5
        );
        task.abort();
    }

    #[tokio::test]
    #[ignore = "large disk-backed integration test: transfers over 1.3 GiB of logical article data"]
    async fn library_over_1_3_gib_round_trips_with_bounded_chunks() {
        let (url, server, task) = server().await;
        let http = client();
        let dir = std::env::temp_dir().join(format!("papr-large-sync-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let a = Mutex::new(db::open(&dir.join("a.db")).unwrap());
        let b = Mutex::new(db::open(&dir.join("b.db")).unwrap());
        {
            let mut c = a.lock().await;
            let tx = c.transaction().unwrap();
            tx.execute(
                "INSERT INTO feeds(feed_url,title) VALUES('https://example.com/rss','Large test')",
                [],
            )
            .unwrap();
            let text = "large library article ".repeat(50_000); // 1.1 MB per article
            for i in 0..1400 {
                tx.execute("INSERT INTO articles(feed_id,guid,title,body_text) VALUES(1,?1,'Large article',?2)",params![format!("large-{i:05}"),text]).unwrap();
            }
            tx.commit().unwrap();
        }
        println!("Large fixture seeded; starting initial upload");
        connect(&a, &http, &url, "u", "p").await.unwrap();
        sync_now(&a, &http).await.unwrap();
        println!("Initial upload completed; starting download/import on second device");
        let m: chunks::Manifest =
            serde_json::from_slice(server.lock().unwrap().body.as_ref().unwrap()).unwrap();
        let raw: usize = m.chunks.iter().map(|p| p.raw_bytes).sum();
        let zipped: usize = m.chunks.iter().map(|p| p.bytes).sum();
        assert!(raw > 13 * 1024 * 1024 * 1024 / 10);
        assert!(m.chunks.iter().all(|p| p.raw_bytes <= chunks::MAX_CHUNK));
        connect(&b, &http, &url, "u", "p").await.unwrap();
        sync_now(&b, &http).await.unwrap();
        assert_eq!(
            b.lock()
                .await
                .query_row(
                    "SELECT count(*),sum(length(body_text)) FROM articles",
                    [],
                    |r| Ok((r.get::<_, usize>(0)?, r.get::<_, usize>(1)?))
                )
                .unwrap(),
            (1400, "large library article ".len() * 50_000 * 1400)
        );
        let (puts, gets) = {
            let s = server.lock().unwrap();
            (s.chunk_puts, s.chunk_gets)
        };
        replication::set_progress(&*a.lock().await, 1, 0.42).unwrap();
        sync_now(&a, &http).await.unwrap();
        sync_now(&b, &http).await.unwrap();
        assert_eq!(server.lock().unwrap().chunk_puts, puts + 1);
        assert_eq!(server.lock().unwrap().chunk_gets, gets + 1);
        assert_eq!(
            db::get_article(&*b.lock().await, 1)
                .unwrap()
                .reading_progress,
            0.42
        );
        println!("Large sync: {raw} uncompressed bytes, {zipped} compressed bytes, {} chunks; subsequent progress edit transferred one chunk in each direction",m.chunks.len());
        drop(a);
        drop(b);
        task.abort();
        std::fs::remove_dir_all(&dir).unwrap();
    }
    #[test]
    fn validates_directory_urls() {
        assert_eq!(
            base_url(" https://example.com/dav/papr ").unwrap().as_str(),
            "https://example.com/dav/papr/"
        );
        for url in [
            "file:///tmp/test",
            "https://user:password@example.com/dav",
            "https://example.com/dav?token=secret",
            "https://example.com/#fragment",
        ] {
            assert!(base_url(url).is_err());
        }
    }
}
