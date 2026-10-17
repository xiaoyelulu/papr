//! Explicit, previewed remote GC: quarantine, exclusive DAV lock and CAS fence.
use super::*;
use inventory::{Listing, Object};
use std::{collections::BTreeMap, time::Instant};

const LEDGER: &str = "papr-gc-candidates-v1.json";
const PLAN: &str = "webdav_gc_preview";
const MAX_META: usize = 16 * 1024 * 1024;
const SESSION_TTL: i64 = 24 * 60 * 60;
fn stale() -> AppError {
    AppError::code("webdavGcStale")
}
fn unsupported() -> AppError {
    AppError::code("webdavGcLockRequired")
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Candidate {
    object: Object,
    since: i64,
}
#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Ledger {
    version: u32,
    candidates: BTreeMap<String, Candidate>,
}
#[derive(Serialize, Deserialize)]
struct Plan {
    token: String,
    target: String,
    days: u32,
    issued: i64,
    root_etag: String,
    ledger_etag: String,
    listing_hash: String,
    candidates: Vec<Object>,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GcPreview {
    pub token: String,
    pub retention_days: u32,
    pub total_chunks: usize,
    pub referenced_chunks: usize,
    pub quarantined_chunks: usize,
    pub eligible_chunks: usize,
    pub eligible_bytes: u64,
    pub examples: Vec<String>,
}
#[derive(Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GcResult {
    pub deleted_chunks: usize,
    pub deleted_bytes: u64,
}
fn signature(listing: &Listing) -> AppResult<String> {
    use sha2::{Digest, Sha256};
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&listing.objects).map_err(|_| inventory::invalid())?)
    ))
}
fn active(listing: &Listing) -> bool {
    listing.objects.values().any(|o| {
        inventory::session_name(&o.name) && listing.now.saturating_sub(o.modified) < SESSION_TTL
    })
}
fn checked_root(root: Root, etag: Option<String>) -> AppResult<(chunks::Manifest, String)> {
    match (root, etag) {
        (Root::Manifest(m), Some(e)) => Ok((m, e)),
        _ => Err(AppError::code("webdavGcSyncFirst")),
    }
}
fn protect_root(listing: &Listing, manifest: &chunks::Manifest) -> AppResult<()> {
    if manifest.chunks.iter().any(|p| {
        listing
            .objects
            .get(&chunks::path(p))
            .is_none_or(|o| o.bytes != p.bytes as u64)
    }) {
        return Err(AppError::code("webdavGcIncomplete"));
    }
    Ok(())
}

struct Lock {
    token: String,
    expires: Instant,
}
impl Lock {
    fn condition(&self, c: &Credentials) -> String {
        format!("<{}> ({})", c.url, self.token)
    }
    async fn acquire(http: &Client, c: &Credentials) -> AppResult<Self> {
        let r=request(http,c,reqwest::Method::from_bytes(b"LOCK").unwrap(),c.url.clone())
            .header("Depth","infinity").header("Timeout","Second-120")
            .header(header::CONTENT_TYPE,"application/xml; charset=utf-8")
            .body("<d:lockinfo xmlns:d=\"DAV:\"><d:lockscope><d:exclusive/></d:lockscope><d:locktype><d:write/></d:locktype><d:owner>Papr remote cleanup</d:owner></d:lockinfo>")
            .timeout(Duration::from_secs(25)).send().await.map_err(network_error)?;
        if r.status().as_u16() == 423 {
            transport::drain(r).await;
            return Err(AppError::code("webdavGcBusy"));
        }
        if !r.status().is_success() {
            transport::drain(r).await;
            return Err(unsupported());
        }
        let token = r
            .headers()
            .get("Lock-Token")
            .and_then(|v| v.to_str().ok())
            .filter(|t| t.starts_with('<') && t.ends_with('>') && !t.contains(['\r', '\n']))
            .ok_or_else(unsupported)?
            .to_owned();
        let mut lock = Self {
            token,
            expires: Instant::now(),
        };
        let check = async {
            let body = inventory::read_body(r, 64 * 1024).await?;
            let xml = inventory::xml(&body)?;
            fn find<'a>(n: &'a inventory::Node, name: &str) -> Option<&'a inventory::Node> {
                if n.name == name {
                    Some(n)
                } else {
                    n.children.iter().find_map(|n| find(n, name))
                }
            }
            let a = find(&xml, "activelock").ok_or_else(unsupported)?;
            if a.text("depth") != Some("infinity")
                || a.child("lockscope")
                    .and_then(|n| n.child("exclusive"))
                    .is_none()
                || a.child("locktype").and_then(|n| n.child("write")).is_none()
                || a.child("locktoken").and_then(|n| n.text("href"))
                    != Some(lock.token.trim_start_matches('<').trim_end_matches('>'))
            {
                return Err(unsupported());
            }
            let secs = a
                .text("timeout")
                .and_then(|s| s.strip_prefix("Second-"))
                .and_then(|s| s.parse::<u64>().ok())
                .filter(|n| *n >= 30)
                .ok_or_else(unsupported)?;
            lock.expires = Instant::now() + Duration::from_secs(secs.min(120));
            Ok(())
        }
        .await;
        if let Err(e) = check {
            let _ = lock.release(http, c).await;
            return Err(e);
        }
        Ok(lock)
    }
    async fn refresh(&mut self, http: &Client, c: &Credentials) -> AppResult<()> {
        if self.expires.saturating_duration_since(Instant::now()) > Duration::from_secs(45) {
            return Ok(());
        }
        let r = request(
            http,
            c,
            reqwest::Method::from_bytes(b"LOCK").unwrap(),
            c.url.clone(),
        )
        .header("If", format!("({})", self.token))
        .header("Timeout", "Second-120")
        .timeout(Duration::from_secs(15))
        .send()
        .await
        .map_err(network_error)?;
        if !r.status().is_success() {
            transport::drain(r).await;
            return Err(AppError::code("webdavGcBusy"));
        }
        let body = inventory::read_body(r, 64 * 1024).await?;
        let text = String::from_utf8_lossy(&body);
        // Refresh response must explicitly grant enough lifetime; otherwise
        // stop before any further deletes. Acquisition validates scope/depth.
        let root = inventory::xml(text.as_bytes())?;
        fn timeout(n: &inventory::Node) -> Option<u64> {
            if n.name == "timeout" {
                n.text.trim().strip_prefix("Second-")?.parse().ok()
            } else {
                n.children.iter().find_map(timeout)
            }
        }
        let secs = timeout(&root)
            .filter(|n| *n >= 30)
            .ok_or_else(unsupported)?;
        self.expires = Instant::now() + Duration::from_secs(secs.min(120));
        Ok(())
    }
    async fn release(&self, http: &Client, c: &Credentials) -> AppResult<()> {
        let r = request(
            http,
            c,
            reqwest::Method::from_bytes(b"UNLOCK").unwrap(),
            c.url.clone(),
        )
        .header("Lock-Token", &self.token)
        .timeout(Duration::from_secs(15))
        .send()
        .await
        .map_err(network_error)?;
        let ok = r.status().is_success();
        transport::drain(r).await;
        if ok {
            Ok(())
        } else {
            Err(AppError::code("webdavGcUnlockFailed"))
        }
    }
}
async fn locked<T>(http: &Client, c: &Credentials, work: impl AsyncWork<T>) -> AppResult<T> {
    let mut lock = Lock::acquire(http, c).await?;
    let result = progress::run(work.run(&mut lock)).await;
    let released = lock.release(http, c).await;
    match result {
        Err(e) => Err(e),
        Ok(value) => {
            released?;
            Ok(value)
        }
    }
}
// A boxed lifetime-bound future avoids spawning work or leaking a DAV lock on
// normal errors/cancellation. Process termination is covered by server timeout.
trait AsyncWork<T> {
    fn run<'a>(
        self,
        lock: &'a mut Lock,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = AppResult<T>> + Send + 'a>>
    where
        Self: 'a;
}

async fn read_ledger(http: &Client, c: &Credentials) -> AppResult<(Ledger, Option<String>)> {
    let r = request(http, c, reqwest::Method::GET, c.url.join(LEDGER)?)
        .header(header::CACHE_CONTROL, "no-cache")
        .send()
        .await
        .map_err(network_error)?;
    if r.status() == StatusCode::NOT_FOUND {
        transport::drain(r).await;
        return Ok((
            Ledger {
                version: 1,
                ..Default::default()
            },
            None,
        ));
    }
    let r = transport::checked(r).await.map_err(|e| e.error)?;
    let etag = transport::etag(&r).ok_or_else(|| AppError::code("webdavEtagRequired"))?;
    let ledger: Ledger = serde_json::from_slice(&inventory::read_body(r, MAX_META).await?)
        .map_err(|_| inventory::invalid())?;
    if ledger.version != 1
        || ledger.candidates.len() > 200_000
        || ledger.candidates.iter().any(|(n, v)| {
            (!inventory::chunk_name(n) && !inventory::session_name(n))
                || *n != v.object.name
                || v.since < 0
        })
    {
        return Err(inventory::invalid());
    }
    Ok((ledger, Some(etag)))
}
async fn put_locked(
    http: &Client,
    c: &Credentials,
    lock: &Lock,
    name: &str,
    etag: Option<&str>,
    body: Vec<u8>,
) -> AppResult<()> {
    let mut req = request(http, c, reqwest::Method::PUT, c.url.join(name)?)
        .header("If", lock.condition(c))
        .header(header::CONTENT_TYPE, "application/json");
    req = if let Some(e) = etag {
        req.header(header::IF_MATCH, e)
    } else {
        req.header(header::IF_NONE_MATCH, "*")
    };
    let r = req
        .body(body)
        .timeout(Duration::from_secs(25))
        .send()
        .await
        .map_err(network_error)?;
    if matches!(r.status().as_u16(), 412 | 423) {
        transport::drain(r).await;
        return Err(stale());
    }
    let r = transport::checked(r).await.map_err(|e| e.error)?;
    transport::drain(r).await;
    Ok(())
}
async fn preview_inner(
    db: &Db,
    http: &Client,
    c: &Credentials,
    days: u32,
    lock: &mut Lock,
) -> AppResult<GcPreview> {
    progress::phase("gcScanning", 0);
    let (root, etag) = download(http, c).await?;
    let (manifest, etag) = checked_root(root, etag)?;
    let listing = inventory::list(http, c)
        .await?
        .ok_or_else(|| AppError::code("webdavGcListingRequired"))?;
    if active(&listing) {
        return Err(AppError::code("webdavGcBusy"));
    }
    protect_root(&listing, &manifest)?;
    let (mut ledger, old_etag) = read_ledger(http, c).await?;
    let referenced = manifest
        .chunks
        .iter()
        .map(chunks::path)
        .collect::<std::collections::BTreeSet<_>>();
    ledger
        .candidates
        .retain(|name, _| listing.objects.contains_key(name) && !referenced.contains(name));
    for (name, object) in &listing.objects {
        if (!inventory::chunk_name(name) && !inventory::session_name(name))
            || referenced.contains(name)
        {
            continue;
        }
        let old = ledger.candidates.get(name);
        if old.is_none_or(|v| v.object != *object || v.since > listing.now) {
            ledger.candidates.insert(
                name.clone(),
                Candidate {
                    object: object.clone(),
                    since: listing.now,
                },
            );
        }
    }
    lock.refresh(http, c).await?;
    let body = serde_json::to_vec(&ledger).map_err(|_| inventory::invalid())?;
    if body.len() > MAX_META {
        return Err(inventory::invalid());
    }
    put_locked(http, c, lock, LEDGER, old_etag.as_deref(), body).await?;
    let (saved, ledger_etag) = read_ledger(http, c).await?;
    if serde_json::to_vec(&saved).ok() != serde_json::to_vec(&ledger).ok() {
        return Err(stale());
    }
    let eligible = ledger
        .candidates
        .values()
        .filter(|v| {
            listing.now.saturating_sub(v.since) >= i64::from(days) * 86400
                && v.object.modified <= listing.now
        })
        .map(|v| v.object.clone())
        .collect::<Vec<_>>();
    let token = replication::probe_id(&*db.lock().await)?;
    let plan = Plan {
        token: token.clone(),
        target: upload_target(c),
        days,
        issued: listing.now,
        root_etag: etag,
        ledger_etag: ledger_etag.ok_or_else(stale)?,
        listing_hash: signature(&listing)?,
        candidates: eligible.clone(),
    };
    db::set_setting(
        &*db.lock().await,
        PLAN,
        &serde_json::to_string(&plan).map_err(|_| inventory::invalid())?,
    )?;
    Ok(GcPreview {
        token,
        retention_days: days,
        total_chunks: listing
            .objects
            .keys()
            .filter(|n| inventory::chunk_name(n))
            .count(),
        referenced_chunks: referenced.len(),
        quarantined_chunks: ledger.candidates.len() - eligible.len(),
        eligible_chunks: eligible.len(),
        eligible_bytes: eligible.iter().map(|o| o.bytes).sum(),
        examples: eligible.iter().take(8).map(|o| o.name.clone()).collect(),
    })
}
async fn execute_inner(
    db: &Db,
    http: &Client,
    c: &Credentials,
    token: &str,
    lock: &mut Lock,
) -> AppResult<GcResult> {
    let plan: Plan =
        serde_json::from_str(&db::get_setting(&*db.lock().await, PLAN)?.ok_or_else(stale)?)
            .map_err(|_| stale())?;
    if token != plan.token || plan.target != upload_target(c) {
        return Err(stale());
    }
    progress::phase("gcScanning", 0);
    let (root, etag) = download(http, c).await?;
    let (manifest, etag) = checked_root(root, etag)?;
    let listing = inventory::list(http, c)
        .await?
        .ok_or_else(|| AppError::code("webdavGcListingRequired"))?;
    if active(&listing) {
        return Err(AppError::code("webdavGcBusy"));
    }
    protect_root(&listing, &manifest)?;
    if etag != plan.root_etag
        || signature(&listing)? != plan.listing_hash
        || listing.now < plan.issued
        || listing.now - plan.issued > 900
    {
        return Err(stale());
    }
    let (ledger, ledger_etag) = read_ledger(http, c).await?;
    if ledger_etag.as_deref() != Some(&plan.ledger_etag) {
        return Err(stale());
    }
    let referenced = manifest
        .chunks
        .iter()
        .map(chunks::path)
        .collect::<std::collections::BTreeSet<_>>();
    for o in &plan.candidates {
        let candidate = ledger.candidates.get(&o.name).ok_or_else(stale)?;
        if referenced.contains(&o.name)
            || candidate.object != *o
            || listing.now.saturating_sub(candidate.since) < i64::from(plan.days) * 86400
        {
            return Err(stale());
        }
    }
    db::set_setting(&*db.lock().await, PLAN, "")?; // consume preview before any deletion
    if plan.candidates.is_empty() {
        return Ok(GcResult::default());
    }
    // Fence writers that fetched the pre-GC root. JSON whitespace changes the
    // representation without changing v7 semantics. Verify a NEW strong ETag
    // before deleting anything; an old If-Match must now fail on the server.
    lock.refresh(http, c).await?;
    let nonce = replication::probe_id(&*db.lock().await)?;
    let fenced = fenced_body(&manifest, &nonce)?;
    put_locked(http, c, lock, FILE, Some(&etag), fenced).await?;
    let (root, after) = download(http, c).await?;
    if after.as_deref() == Some(&etag) || !matches!(root,Root::Manifest(m) if m==manifest) {
        return Err(stale());
    }
    progress::phase("gcDeleting", plan.candidates.len());
    let mut result = GcResult::default();
    // Refresh between bounded batches. Each DELETE independently carries the
    // collection lock token AND object ETag; expired locks cannot authorize it.
    for batch in plan.candidates.chunks(c.options.concurrency.min(4)) {
        lock.refresh(http, c).await?;
        let condition = lock.condition(c);
        let jobs = batch.iter().cloned().map(|o| {
            let condition = condition.clone();
            async move {
                let r = request(http, c, reqwest::Method::DELETE, c.url.join(&o.name)?)
                    .header("If", condition)
                    .header(header::IF_MATCH, &o.etag)
                    .timeout(Duration::from_secs(25))
                    .send()
                    .await
                    .map_err(network_error)?;
                let status = r.status();
                transport::drain(r).await;
                if !matches!(status.as_u16(), 200 | 204 | 404) {
                    return Err(stale());
                }
                progress::completed();
                Ok::<_, AppError>((status != StatusCode::NOT_FOUND, o.bytes))
            }
        });
        let mut jobs = stream::iter(jobs).buffer_unordered(c.options.concurrency.min(4));
        while let Some(r) = jobs.next().await {
            let (deleted, bytes) = r?;
            if deleted {
                result.deleted_chunks += 1;
                result.deleted_bytes += bytes;
            }
        }
    }
    Ok(result)
}

struct PreviewWork<'a> {
    db: &'a Db,
    http: &'a Client,
    c: &'a Credentials,
    days: u32,
}
impl AsyncWork<GcPreview> for PreviewWork<'_> {
    fn run<'a>(
        self,
        lock: &'a mut Lock,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = AppResult<GcPreview>> + Send + 'a>>
    where
        Self: 'a,
    {
        Box::pin(preview_inner(self.db, self.http, self.c, self.days, lock))
    }
}
struct ExecuteWork<'a> {
    db: &'a Db,
    http: &'a Client,
    c: &'a Credentials,
    token: &'a str,
}
impl AsyncWork<GcResult> for ExecuteWork<'_> {
    fn run<'a>(
        self,
        lock: &'a mut Lock,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = AppResult<GcResult>> + Send + 'a>>
    where
        Self: 'a,
    {
        Box::pin(execute_inner(self.db, self.http, self.c, self.token, lock))
    }
}
pub async fn gc_preview(db: &Db, http: &Client, retention_days: u32) -> AppResult<GcPreview> {
    if !(7..=365).contains(&retention_days) {
        return Err(AppError::code("webdavGcRetention"));
    }
    let _guard = SYNC_LOCK
        .try_lock()
        .map_err(|_| AppError::code("webdavBusy"))?;
    let c = credentials(&*db.lock().await)?.ok_or_else(|| AppError::code("webdavNotConnected"))?;
    let result = locked(
        http,
        &c,
        PreviewWork {
            db,
            http,
            c: &c,
            days: retention_days,
        },
    )
    .await;
    if result.is_ok() {
        db::set_setting(
            &*db.lock().await,
            "webdav_gc_retention_days",
            &retention_days.to_string(),
        )?;
        progress::phase("gcPreview", 0);
    }
    result
}
pub async fn gc_execute(db: &Db, http: &Client, token: &str) -> AppResult<GcResult> {
    let _guard = SYNC_LOCK
        .try_lock()
        .map_err(|_| AppError::code("webdavBusy"))?;
    let c = credentials(&*db.lock().await)?.ok_or_else(|| AppError::code("webdavNotConnected"))?;
    let result = locked(
        http,
        &c,
        ExecuteWork {
            db,
            http,
            c: &c,
            token,
        },
    )
    .await;
    if result.is_ok() {
        progress::phase("gcDone", 0);
    }
    result
}

/// An unguessable, short-lived remote activity marker protects new readers
/// during GC. Interrupted clients expire after 24h by SERVER modification time.
/// The CAS root fence also protects writers on versions without this marker.
pub(super) async fn session_start(db: &Db, http: &Client, c: &Credentials) -> AppResult<String> {
    let name = format!(
        "papr-sync-active-{}.json",
        replication::probe_id(&*db.lock().await)?
    );
    let r = request(http, c, reqwest::Method::PUT, c.url.join(&name)?)
        .header(header::IF_NONE_MATCH, "*")
        .header(header::CONTENT_TYPE, "application/json")
        .body("{\"version\":1}")
        .send()
        .await
        .map_err(network_error)?;
    let r = transport::checked(r).await.map_err(|e| e.error)?;
    transport::drain(r).await;
    Ok(name)
}
pub(super) async fn session_end(http: &Client, c: &Credentials, name: &str) {
    if let Ok(r) = request(http, c, reqwest::Method::DELETE, c.url.join(name).unwrap())
        .timeout(Duration::from_secs(15))
        .send()
        .await
    {
        transport::drain(r).await;
    }
}

fn fenced_body(manifest: &chunks::Manifest, nonce: &str) -> AppResult<Vec<u8>> {
    let mut body = serde_json::to_vec(manifest).map_err(|_| inventory::invalid())?;
    let extra = 1 + nonce.len().saturating_mul(8);
    if body.len().saturating_add(extra) > 16 * 1024 * 1024 {
        return Err(AppError::code("webdavTooLarge"));
    }
    body.push(b'\n');
    for b in nonce.bytes() {
        for bit in 0..8 {
            body.push(if b & (1 << bit) == 0 { b' ' } else { b'\t' });
        }
    }
    Ok(body)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fence_refuses_to_make_a_valid_near_limit_manifest_unreadable() {
        let mut manifest = chunks::Manifest::empty();
        let part = chunks::Part {
            hash: "0".repeat(64),
            kind: "article".into(),
            bucket: 0,
            bytes: 1,
            raw_bytes: 1,
            records: 1,
        };
        let base = serde_json::to_vec(&manifest).unwrap().len();
        let each = serde_json::to_vec(&part).unwrap().len() + 1;
        let count = (16 * 1024 * 1024 - base + 1) / each;
        manifest.chunks = (0..count)
            .map(|i| chunks::Part {
                hash: format!("{i:064x}"),
                ..part.clone()
            })
            .collect();
        manifest.validate().unwrap();
        assert!(serde_json::to_vec(&manifest).unwrap().len() <= 16 * 1024 * 1024);
        assert!(matches!(
            fenced_body(&manifest, &"a".repeat(32)),
            Err(AppError::Coded("webdavTooLarge"))
        ));
    }
}
