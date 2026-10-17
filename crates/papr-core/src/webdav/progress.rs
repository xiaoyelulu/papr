//! Progress is independent of SQLite's writer lock, including while indexing.
use serde::Serialize;
use std::{
    collections::VecDeque,
    sync::{LazyLock, Mutex},
    time::Instant,
};

#[derive(Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Progress {
    pub running: bool,
    pub phase: &'static str,
    pub completed: usize,
    pub total: usize,
    pub downloaded: u64,
    pub uploaded: u64,
    pub bytes_per_second: u64,
    pub retries: usize,
    pub elapsed_ms: u64,
    pub active: usize,
    pub limit: usize,
    pub uploaded_chunks: usize,
    pub downloaded_chunks: usize,
    pub reused_chunks: usize,
    pub planned_chunks: usize,
    pub upload_total: usize,
}
struct State {
    value: Progress,
    start: Instant,
    samples: VecDeque<(Instant, u64)>,
}
static STATE: LazyLock<Mutex<State>> = LazyLock::new(|| {
    Mutex::new(State {
        value: Progress::default(),
        start: Instant::now(),
        samples: VecDeque::new(),
    })
});
fn edit(f: impl FnOnce(&mut State)) {
    f(&mut STATE.lock().unwrap_or_else(|e| e.into_inner()));
}
pub(super) fn start() {
    edit(|s| {
        s.value = Progress {
            running: true,
            phase: "connecting",
            ..Default::default()
        };
        s.start = Instant::now();
        s.samples.clear();
    });
}
pub(super) fn phase(phase: &'static str, total: usize) {
    edit(|s| {
        s.value.phase = phase;
        s.value.total = total;
        s.value.completed = 0;
        s.samples.clear();
        s.samples
            .push_back((Instant::now(), s.value.downloaded + s.value.uploaded));
    });
}
pub(super) fn bytes(count: usize, upload: bool) {
    edit(|s| {
        if upload {
            s.value.uploaded += count as u64;
        } else {
            s.value.downloaded += count as u64;
        }
        sample(s);
    });
}
fn sample(s: &mut State) {
    let now = Instant::now();
    s.samples
        .push_back((now, s.value.downloaded + s.value.uploaded));
    while s.samples.len() > 2 && now.duration_since(s.samples[1].0).as_secs_f64() > 3.0 {
        s.samples.pop_front();
    }
}
pub(super) fn completed() {
    edit(|s| s.value.completed += 1);
}
pub(super) fn limit(n:usize){edit(|s|s.value.limit=n);}
pub(super) fn upload_plan(total:usize,reused:usize){edit(|s|{s.value.upload_total=total;s.value.planned_chunks=total-reused;s.value.reused_chunks=reused;s.value.uploaded_chunks=0;});}
pub(super) fn uploaded_chunk(uploaded:bool){edit(|s|{s.value.planned_chunks=s.value.planned_chunks.saturating_sub(1);if uploaded{s.value.uploaded_chunks+=1;}else{s.value.reused_chunks+=1;}});}
pub(super) fn downloaded_chunk(){edit(|s|s.value.downloaded_chunks+=1);}
pub(super) struct Job;
pub(super) fn job()->Job{edit(|s|s.value.active+=1);Job}
impl Drop for Job {fn drop(&mut self){edit(|s|s.value.active=s.value.active.saturating_sub(1));}}
pub(super) fn retry() {
    edit(|s| s.value.retries += 1);
}
pub(super) fn finish(ok: bool) {
    edit(|s| {
        s.value.running = false;
        s.value.phase = if ok { "done" } else { "failed" };
        s.value.elapsed_ms = s.start.elapsed().as_millis() as u64;
        s.samples.clear();
    });
}
pub fn get() -> Progress {
    let mut result = Progress::default();
    edit(|s| {
        result = s.value.clone();
        if result.running {
            result.elapsed_ms = s.start.elapsed().as_millis() as u64;
        }
        if result.running
            && ["download", "upload", "connecting", "publishing"].contains(&result.phase)
        {
            sample(s);
            if let Some((t, n)) = s.samples.front() {
                let elapsed = t.elapsed().as_secs_f64();
                if elapsed >= 0.25 {
                    result.bytes_per_second =
                        ((result.downloaded + result.uploaded - n) as f64 / elapsed) as u64;
                }
            }
        }
    });
    result
}

static CANCEL: LazyLock<tokio::sync::watch::Sender<bool>> =
    LazyLock::new(|| tokio::sync::watch::channel(false).0);
pub fn cancel() {
    if get().running {
        CANCEL.send_replace(true);
    }
}
pub(super) async fn run<T>(
    work: impl std::future::Future<Output = super::AppResult<T>>,
) -> super::AppResult<T> {
    CANCEL.send_replace(false);
    let mut cancelled = CANCEL.subscribe();
    start();
    let result = tokio::select! {biased;_ = cancelled.changed()=>Err(super::AppError::code("webdavCancelled")),result=work=>result};
    finish(result.is_ok());
    if matches!(&result, Err(super::AppError::Coded("webdavCancelled"))) {
        edit(|s| s.value.phase = "cancelled");
    }
    if matches!(&result, Err(super::AppError::Coded("webdavNeedsResolution"))) {
        edit(|s| s.value.phase = "needsResolution");
    }
    result
}

pub(super) fn clear() {
    edit(|s| s.value = Progress::default());
}
