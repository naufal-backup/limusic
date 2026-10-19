//! Downloads: the one path that turns a YouTube stream into a file the user keeps.
//!
//! Playback streams a track and lets it go; mpv's cache is not a library. This writes the bytes to
//! disk, tags them, and records the result in `db::downloads`, so a track survives with no network
//! at all. Nothing new is invented for playing it back: the saved file is an ordinary tagged audio
//! file, so the moment its folder is added to Local Music it is a `SongItem` like any other
//! (`local.rs`), and the Downloads view is only a second door to the same bytes.
//!
//! The work is one job at a time — a queue drained by a single background task — mirrored to the UI
//! through `download-progress` events exactly as an import is (`import.rs`), and cancellable between
//! chunks. Streams are asked for the MP4/AAC container (`find_format_preferring`) so the saved file
//! can be tagged and opened by other software; a track with no AAC still downloads as Opus, only
//! untagged.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use innertube::SongItem;
use lofty::config::WriteOptions;
use lofty::picture::{MimeType, Picture, PictureType};
use lofty::tag::{Accessor, ItemKey, Tag, TagExt, TagType};
use tauri::{AppHandle, Emitter, Manager};
use tokio::io::AsyncWriteExt;

use crate::db::Download;
use crate::state::AppState;

/// Where the downloads folder lives, as a setting. Absent means the default below the database.
const DIR_SETTING: &str = "download_dir";

/// The one thing a caller has to know about a file this module writes: the track that came back
/// untagged, or `cancelled` when the user stopped it. Both read as a failed row in the UI.
const CANCELLED: &str = "cancelled";

/// Where downloads land: the folder the user picked, else `Downloads` beside the database. Read
/// fresh each time — the setting can change while a job runs and the next track should follow it.
pub fn dir(app: &AppHandle, db: &crate::db::Db) -> PathBuf {
    match db.get_setting(DIR_SETTING).map(|s| s.trim().to_owned()).filter(|s| !s.is_empty()) {
        Some(path) => PathBuf::from(path),
        None => default_dir(app),
    }
}

fn default_dir(app: &AppHandle) -> PathBuf {
    app.path().app_data_dir().unwrap_or_else(|_| std::env::temp_dir()).join("Downloads")
}

/// Whether a folder is usable as the download target. A path that exists but is not a directory
/// (a stray file, say) is refused rather than half-used.
pub fn usable_dir(path: &str) -> Result<(), String> {
    let p = Path::new(path);
    if p.is_dir() {
        return Ok(());
    }
    if p.exists() {
        return Err("not a folder".into());
    }
    std::fs::create_dir_all(p).map_err(|e| e.to_string())
}

// --- job state ------------------------------------------------------------------------------------
//
// One job per process, in a static rather than an AppState field, exactly like the importer: there
// is one Downloads view and one queue, and a second `start` appends to the job already running
// instead of racing it.

/// The bit of a `SongItem` a download needs, copied out of the event-shaped struct so the job does
/// not hold a whole queue row per track.
#[derive(Clone)]
struct MiniSong {
    video_id: String,
    title: String,
    artists: String,
    album: String,
    duration_secs: i64,
    is_upload: bool,
    thumbnail: Option<String>,
}

impl MiniSong {
    fn of(s: &SongItem) -> Self {
        MiniSong {
            video_id: s.video_id.clone(),
            title: s.title.clone(),
            artists: s.artists.clone(),
            album: s.album.clone().unwrap_or_default(),
            duration_secs: parse_duration(s.duration.as_deref()),
            is_upload: s.is_upload,
            thumbnail: s.thumbnail.clone(),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
enum Status {
    Queued,
    Active,
    Done,
    Failed,
    Cancelled,
}

struct Entry {
    song: MiniSong,
    status: Status,
    /// 0..1 for the active row; 1 once done.
    progress: f64,
    error: Option<String>,
}

/// What the Downloads view draws, small enough to send four times a second.
#[derive(Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Item {
    video_id: String,
    title: String,
    artists: String,
    thumbnail: Option<String>,
    status: Status,
    progress: f64,
    error: Option<String>,
}

#[derive(Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Snapshot {
    items: Vec<Item>,
    /// A worker is draining the queue right now.
    running: bool,
    done: usize,
    failed: usize,
}

impl Snapshot {
    fn of(items: &[Entry], running: bool) -> Snapshot {
        Snapshot {
            items: items
                .iter()
                .map(|e| Item {
                    video_id: e.song.video_id.clone(),
                    title: e.song.title.clone(),
                    artists: e.song.artists.clone(),
                    thumbnail: e.song.thumbnail.clone(),
                    status: e.status,
                    progress: e.progress,
                    error: e.error.clone(),
                })
                .collect(),
            running,
            done: items.iter().filter(|e| e.status == Status::Done).count(),
            failed: items.iter().filter(|e| e.status == Status::Failed).count(),
        }
    }
}

struct Job {
    /// Only bumped when a job replaces one that had finished, so a worker can tell "mine" from
    /// "a newer run took over".
    gen: u64,
    /// Set by `cancel`. Checked between chunks and before each track.
    cancel: Arc<AtomicBool>,
    /// A worker task owns the queue while this is true.
    working: bool,
    items: Vec<Entry>,
    last_emit: Option<Instant>,
}

static JOB: Mutex<Option<Job>> = Mutex::new(None);
static NEXT_GEN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// The current job, or `None` once it has been put away.
pub fn status() -> Option<Snapshot> {
    JOB.lock().unwrap().as_ref().map(|j| Snapshot::of(&j.items, j.working))
}

/// Queue `songs` and make sure a worker is running. Tracks already downloaded (or already on this
/// disk as local files) are skipped; one that failed or was cancelled earlier goes back in the
/// queue rather than being refused.
pub fn start(state: &Arc<AppState>, songs: Vec<SongItem>) -> Snapshot {
    let have = state.db.download_ids();
    let mut g = JOB.lock().unwrap();
    if g.as_ref().is_none_or(|j| !j.working) {
        // Nothing running: a finished job's rows are stale, so this starts a fresh one.
        *g = Some(Job {
            gen: NEXT_GEN.fetch_add(1, Ordering::Relaxed),
            cancel: Arc::new(AtomicBool::new(false)),
            working: false,
            items: Vec::new(),
            last_emit: None,
        });
    }
    let job = g.as_mut().expect("job just ensured");
    for s in songs {
        let id = s.video_id.clone();
        // A local file is already on this disk, and a downloaded id is too.
        if crate::local::is_local_song(&id) || have.contains(&id) {
            continue;
        }
        match job.items.iter_mut().find(|e| e.song.video_id == id) {
            Some(e) => {
                if matches!(e.status, Status::Failed | Status::Cancelled) {
                    e.status = Status::Queued;
                    e.progress = 0.0;
                    e.error = None;
                }
            }
            None => job.items.push(Entry {
                song: MiniSong::of(&s),
                status: Status::Queued,
                progress: 0.0,
                error: None,
            }),
        }
    }
    let gen = job.gen;
    let snap = Snapshot::of(
        &job.items,
        job.working || job.items.iter().any(|e| e.status == Status::Queued),
    );
    drop(g);
    spawn_worker(state.clone(), gen);
    snap
}

/// Stop the running job, or put a finished one away. Cancelling keeps the rows on screen (marked
/// cancelled) so the user can see what did not get through; dismissing clears them.
pub fn cancel(state: &Arc<AppState>) {
    let mut clear = false;
    {
        let mut g = JOB.lock().unwrap();
        match g.as_mut() {
            Some(j) if j.working => j.cancel.store(true, Ordering::Relaxed),
            Some(_) => clear = true,
            None => {}
        }
    }
    if clear {
        *JOB.lock().unwrap() = None;
        let _ = state.app.emit("download-progress", Option::<Snapshot>::None);
    }
}

/// Hand the queue to a new worker, if there is anything in it to work on.
///
/// The "anything to work on" half is not an optimisation. [`run`] ends by asking for a successor so
/// a track queued while it was finishing is not stranded — and a worker with an empty queue returns
/// immediately. Without this check the two call each other forever: a tight loop that holds and
/// releases `JOB` thousands of times a second, pegs a core, and emits a full snapshot per pass. The
/// webview drowns in progress events and the click that would clear the job ("Dismiss") never gets
/// the lock — which is what "the app freezes when I dismiss the download popup" was.
fn claim_for_worker(job: &mut Job, gen: u64) -> bool {
    if job.gen != gen || job.working || !job.items.iter().any(|e| e.status == Status::Queued) {
        return false;
    }
    job.working = true;
    true
}

fn spawn_worker(state: Arc<AppState>, gen: u64) {
    let spawn = {
        let mut g = JOB.lock().unwrap();
        g.as_mut().is_some_and(|j| claim_for_worker(j, gen))
    };
    if !spawn {
        return;
    }
    tauri::async_runtime::spawn(async move { run(state, gen).await });
}

async fn run(state: Arc<AppState>, gen: u64) {
    loop {
        let next = {
            let mut g = JOB.lock().unwrap();
            let Some(j) = g.as_mut().filter(|j| j.gen == gen) else { return };
            match j.items.iter_mut().find(|e| e.status == Status::Queued) {
                Some(e) => {
                    e.status = Status::Active;
                    e.progress = 0.0;
                    Some(e.song.clone())
                }
                None => None,
            }
        };
        let Some(song) = next else { break };
        emit(&state, gen, true);

        let cancel = { JOB.lock().unwrap().as_ref().map(|j| j.cancel.clone()) };
        let Some(cancel) = cancel else { return };
        let covers = crate::local::covers_dir(&state.app);
        let result = download_one(&state, &song, &cancel, &covers, |written, total| {
            set_progress(&state, gen, &song.video_id, written, total)
        })
        .await;

        {
            let mut g = JOB.lock().unwrap();
            let Some(j) = g.as_mut().filter(|j| j.gen == gen) else { return };
            if let Some(e) = j.items.iter_mut().find(|e| e.song.video_id == song.video_id) {
                match result {
                    Ok(_) => {
                        e.status = Status::Done;
                        e.progress = 1.0;
                    }
                    Err(err) if err == CANCELLED => e.status = Status::Cancelled,
                    Err(err) => {
                        e.status = Status::Failed;
                        e.error = Some(err);
                    }
                }
            }
        }
        emit(&state, gen, true);

        if cancel.load(Ordering::Relaxed) {
            break;
        }
    }

    {
        let mut g = JOB.lock().unwrap();
        if let Some(j) = g.as_mut().filter(|j| j.gen == gen) {
            j.working = false;
            if j.cancel.load(Ordering::Relaxed) {
                for e in j.items.iter_mut().filter(|e| e.status == Status::Queued) {
                    e.status = Status::Cancelled;
                }
            }
        }
    }
    emit(&state, gen, true);
    // A track queued during the last iteration would otherwise sit there: the worker only stops for
    // an empty queue or a cancel, and this catches the race where one arrived in between.
    spawn_worker(state, gen);
}

/// Throttled like an import's: 4 Hz is enough for a progress bar and keeps a fast download from
/// turning into thousands of events.
fn emit(state: &AppState, gen: u64, force: bool) {
    let snapshot = {
        let mut g = JOB.lock().unwrap();
        let Some(j) = g.as_mut().filter(|j| j.gen == gen) else { return };
        if !force && j.last_emit.is_some_and(|t| t.elapsed() < Duration::from_millis(250)) {
            return;
        }
        j.last_emit = Some(Instant::now());
        Snapshot::of(&j.items, j.working)
    };
    let _ = state.app.emit("download-progress", snapshot);
}

fn set_progress(state: &AppState, gen: u64, video_id: &str, written: u64, total: Option<u64>) {
    {
        let mut g = JOB.lock().unwrap();
        let Some(j) = g.as_mut().filter(|j| j.gen == gen) else { return };
        if let Some(e) = j.items.iter_mut().find(|e| e.song.video_id == video_id) {
            e.progress = match total {
                Some(t) if t > 0 => (written as f64 / t as f64).clamp(0.0, 1.0),
                _ => 0.0,
            };
        }
    }
    emit(state, gen, false);
}

// --- one track ------------------------------------------------------------------------------------

async fn download_one(
    state: &Arc<AppState>,
    song: &MiniSong,
    cancel: &Arc<AtomicBool>,
    covers: &Path,
    on_progress: impl Fn(u64, Option<u64>),
) -> Result<Download, String> {
    // Resolve like playback does, but asking for the taggable container. Bypasses the URL cache on
    // purpose: a cached URL was chosen for streaming (Opus), and reusing it would defeat the point.
    let data = state
        .orchestrator
        .resolve(&song.video_id, song.is_upload, state.quality(), &state.disabled_clients(), true)
        .await
        .map_err(|e| e.to_string())?;

    let mut req = crate::http::client()
        .get(&data.stream_url)
        .header(reqwest::header::ACCEPT_ENCODING, "identity");
    for (k, v) in &data.headers {
        req = req.header(k.as_str(), v.as_str());
    }
    let resp = req.send().await.map_err(|e| e.to_string())?;
    if !resp.status().is_success() {
        return Err(format!("http {}", resp.status().as_u16()));
    }
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let ext = ext_for(data.itag, content_type.as_deref());
    let total = resp.content_length();

    let root = dir(&state.app, &state.db);
    let artist = crate::local::primary_artist(&song.artists).to_string();
    // An untaggable container (Opus-in-WebM) still has to read right in a music folder, where the
    // filename is the fallback when the tags are silent (local.rs `split_filename`) — so only those
    // carry "Artist - ".
    let untagged = tag_type_for(ext).is_none();
    let target = unique_target(
        &state.db,
        root,
        &artist,
        &song.album,
        &song.title,
        ext,
        untagged,
        &song.video_id,
    );
    if let Some(parent) = target.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(|e| e.to_string())?;
    }
    let part = target.with_file_name(format!("{}.part", file_name(&target)));

    // To a `.part` file first: a cancel or a failure then leaves nothing that looks like a track.
    let mut file = tokio::fs::File::create(&part).await.map_err(|e| e.to_string())?;
    let mut stream = resp.bytes_stream();
    let mut written: u64 = 0;
    while let Some(chunk) = stream.next().await {
        if cancel.load(Ordering::Relaxed) {
            drop(file);
            let _ = tokio::fs::remove_file(&part).await;
            return Err(CANCELLED.into());
        }
        let chunk = match chunk {
            Ok(c) => c,
            Err(e) => {
                drop(file);
                let _ = tokio::fs::remove_file(&part).await;
                return Err(e.to_string());
            }
        };
        if let Err(e) = file.write_all(&chunk).await {
            drop(file);
            let _ = tokio::fs::remove_file(&part).await;
            return Err(e.to_string());
        }
        written += chunk.len() as u64;
        on_progress(written, total);
    }
    file.flush().await.map_err(|e| e.to_string())?;
    drop(file);
    tokio::fs::rename(&part, &target).await.map_err(|e| e.to_string())?;

    // Artwork: embedded into the file and kept beside the covers directory, both best-effort — a
    // track without a picture is still a track.
    let mut cover_path = None;
    if let Some(url) = song.thumbnail.as_deref() {
        if let Some((bytes, ct)) = fetch_thumb(url).await {
            let (mime, cext) = if ct.as_deref().is_some_and(|c| c.contains("png")) {
                (MimeType::Png, "png")
            } else {
                (MimeType::Jpeg, "jpg")
            };
            let out = covers.join(format!("dl-{}.{cext}", song.video_id));
            if std::fs::create_dir_all(covers).is_ok() && std::fs::write(&out, &bytes).is_ok() {
                cover_path = Some(out.to_string_lossy().to_string());
            }
            write_tags(&target, ext, song, &artist, Some((&bytes, mime)));
        } else {
            write_tags(&target, ext, song, &artist, None);
        }
    } else {
        write_tags(&target, ext, song, &artist, None);
    }

    let size = std::fs::metadata(&target).map(|m| m.len() as i64).unwrap_or(written as i64);
    let row = Download {
        video_id: song.video_id.clone(),
        path: target.to_string_lossy().to_string(),
        title: song.title.clone(),
        artist: artist.clone(),
        album: song.album.clone(),
        duration_secs: song.duration_secs,
        cover: cover_path,
        itag: data.itag,
        size_bytes: size,
        created_at: crate::db::now_secs(),
    };
    state.db.put_download(&row);
    Ok(row)
}

/// The thumbnail bytes and their content type, at a size worth embedding. Best-effort.
async fn fetch_thumb(url: &str) -> Option<(Vec<u8>, Option<String>)> {
    let url = bigger_thumb(url);
    let resp = crate::http::client().get(&url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let ct = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let bytes = resp.bytes().await.ok()?;
    (!bytes.is_empty()).then(|| (bytes.to_vec(), ct))
}

/// `…=w60-h60-l90-rj` → `…=w1080-h1080-l90-rj`. The queue's thumbnail is sized for a list row; a
/// file's cover should not be. Anything without the size suffix is left alone.
fn bigger_thumb(url: &str) -> String {
    match url.split_once('=') {
        Some((base, params)) if params.starts_with('w') || params.starts_with('h') => {
            format!("{base}=w1080-h1080-l90-rj")
        }
        Some((base, params)) if params.starts_with('s') => format!("{base}=s1080"),
        _ => url.to_owned(),
    }
}

/// Write title/artist/album/album-artist and the cover into a file lofty can tag. A container it
/// cannot (WebM) is left as it came: the file still plays, and its name carries the artist.
fn write_tags(
    path: &Path,
    ext: &str,
    song: &MiniSong,
    artist: &str,
    cover: Option<(&[u8], MimeType)>,
) {
    let Some(tag_type) = tag_type_for(ext) else { return };
    let mut tag = Tag::new(tag_type);
    tag.set_title(song.title.clone());
    tag.set_artist(artist.to_owned());
    if !song.album.trim().is_empty() {
        tag.set_album(song.album.clone());
    }
    // The album artist, so a folder of downloads groups into albums instead of one per performer
    // (`local.rs::tagged_album_key`).
    tag.insert_text(ItemKey::AlbumArtist, artist.to_owned());
    if let Some((bytes, mime)) = cover {
        tag.push_picture(Picture::new_unchecked(
            PictureType::CoverFront,
            Some(mime),
            None,
            bytes.to_vec(),
        ));
    }
    let _ = tag.save_to_path(path, WriteOptions::default());
}

/// The tag format a saved file gets, or `None` when lofty cannot write its container.
fn tag_type_for(ext: &str) -> Option<TagType> {
    match ext {
        "m4a" => Some(TagType::Mp4Ilst),
        "mp3" => Some(TagType::Id3v2),
        "flac" | "opus" | "ogg" => Some(TagType::VorbisComments),
        // WebM/Matroska: lofty does not write it.
        _ => None,
    }
}

/// The file extension for a stream. The response's own `Content-Type` is the reliable answer;
/// `itag` is the fallback for a server that sent none, and MP4/AAC the last resort (what the
/// download resolve asks for, so what the itag should be).
fn ext_for(itag: i64, content_type: Option<&str>) -> &'static str {
    if let Some(ct) = content_type {
        let ct = ct.to_ascii_lowercase();
        if ct.contains("webm") {
            return "webm";
        }
        if ct.contains("ogg") {
            return "opus";
        }
        if ct.contains("mpeg") {
            return "mp3";
        }
        if ct.contains("mp4") || ct.contains("aac") {
            return "m4a";
        }
        if ct.contains("flac") {
            return "flac";
        }
    }
    match itag {
        249 | 250 | 251 | 774 => "webm",
        _ => "m4a",
    }
}

/// Where a track goes: `Artist/Album/Title.ext`, or `Artist/Title.ext` with no album. A same-named
/// track from a different videoId (a cover, a live cut) gets a numeric suffix rather than being
/// overwritten — the second download must not silently replace the first.
#[allow(clippy::too_many_arguments)]
fn unique_target(
    db: &crate::db::Db,
    root: PathBuf,
    artist: &str,
    album: &str,
    title: &str,
    ext: &str,
    untagged: bool,
    video_id: &str,
) -> PathBuf {
    let name = if untagged { format!("{artist} - {title}") } else { title.to_owned() };
    let stem = sanitize(&name);
    let mut dir = root.join(sanitize(artist));
    if !album.trim().is_empty() {
        dir = dir.join(sanitize(album));
    }
    let taken: Vec<String> =
        db.downloads().into_iter().filter(|d| d.video_id != video_id).map(|d| d.path).collect();
    let mut candidate = dir.join(format!("{stem}.{ext}"));
    let mut n = 2;
    while taken.iter().any(|p| Path::new(p) == candidate) {
        candidate = dir.join(format!("{stem} ({n}).{ext}"));
        n += 1;
    }
    candidate
}

/// A path component safe on every filesystem we ship to: the separators and the Windows-reserved
/// characters collapse to `-`, trailing dots and spaces go (Windows drops them silently), and an
/// empty result becomes "Unknown" rather than a directory called nothing.
fn sanitize(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for c in raw.chars() {
        match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => out.push('-'),
            c if c.is_control() => out.push('-'),
            c => out.push(c),
        }
    }
    let out = out.trim().trim_end_matches(['.', ' ']).to_owned();
    let out: String = out.chars().take(120).collect();
    if out.is_empty() {
        "Unknown".into()
    } else {
        out
    }
}

fn file_name(path: &Path) -> String {
    path.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default()
}

/// "3:47" / "1:08:20" → seconds. 0 when there is nothing to read (the player fills the real length
/// in once it plays; the download only wants it for the row's tags).
fn parse_duration(s: Option<&str>) -> i64 {
    let Some(s) = s else { return 0 };
    let mut secs = 0i64;
    for part in s.split(':') {
        let Ok(n) = part.trim().parse::<i64>() else { return 0 };
        secs = secs * 60 + n;
    }
    secs
}

// --- the user's own files --------------------------------------------------------------------------

/// Delete one download: the row and the bytes. The cover copy goes with it when it is ours.
pub fn remove(db: &crate::db::Db, video_id: &str) {
    let cover = db.downloads().into_iter().find(|d| d.video_id == video_id).and_then(|d| d.cover);
    if let Some(path) = db.delete_download(video_id) {
        let _ = std::fs::remove_file(&path);
        // An empty Artist/Album left behind would show up as an album in Local Music.
        if let Some(dir) = Path::new(&path).parent() {
            let _ = std::fs::remove_dir(dir);
        }
    }
    if let Some(cover) = cover {
        if Path::new(&cover).file_name().is_some_and(|n| n.to_string_lossy().starts_with("dl-")) {
            let _ = std::fs::remove_file(cover);
        }
    }
}

/// Move everything already saved into `new_root`, keeping each file's `Artist/Album/Title` place,
/// and point its row at the new path — so changing the download folder takes the library along
/// instead of leaving it in a folder the app no longer looks at. Best-effort per track: one that
/// cannot be moved keeps the row it had (a row pointing at a missing file would break playback, and
/// a half-moved library is worse than one that stayed put). Returns how many files actually moved.
pub fn move_all(db: &crate::db::Db, old_root: &Path, new_root: &Path) -> usize {
    let mut moved = 0;
    for d in db.downloads() {
        let src = PathBuf::from(&d.path);
        if !src.is_file() {
            continue;
        }
        // Keep the file's own place under the old root, so a name that was arranged by hand
        // survives the move untouched.
        let dst = match src.strip_prefix(old_root).ok().filter(|r| !r.as_os_str().is_empty()) {
            Some(rel) => new_root.join(rel),
            // Outside the folder the setting named — an older build, a hand-edited setting, a file
            // moved in from elsewhere. Lay it out the way a fresh download would rather than
            // flattening it into the root and losing its artist in the process.
            None => canonical_target(db, new_root, &d, &src),
        };
        if dst == src {
            continue;
        }
        let dst = if dst.exists() { unique_path(&dst) } else { dst };
        if dst.parent().is_none_or(|p| std::fs::create_dir_all(p).is_err()) {
            continue;
        }
        if std::fs::rename(&src, &dst).is_err() {
            continue;
        }
        // The old album/artist, now empty, would linger as an empty album in Local Music.
        if let Some(dir) = src.parent() {
            let _ = std::fs::remove_dir(dir);
        }
        db.put_download(&Download { path: dst.to_string_lossy().to_string(), ..d });
        moved += 1;
    }
    moved
}

/// Where a row's bytes belong under `root`, named the way `download_one` would have named them:
/// `Artist/Album/Title.ext`, or `Artist - Title.ext` for a container lofty cannot tag. The
/// extension comes from the file on disk, since the row does not carry it.
fn canonical_target(db: &crate::db::Db, root: &Path, d: &Download, src: &Path) -> PathBuf {
    let name = |src: &Path| root.join(src.file_name().unwrap_or_default());
    let Some(ext) = src.extension().map(|e| e.to_string_lossy().to_ascii_lowercase()) else {
        return name(src);
    };
    if ext.is_empty() {
        return name(src);
    }
    let untagged = tag_type_for(&ext).is_none();
    unique_target(
        db,
        root.to_path_buf(),
        &d.artist,
        &d.album,
        &d.title,
        &ext,
        untagged,
        &d.video_id,
    )
}

/// `Song.m4a` → `Song (2).m4a` …, so a move never overwrites a file already at the destination.
fn unique_path(path: &Path) -> PathBuf {
    let stem = path.file_stem().map(|s| s.to_string_lossy().to_string()).unwrap_or_default();
    let ext = path.extension().map(|s| s.to_string_lossy().to_string());
    let dir = path.parent().unwrap_or(Path::new(""));
    let mut n = 2;
    loop {
        let name = match &ext {
            Some(ext) => format!("{stem} ({n}).{ext}"),
            None => format!("{stem} ({n})"),
        };
        let candidate = dir.join(name);
        if !candidate.exists() {
            return candidate;
        }
        n += 1;
    }
}

/// Open the folder in the desktop's file manager.
pub fn open_dir(path: &Path) -> Result<(), String> {
    let p = path.to_string_lossy().to_string();
    #[cfg(target_os = "linux")]
    let spawned = {
        let mut cmd = std::process::Command::new("xdg-open");
        cmd.arg(&p);
        crate::lastfm::unappimage(&mut cmd);
        cmd.spawn().map(|_| ())
    };
    #[cfg(target_os = "macos")]
    let spawned = std::process::Command::new("open").arg(&p).spawn().map(|_| ());
    // ShellExecuteW "open" on a directory hands it to Explorer, the same way the Last.fm link is
    // handed to the browser (lastfm.rs) — and for the same reason: no cmd.exe re-parsing a path.
    #[cfg(target_os = "windows")]
    let spawned = {
        use windows::core::{HSTRING, PCWSTR};
        use windows::Win32::UI::Shell::ShellExecuteW;
        use windows::Win32::UI::WindowsAndMessaging::SW_SHOWNORMAL;
        let file = HSTRING::from(p.as_str());
        let h = unsafe {
            ShellExecuteW(
                None,
                windows::core::w!("open"),
                &file,
                PCWSTR::null(),
                PCWSTR::null(),
                SW_SHOWNORMAL,
            )
        };
        if h.0 as isize > 32 {
            Ok(())
        } else {
            Err(format!("explorer: {}", h.0 as isize))
        }
    };
    spawned.map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{Db, Download};

    fn db() -> Db {
        Db::open(Path::new(":memory:")).unwrap()
    }

    /// A scratch directory under the system temp dir, emptied first so a re-run starts clean.
    fn scratch(tag: &str) -> PathBuf {
        let p =
            std::env::temp_dir().join(format!("limusic-downloads-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn row(video_id: &str, path: &Path, cover: Option<&Path>) -> Download {
        Download {
            video_id: video_id.into(),
            path: path.to_string_lossy().into(),
            title: format!("song {video_id}"),
            artist: "Artist".into(),
            album: "Album".into(),
            duration_secs: 200,
            cover: cover.map(|c| c.to_string_lossy().into()),
            itag: 140,
            size_bytes: 5,
            created_at: 1,
        }
    }

    #[test]
    fn remove_takes_the_row_the_file_the_empty_album_and_our_cover() {
        let root = scratch("remove");
        let album = root.join("Artist").join("Album");
        std::fs::create_dir_all(&album).unwrap();
        let file = album.join("Song.m4a");
        std::fs::write(&file, b"bytes").unwrap();
        let covers = root.join("covers");
        std::fs::create_dir_all(&covers).unwrap();
        let cover = covers.join("dl-abc.jpg");
        std::fs::write(&cover, b"img").unwrap();

        let d = db();
        d.put_download(&row("abc", &file, Some(&cover)));
        remove(&d, "abc");

        assert!(d.downloads().is_empty(), "the row is gone");
        assert!(!file.exists(), "the bytes are gone");
        assert!(!album.exists(), "the emptied album folder does not linger");
        assert!(!cover.exists(), "our own cover copy went with it");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn move_all_relocates_files_keeps_the_layout_and_repoints_the_rows() {
        let old = scratch("move-old");
        let new = scratch("move-new");
        let nested = old.join("Artist").join("Album");
        std::fs::create_dir_all(&nested).unwrap();
        let one = nested.join("One.m4a");
        let two = old.join("Artist").join("Two.m4a");
        std::fs::write(&one, b"1").unwrap();
        std::fs::write(&two, b"2").unwrap();

        let d = db();
        d.put_download(&row("1", &one, None));
        d.put_download(&row("2", &two, None));
        let moved = move_all(&d, &old, &new);

        assert_eq!(moved, 2, "both files moved");
        assert!(!one.exists() && !two.exists(), "nothing is left in the old folder");
        assert!(new.join("Artist").join("Album").join("One.m4a").is_file(), "layout kept");
        assert!(new.join("Artist").join("Two.m4a").is_file(), "and the album-less one too");
        for r in d.downloads() {
            assert!(Path::new(&r.path).starts_with(&new), "the row follows the file: {}", r.path);
        }
        let _ = std::fs::remove_dir_all(&old);
        let _ = std::fs::remove_dir_all(&new);
    }

    #[test]
    fn move_all_renames_rather_than_overwriting_a_name_already_at_the_destination() {
        let old = scratch("clash-old");
        let new = scratch("clash-new");
        std::fs::create_dir_all(&old).unwrap();
        std::fs::create_dir_all(&new).unwrap();
        let src = old.join("Song.m4a");
        std::fs::write(&src, b"mine").unwrap();
        std::fs::write(new.join("Song.m4a"), b"theirs").unwrap();

        let d = db();
        d.put_download(&row("x", &src, None));
        assert_eq!(move_all(&d, &old, &new), 1);

        assert_eq!(
            std::fs::read(new.join("Song.m4a")).unwrap(),
            b"theirs",
            "the file already here wins"
        );
        assert_eq!(
            std::fs::read(new.join("Song (2).m4a")).unwrap(),
            b"mine",
            "ours is kept, renamed"
        );
        let _ = std::fs::remove_dir_all(&old);
        let _ = std::fs::remove_dir_all(&new);
    }

    #[test]
    fn move_all_lays_a_file_from_outside_the_old_folder_out_like_a_fresh_download() {
        let old = scratch("orphan-old");
        let new = scratch("orphan-new");
        let elsewhere = scratch("orphan-elsewhere");
        std::fs::create_dir_all(&old).unwrap();
        let src = elsewhere.join("Song.m4a");
        std::fs::write(&src, b"bytes").unwrap();

        let d = db();
        d.put_download(&row("orphan", &src, None)); // artist "Artist", album "Album"
        assert_eq!(move_all(&d, &old, &new), 1);

        let want = new.join("Artist").join("Album").join("song orphan.m4a");
        assert!(want.is_file(), "kept Artist/Album/Title instead of flattening: {want:?}");
        assert!(!src.exists(), "left the folder it was in");
        assert_eq!(d.downloads()[0].path, want.to_string_lossy(), "the row follows it");
        for p in [&old, &new, &elsewhere] {
            let _ = std::fs::remove_dir_all(p);
        }
    }

    /// A job with one entry per status and nothing else filled in: the shape `claim_for_worker`
    /// reads, so the rule that decides whether a worker starts is testable without an `AppState`.
    fn job(gen: u64, working: bool, statuses: &[Status]) -> Job {
        Job {
            gen,
            cancel: Arc::new(AtomicBool::new(false)),
            working,
            items: statuses
                .iter()
                .enumerate()
                .map(|(i, status)| Entry {
                    song: MiniSong {
                        video_id: format!("v{i}"),
                        title: format!("song {i}"),
                        artists: "Artist".into(),
                        album: "Album".into(),
                        duration_secs: 200,
                        is_upload: false,
                        thumbnail: None,
                    },
                    status: *status,
                    progress: 0.0,
                    error: None,
                })
                .collect(),
            last_emit: None,
        }
    }

    /// The loop behind "the app freezes when I dismiss the download popup": `run` ends by asking for
    /// a successor so a track queued while it was finishing is not stranded, and a worker that finds
    /// an empty queue returns at once. Claiming the queue with nothing queued made those two spawn
    /// each other forever, pegging a core and flooding the webview until nothing answered.
    #[test]
    fn a_drained_job_never_asks_for_another_worker() {
        let mut drained = job(7, false, &[Status::Done, Status::Failed, Status::Cancelled]);
        assert!(!claim_for_worker(&mut drained, 7), "nothing queued is nothing to spawn");
        assert!(!drained.working, "and no worker is recorded as owning the queue");

        let mut empty = job(7, false, &[]);
        assert!(!claim_for_worker(&mut empty, 7), "a job with no tracks at all is not work either");

        let mut queued = job(7, false, &[Status::Done, Status::Queued]);
        assert!(
            claim_for_worker(&mut queued, 7),
            "a track queued while finishing needs a successor"
        );
        assert!(queued.working, "the successor owns the queue");

        // One owner per queue, and only for the run that asked for it.
        assert!(!claim_for_worker(&mut queued, 7), "one worker at a time");
        let mut other = job(8, false, &[Status::Queued]);
        assert!(!claim_for_worker(&mut other, 7), "not another job's queue");
        assert!(!other.working);
    }
}
