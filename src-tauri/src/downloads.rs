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
    let snap = Snapshot::of(&job.items, job.working || job.items.iter().any(|e| e.status == Status::Queued));
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

fn spawn_worker(state: Arc<AppState>, gen: u64) {
    let spawn = {
        let mut g = JOB.lock().unwrap();
        match g.as_mut().filter(|j| j.gen == gen) {
            Some(j) if !j.working => {
                j.working = true;
                true
            }
            _ => false,
        }
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
        .resolve(
            &song.video_id,
            song.is_upload,
            state.quality(),
            &state.disabled_clients(),
            true,
        )
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
    let target = unique_target(&state.db, root, &artist, &song.album, &song.title, ext, untagged, &song.video_id);
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
pub fn remove(state: &Arc<AppState>, video_id: &str) {
    let cover = state
        .db
        .downloads()
        .into_iter()
        .find(|d| d.video_id == video_id)
        .and_then(|d| d.cover);
    if let Some(path) = state.db.delete_download(video_id) {
        let _ = std::fs::remove_file(&path);
        // An empty Artist/Album left behind would show up as an album in Local Music.
        if let Some(dir) = Path::new(&path).parent() {
            let _ = std::fs::remove_dir(dir);
        }
    }
    if let Some(cover) = cover {
        if Path::new(&cover)
            .file_name()
            .is_some_and(|n| n.to_string_lossy().starts_with("dl-"))
        {
            let _ = std::fs::remove_file(cover);
        }
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
            ShellExecuteW(None, windows::core::w!("open"), &file, PCWSTR::null(), PCWSTR::null(), SW_SHOWNORMAL)
        };
        if h.0 as isize > 32 {
            Ok(())
        } else {
            Err(format!("explorer: {}", h.0 as isize))
        }
    };
    spawned.map_err(|e| e.to_string())
}
