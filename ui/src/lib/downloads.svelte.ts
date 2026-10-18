// Downloads (Rust `downloads.rs`): the dialog's state, the background job it follows, and the
// tracks already saved to this disk. The work itself is Rust's; this only mirrors the
// `download-progress` events and keeps the "downloaded" answers the track menu asks for.
import { open } from '@tauri-apps/plugin-dialog';
import * as api from './api';
import { t } from './i18n.svelte';
import { toast, ui } from './player.svelte';

export const dl = $state({
	/** The running or finished job, `null` when there is none. */
	job: null as api.DownloadSnapshot | null,
	/** Everything on disk, newest first. */
	list: [] as api.DownloadRecord[],
	/** The folder downloads land in. */
	dir: '',
	loaded: false
});

/** Whether this track is already saved to this disk (the menu says "Downloaded" instead). */
export const isDownloaded = (videoId?: string | null): boolean =>
	!!videoId && dl.list.some((d) => d.videoId === videoId);

export async function refreshDownloads() {
	try {
		dl.list = await api.getDownloads();
		dl.loaded = true;
	} catch {
		// Keep whatever list we have: a failed read is not a reason to blank the view.
	}
}

async function refreshDir() {
	try {
		dl.dir = await api.getDownloadDir();
	} catch {
		/* the default folder is still shown by the placeholder */
	}
}

/** Open the dialog, refreshing what it draws. */
export function openDownloads() {
	ui.downloadsOpen = true;
	void refreshDownloads();
	void refreshDir();
}

/** Queue one or more tracks, and open the dialog so the progress is visible. Local files and ones
 *  already on disk are left out; Rust does the same filtering, this only avoids a pointless call. */
export async function download(songs: api.SongItem[]) {
	const fresh = songs.filter((s) => !api.isLocalId(s.video_id) && !isDownloaded(s.video_id));
	if (!fresh.length) {
		toast(t('downloads.already'));
		openDownloads();
		return;
	}
	try {
		dl.job = await api.downloadTracks(fresh);
		openDownloads();
	} catch (e) {
		toast.error(String(e));
	}
}

/** Stop the running job, or put a finished one away. */
export async function cancelDownloads() {
	await api.cancelDownloads().catch(() => {});
}

export async function removeDownload(videoId: string) {
	try {
		dl.list = await api.removeDownload(videoId);
	} catch (e) {
		toast.error(String(e));
	}
	// The file left this disk, so a queue or Shortcuts tile pointing at it is now dead.
	void refreshDownloads();
}

export async function pickDownloadDir() {
	const picked = await open({
		directory: true,
		multiple: false,
		title: t('downloads.pick_folder')
	});
	if (typeof picked !== 'string') return;
	try {
		dl.dir = await api.setDownloadDir(picked);
		toast.success(t('downloads.folder_set'));
	} catch (e) {
		toast.error(String(e));
	}
}

let started = false;

/** Once, from the root layout: follow the events, and pick up a job already under way (the webview
 *  reloaded while Rust kept downloading). */
export function initDownloads() {
	if (started) return;
	started = true;
	api.onDownloadProgress((s) => {
		const was = dl.job;
		dl.job = s;
		const finished = (was?.done ?? 0) + (was?.failed ?? 0);
		const now = (s?.done ?? 0) + (s?.failed ?? 0);
		if (now !== finished || !s) void refreshDownloads();
		// Say it once, when the job that was running stops with failures behind it.
		if (s && was?.running && !s.running && s.failed) {
			toast.error(t('downloads.failed_count', { count: s.failed }));
		}
	});
	api
		.downloadStatus()
		.then((s) => {
			if (s) dl.job = s;
		})
		.catch(() => {});
	void refreshDownloads();
}
