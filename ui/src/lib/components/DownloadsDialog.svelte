<script lang="ts">
	// The Downloads view: the running job at the top, everything already on this disk below it, and
	// the folder it all lands in. Opened from a track menu's Download, or the sidebar's Downloads
	// row; the work itself is Rust's (`downloads.rs`), and this only draws what it reports.
	import { fade, scale } from 'svelte/transition';
	import { cubicOut } from 'svelte/easing';
	import { HugeiconsIcon } from '@hugeicons/svelte';
	import {
		AlertCircleIcon,
		Cancel01Icon,
		CheckmarkCircle02Icon,
		Delete02Icon,
		Download04Icon,
		Folder01Icon,
		FolderEditIcon,
		FolderOpenIcon,
		Loading03Icon,
		MusicNote01Icon,
		PlayIcon
	} from '@hugeicons/core-free-icons';
	import { Button } from '$lib/components/ui/button';
	import { thumb } from '$lib/thumb';
	import { t } from '$lib/i18n.svelte';
	import { toast, playSong, ui } from '$lib/player.svelte';
	import { cancelDownloads, dl, pickDownloadDir, removeDownload } from '$lib/downloads.svelte';
	import * as api from '$lib/api';

	function close() {
		ui.downloadsOpen = false;
	}

	async function openFolder() {
		try {
			await api.openDownloadDir();
		} catch (e) {
			toast.error(String(e));
		}
	}

	/** A downloaded file plays through the same local path the Local tab uses. */
	function play(d: api.DownloadRecord) {
		playSong({
			video_id: `LOCAL:${d.path}`,
			title: d.title,
			artists: d.artist,
			album: d.album || undefined,
			duration: duration(d.durationSecs),
			thumbnail: d.cover ?? undefined
		});
	}

	function duration(secs: number): string | undefined {
		if (!secs) return undefined;
		const h = Math.floor(secs / 3600);
		const m = Math.floor((secs % 3600) / 60);
		const s = secs % 60;
		return h ? `${h}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}` : `${m}:${String(s).padStart(2, '0')}`;
	}

	function size(bytes: number): string {
		if (bytes >= 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
		if (bytes >= 1024) return `${Math.round(bytes / 1024)} KB`;
		return `${bytes} B`;
	}

	function statusText(it: api.DownloadItem): string {
		switch (it.status) {
			case 'queued':
				return t('downloads.queued');
			case 'active':
				return t('downloads.downloading');
			case 'done':
				return t('downloads.done');
			case 'cancelled':
				return t('downloads.cancelled');
			default:
				return it.error ? it.error : t('downloads.status_failed');
		}
	}
</script>

<svelte:window
	onkeydown={(e) => {
		if (ui.downloadsOpen && e.key === 'Escape') close();
	}}
/>

{#if ui.downloadsOpen}
	<div
		transition:fade={{ duration: 150 }}
		class="fixed inset-0 z-[49] flex items-center justify-center bg-black/50 p-4"
		role="presentation"
		onclick={(e) => {
			if (e.target === e.currentTarget) close();
		}}
	>
		<div
			transition:scale={{ duration: 180, start: 0.96, easing: cubicOut }}
			class="flex max-h-[34rem] w-full max-w-lg flex-col rounded-xl border bg-card p-4 shadow-xl"
		>
			<div class="mb-3 flex items-center gap-2">
				<span class="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg bg-primary/10 text-primary">
					<HugeiconsIcon icon={Download04Icon} class="h-5 w-5" />
				</span>
				<h2 class="min-w-0 flex-1 font-heading text-base font-semibold">{t('downloads.title')}</h2>
				<button
					class="flex h-8 w-8 shrink-0 items-center justify-center rounded-full text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
					onclick={close}
					aria-label={t('a11y.close')}
				>
					<HugeiconsIcon icon={Cancel01Icon} class="h-4 w-4" />
				</button>
			</div>

			<!-- Where the files go. Two buttons, not a settings row: this is the moment the question
			     is actually asked. -->
			<div class="mb-3 flex items-center gap-2 rounded-lg border bg-background/50 px-3 py-2">
				<HugeiconsIcon icon={Folder01Icon} class="h-4 w-4 shrink-0 text-muted-foreground" />
				<span class="min-w-0 flex-1 truncate text-xs text-muted-foreground" title={dl.dir}>
					{dl.dir || t('common.loading')}
				</span>
				<Button variant="ghost" size="xs" class="gap-1" onclick={openFolder}>
					<HugeiconsIcon icon={FolderOpenIcon} class="h-3.5 w-3.5" />
					<span class="hidden sm:inline">{t('downloads.open_folder')}</span>
				</Button>
				<Button variant="ghost" size="xs" class="gap-1" onclick={pickDownloadDir}>
					<HugeiconsIcon icon={FolderEditIcon} class="h-3.5 w-3.5" />
					<span class="hidden sm:inline">{t('downloads.change_folder')}</span>
				</Button>
			</div>

			<div class="min-h-0 flex-1 overflow-y-auto">
				{#if dl.job}
					<div class="mb-1 flex items-center justify-between gap-2 px-1">
						<span class="text-xs font-medium text-muted-foreground">
							{dl.job.running
								? t('downloads.in_progress', { done: dl.job.done, total: dl.job.items.length })
								: t('downloads.finished', { done: dl.job.done, total: dl.job.items.length })}
						</span>
						<Button variant="ghost" size="xs" onclick={cancelDownloads}>
							{dl.job.running ? t('downloads.cancel') : t('downloads.dismiss')}
						</Button>
					</div>
					{#each dl.job.items as it (it.videoId)}
						<div class="flex items-center gap-3 rounded-lg p-2">
							{#if it.thumbnail}
								<img src={thumb(it.thumbnail, 96)} alt="" class="h-10 w-10 shrink-0 rounded-md object-cover" />
							{:else}
								<div class="flex h-10 w-10 shrink-0 items-center justify-center rounded-md bg-muted text-muted-foreground/50">
									<HugeiconsIcon icon={MusicNote01Icon} class="h-4 w-4" />
								</div>
							{/if}
							<div class="min-w-0 flex-1">
								<div class="truncate text-sm font-medium">{it.title}</div>
								<div class="flex items-center gap-1.5 text-xs text-muted-foreground">
									{#if it.status === 'active'}
										<HugeiconsIcon icon={Loading03Icon} class="h-3 w-3 shrink-0 animate-spin" />
									{:else if it.status === 'done'}
										<HugeiconsIcon icon={CheckmarkCircle02Icon} class="h-3 w-3 shrink-0 text-primary" />
									{:else if it.status === 'failed' || it.status === 'cancelled'}
										<HugeiconsIcon icon={AlertCircleIcon} class="h-3 w-3 shrink-0 text-destructive" />
									{/if}
									<span class="truncate">{statusText(it)}</span>
								</div>
								{#if it.status === 'active'}
									<div class="mt-1 h-1 w-full overflow-hidden rounded-full bg-muted">
										<div
											class="h-full rounded-full bg-primary transition-[width] duration-300"
											style="width: {Math.round(it.progress * 100)}%"
										></div>
									</div>
								{/if}
							</div>
						</div>
					{/each}
				{/if}

				{#if dl.list.length}
					<div class="mt-2 px-1 text-xs font-medium text-muted-foreground">
						{t('downloads.on_disk', { count: dl.list.length })}
					</div>
					{#each dl.list as d (d.videoId)}
						<div class="group/row flex items-center gap-3 rounded-lg p-2 hover:bg-accent/10">
							<button
								class="relative flex h-10 w-10 shrink-0 items-center justify-center overflow-hidden rounded-md bg-muted"
								onclick={() => play(d)}
								aria-label={t('player.play')}
							>
								{#if d.cover}
									<img src={thumb(d.cover, 96)} alt="" class="h-full w-full object-cover" />
								{:else}
									<HugeiconsIcon icon={MusicNote01Icon} class="h-4 w-4 text-muted-foreground/50" />
								{/if}
								<span class="absolute inset-0 flex items-center justify-center bg-black/40 text-white opacity-0 transition-opacity group-hover/row:opacity-100">
									<HugeiconsIcon icon={PlayIcon} class="h-4 w-4" />
								</span>
							</button>
							<div class="min-w-0 flex-1">
								<div class="truncate text-sm font-medium">{d.title}</div>
								<div class="truncate text-xs text-muted-foreground">
									{[d.artist, d.album, size(d.sizeBytes)].filter(Boolean).join(' • ')}
								</div>
							</div>
							<Button
								variant="ghost"
								size="icon-sm"
								class="shrink-0 text-muted-foreground hover:text-destructive"
								aria-label={t('downloads.remove')}
								title={t('downloads.remove')}
								onclick={() => removeDownload(d.videoId)}
							>
								<HugeiconsIcon icon={Delete02Icon} class="h-4 w-4" />
							</Button>
						</div>
					{/each}
				{/if}

				{#if !dl.job && !dl.list.length}
					<div class="flex flex-col items-center gap-2 px-6 py-10 text-center">
						<HugeiconsIcon icon={Download04Icon} class="h-7 w-7 text-muted-foreground/50" />
						<p class="text-sm text-muted-foreground">{t('downloads.empty')}</p>
					</div>
				{/if}
			</div>

			<p class="mt-3 text-xs text-muted-foreground">{t('downloads.hint')}</p>
		</div>
	</div>
{/if}
