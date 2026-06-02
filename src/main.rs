#[cfg(not(target_os = "linux"))]
compile_error!("tidaload currently supports Linux only");

mod config;
mod doh;
mod download;
mod tidal;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use futures_util::stream::{self, StreamExt};
use mp4ameta::{Img, Tag};
use tokio::sync::{Mutex, Semaphore};
use tokio::time::sleep;

use crate::config::{
    Config, DEFAULT_DOWNLOAD_CONCURRENCY, default_config_path, music_download_dir,
};
use crate::download::{
    DownloadProgress, ProgressCallback, download_segmented_to_file, download_to_file,
    remove_existing_path, sanitize_file_name,
};
use crate::tidal::{
    Album, DownloadInfo, ParsedResource, Playlist, ResourceKind, TidalClient, Track,
    cover_image_url, parse_resource,
};

type CoverCache = Arc<Mutex<HashMap<String, Arc<Vec<u8>>>>>;
const TRACK_START_DELAY_MIN_MS: u64 = 2_500;
const TRACK_START_DELAY_MAX_MS: u64 = 11_000;

#[derive(Debug, Parser)]
#[command(name = "tidaload")]
#[command(about = "Download TIDAL tracks, albums, and playlists")]
struct Cli {
    #[arg(long, global = true, default_value_os_t = default_config_path())]
    config: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Login,
    Download {
        #[arg(required = true)]
        items: Vec<String>,

        #[arg(long, value_enum)]
        kind: Option<ResourceKind>,

        #[arg(long)]
        concurrency: Option<usize>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut config = Config::load(&cli.config)?;

    match cli.command {
        Command::Login => {
            let mut client = TidalClient::new(config.tidal.clone())?;
            client.login_device_flow().await?;
            config.tidal = client.config;
            config.save(&cli.config)?;
            println!("Saved TIDAL credentials to {}", cli.config.display());
        }
        Command::Download {
            items,
            kind,
            concurrency,
        } => {
            if let Some(concurrency) = concurrency {
                config.downloads.concurrency = concurrency.max(1);
            } else {
                config.downloads.concurrency = config
                    .downloads
                    .concurrency
                    .clamp(1, DEFAULT_DOWNLOAD_CONCURRENCY);
            }

            let mut client = TidalClient::new(config.tidal.clone())?;
            let changed = client.ensure_login().await?;
            if changed {
                config.tidal = client.config.clone();
                config.save(&cli.config)?;
            }

            let resources = items
                .iter()
                .map(|item| parse_resource(item, kind))
                .collect::<Result<Vec<_>>>()?;
            let download_root = music_download_dir()?;

            for resource in resources {
                download_resource(&client, &config, resource, &download_root).await?;
            }
        }
    }

    Ok(())
}

async fn download_resource(
    client: &TidalClient,
    config: &Config,
    resource: ParsedResource,
    download_root: &Path,
) -> Result<()> {
    match resource.kind {
        ResourceKind::Track => {
            let track = client.get_track(&resource.id).await?;
            download_single_track(client, track, download_root).await?;
        }
        ResourceKind::Album => {
            let album = client.get_album(&resource.id).await?;
            download_album(client, config, album, download_root).await?;
        }
        ResourceKind::Playlist => {
            let playlist = client.get_playlist(&resource.id).await?;
            download_playlist(client, config, playlist, download_root).await?;
        }
    }
    Ok(())
}

async fn download_album(
    client: &TidalClient,
    config: &Config,
    album: Album,
    download_root: &Path,
) -> Result<()> {
    println!(
        "Downloading album: {} - {} [{}] ({} tracks, {} disc{})",
        album.artist,
        album.title,
        album.id,
        album.tracks.len(),
        album.disc_total,
        if album.disc_total == 1 { "" } else { "s" }
    );

    let year = album.year.as_deref().unwrap_or("Unknown Year");
    let folder = download_root.join(sanitize_file_name(&format!(
        "{} - {} ({})",
        album.artist, album.title, year
    )));
    let disc_subdirectories = album.disc_total > 1;
    remove_existing_path(&folder).await?;
    download_tracks_concurrently(
        client,
        config,
        album.tracks,
        folder,
        TrackNumbering::Album {
            disc_subdirectories,
        },
    )
    .await
}

async fn download_playlist(
    client: &TidalClient,
    config: &Config,
    playlist: Playlist,
    download_root: &Path,
) -> Result<()> {
    println!(
        "Downloading playlist: {} [{}] ({} tracks)",
        playlist.title,
        playlist.id,
        playlist.tracks.len()
    );

    let folder = download_root.join(sanitize_file_name(&playlist.title));
    remove_existing_path(&folder).await?;
    download_tracks_concurrently(
        client,
        config,
        playlist.tracks,
        folder,
        TrackNumbering::Playlist,
    )
    .await
}

#[derive(Debug, Clone, Copy)]
enum TrackNumbering {
    Album { disc_subdirectories: bool },
    Playlist,
}

async fn download_tracks_concurrently(
    client: &TidalClient,
    config: &Config,
    tracks: Vec<Track>,
    folder: PathBuf,
    numbering: TrackNumbering,
) -> Result<()> {
    let concurrency = config.downloads.concurrency.max(1);
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let cover_cache = Arc::new(Mutex::new(HashMap::new()));
    let completed_tracks = Arc::new(AtomicUsize::new(0));
    let total_tracks = tracks.len();

    let results: Vec<Result<()>> = stream::iter(tracks.into_iter().enumerate())
        .map(|(index, track)| {
            let semaphore = Arc::clone(&semaphore);
            let cover_cache = Arc::clone(&cover_cache);
            let completed_tracks = Arc::clone(&completed_tracks);
            let folder = folder.clone();
            async move {
                let _permit = semaphore.acquire_owned().await?;
                let delay = track_start_delay(index, concurrency);
                if !delay.is_zero() {
                    println!(
                        "{} Waiting {:.1}s before next track",
                        track_scope(index, total_tracks),
                        delay.as_millis() as f64 / 1000.0
                    );
                    sleep(delay).await;
                }
                let playlist_position = match numbering {
                    TrackNumbering::Playlist => Some(index + 1),
                    TrackNumbering::Album { .. } => None,
                };
                let disc_subdirectories = match numbering {
                    TrackNumbering::Album {
                        disc_subdirectories,
                    } => disc_subdirectories,
                    TrackNumbering::Playlist => false,
                };
                let result = download_track(
                    client,
                    track,
                    &folder,
                    playlist_position,
                    disc_subdirectories,
                    &cover_cache,
                    TrackProgressScope {
                        index: index + 1,
                        total: total_tracks,
                    },
                )
                .await;

                let finished = completed_tracks.fetch_add(1, Ordering::SeqCst) + 1;
                match &result {
                    Ok(()) => println!("[global {finished}/{total_tracks}] Track complete"),
                    Err(err) => {
                        eprintln!("[global {finished}/{total_tracks}] Track failed: {err:#}")
                    }
                }
                result
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    let mut failures = 0;
    for result in results {
        if let Err(err) = result {
            failures += 1;
            eprintln!("download failed: {err:#}");
        }
    }

    if failures > 0 {
        bail!("{failures} track download(s) failed");
    }

    Ok(())
}

async fn download_single_track(client: &TidalClient, track: Track, folder: &Path) -> Result<()> {
    let cover_cache = Arc::new(Mutex::new(HashMap::new()));
    download_track(
        client,
        track,
        folder,
        None,
        false,
        &cover_cache,
        TrackProgressScope { index: 1, total: 1 },
    )
    .await?;
    println!("[global 1/1] Track complete");
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct TrackProgressScope {
    index: usize,
    total: usize,
}

impl TrackProgressScope {
    fn label(self) -> String {
        track_scope(self.index.saturating_sub(1), self.total)
    }
}

async fn download_track(
    client: &TidalClient,
    track: Track,
    folder: &Path,
    playlist_position: Option<usize>,
    disc_subdirectories: bool,
    cover_cache: &CoverCache,
    progress_scope: TrackProgressScope,
) -> Result<()> {
    if !track.allow_streaming {
        println!(
            "{} Skipping unavailable track {} ({})",
            progress_scope.label(),
            track.title,
            track.id
        );
        return Ok(());
    }

    println!(
        "{} Preparing: {} - {}",
        progress_scope.label(),
        track.artist,
        track.title
    );
    let info = client
        .get_download_info(&track.id)
        .await
        .with_context(|| format!("failed to get playback info for {}", track.id))?;

    let filename = track_filename(&track, info.extension(), playlist_position);
    let target_folder = if disc_subdirectories {
        folder.join(format!("Disc {}", track.volume_number.unwrap_or(1)))
    } else {
        folder.to_path_buf()
    };
    let path = target_folder.join(filename);
    remove_existing_path(&path).await?;

    println!(
        "{} Downloading: {} - {}",
        progress_scope.label(),
        track.artist,
        track.title
    );
    let progress_callback = track_progress_callback(progress_scope, &track);
    match &info {
        DownloadInfo::Direct {
            url,
            encryption_key,
            ..
        } => {
            download_to_file(
                client.http_client(),
                url,
                &path,
                encryption_key.as_deref(),
                Some(Arc::clone(&progress_callback)),
            )
            .await
        }
        DownloadInfo::Segmented {
            initialization_url,
            media_url_template,
            start_number,
            segment_count,
            ..
        } => {
            download_segmented_to_file(
                client.http_client(),
                initialization_url,
                media_url_template,
                *start_number,
                *segment_count,
                &path,
                Some(Arc::clone(&progress_callback)),
            )
            .await
        }
    }
    .with_context(|| format!("failed to download {}", track.id))?;
    embed_cover_art(client.http_client(), &track, &path, cover_cache)
        .await
        .with_context(|| format!("failed to embed cover art for {}", track.id))?;
    println!("{} Saved: {}", progress_scope.label(), path.display());
    Ok(())
}

fn track_progress_callback(progress_scope: TrackProgressScope, track: &Track) -> ProgressCallback {
    let label = progress_scope.label();
    let title = format!("{} - {}", track.artist, track.title);

    Arc::new(move |progress| match progress {
        DownloadProgress::DirectBytes { downloaded, total } => {
            println!(
                "{label} {title}: downloaded {}{}",
                format_bytes(downloaded),
                total
                    .map(|total| format!(" / {}", format_bytes(total)))
                    .unwrap_or_default()
            );
        }
        DownloadProgress::DashInitialized => {
            println!("{label} {title}: DASH initialization downloaded");
        }
        DownloadProgress::DashSegment { downloaded, total } => {
            if should_log_dash_segment_progress(downloaded, total) {
                println!("{label} {title}: DASH segments {downloaded}/{total}");
            }
        }
    })
}

fn should_log_dash_segment_progress(downloaded: u32, total: u32) -> bool {
    downloaded == 1 || downloaded == total || downloaded % 8 == 0
}

async fn embed_cover_art(
    client: &reqwest::Client,
    track: &Track,
    path: &Path,
    cover_cache: &CoverCache,
) -> Result<()> {
    let Some(cover_uuid) = track.cover_uuid.as_deref() else {
        return Ok(());
    };

    let cover = cover_art_bytes(client, cover_uuid, cover_cache).await?;
    let path = path.to_path_buf();

    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut tag = Tag::read_from_path(&path)
            .with_context(|| format!("failed to read MP4 metadata from {}", path.display()))?;
        tag.set_artwork(Img::jpeg(cover.as_ref().clone()));
        tag.write_to_path(&path)
            .with_context(|| format!("failed to write MP4 metadata to {}", path.display()))?;
        Ok(())
    })
    .await
    .context("cover art metadata writer task failed")?
}

async fn cover_art_bytes(
    client: &reqwest::Client,
    cover_uuid: &str,
    cover_cache: &CoverCache,
) -> Result<Arc<Vec<u8>>> {
    {
        let cache = cover_cache.lock().await;
        if let Some(cover) = cache.get(cover_uuid) {
            return Ok(Arc::clone(cover));
        }
    }

    let url = cover_image_url(cover_uuid);
    let cover = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("failed to request cover art {url}"))?
        .error_for_status()
        .with_context(|| format!("cover art request failed for {url}"))?
        .bytes()
        .await
        .with_context(|| format!("failed to read cover art {url}"))?;
    let cover = Arc::new(cover.to_vec());

    let mut cache = cover_cache.lock().await;
    Ok(Arc::clone(
        cache
            .entry(cover_uuid.to_string())
            .or_insert_with(|| Arc::clone(&cover)),
    ))
}

fn track_filename(track: &Track, extension: &str, playlist_position: Option<usize>) -> String {
    let number = playlist_position
        .or_else(|| track.track_number.map(|n| n as usize))
        .map(|n| format!("{n:02}. "))
        .unwrap_or_default();
    let name = sanitize_file_name(&format!("{number}{} - {}", track.artist, track.title));
    format!("{name}.{extension}")
}

fn track_scope(index: usize, total: usize) -> String {
    format!("[{}/{}]", index + 1, total.max(1))
}

fn format_bytes(bytes: u64) -> String {
    let mib = bytes as f64 / 1024.0 / 1024.0;
    format!("{mib:.1} MiB")
}

fn track_start_delay(index: usize, concurrency: usize) -> Duration {
    if index < concurrency {
        return Duration::ZERO;
    }

    irregular_track_delay(delay_seed(index))
}

fn irregular_track_delay(seed: u64) -> Duration {
    let span = TRACK_START_DELAY_MAX_MS - TRACK_START_DELAY_MIN_MS + 1;
    Duration::from_millis(TRACK_START_DELAY_MIN_MS + xorshift64(seed) % span)
}

fn delay_seed(index: usize) -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or_default();
    now ^ ((index as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15))
}

fn xorshift64(mut value: u64) -> u64 {
    if value == 0 {
        value = 0xA076_1D64_78BD_642F;
    }
    value ^= value << 13;
    value ^= value >> 7;
    value ^= value << 17;
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skips_delay_for_initial_concurrency_slots() {
        assert_eq!(track_start_delay(0, 2), Duration::ZERO);
        assert_eq!(track_start_delay(1, 2), Duration::ZERO);
    }

    #[test]
    fn irregular_delay_stays_in_configured_bounds() {
        for seed in [1, 2, 99, u64::MAX] {
            let delay = irregular_track_delay(seed);
            assert!(delay >= Duration::from_millis(TRACK_START_DELAY_MIN_MS));
            assert!(delay <= Duration::from_millis(TRACK_START_DELAY_MAX_MS));
        }
    }

    #[test]
    fn formats_track_scope() {
        assert_eq!(track_scope(0, 12), "[1/12]");
        assert_eq!(track_scope(2, 0), "[3/1]");
    }

    #[test]
    fn logs_dash_segment_progress_periodically() {
        assert!(should_log_dash_segment_progress(1, 32));
        assert!(should_log_dash_segment_progress(8, 32));
        assert!(should_log_dash_segment_progress(32, 32));
        assert!(!should_log_dash_segment_progress(7, 32));
    }
}
