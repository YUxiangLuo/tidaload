#[cfg(not(target_os = "linux"))]
compile_error!("tidaload currently supports Linux only");

mod config;
mod doh;
mod download;
mod http;
mod metadata;
mod runner;
mod tidal;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use futures_util::stream::{self, StreamExt};
use tokio::sync::Semaphore;
use tokio::time::sleep;

use crate::config::{Config, DEFAULT_DOWNLOAD_CONCURRENCY, default_config_path};
use crate::download::{
    DownloadProgress, ProgressCallback, SegmentedDownload, download_direct_to_flac_file,
    download_segmented_to_file, download_to_file, remove_existing_path, sanitize_file_name,
};
use crate::http::is_rate_limited_error;
use crate::metadata::{CoverCache, TrackTagContext, new_cover_cache, write_track_metadata};
use crate::tidal::{
    Album, DownloadInfo, ParsedResource, PlaybackQuality, Playlist, ResourceKind, TidalClient,
    Track,
};

const TRACK_START_DELAY_MIN_MS: u64 = 2_000;
const TRACK_START_DELAY_MAX_MS: u64 = 6_000;
const TRACK_DOWNLOAD_MAX_ATTEMPTS: usize = 2;
const ANSI_BOLD_GREEN: &str = "\x1b[1;32m";
const ANSI_BOLD_RED: &str = "\x1b[1;31m";
const ANSI_RESET: &str = "\x1b[0m";

#[derive(Debug, Parser)]
#[command(name = "tidaload")]
#[command(about = "Download TIDAL tracks, albums, and playlists")]
struct Cli {
    #[arg(long, global = true, default_value_os_t = default_config_path())]
    config: PathBuf,

    #[arg(long, global = true, value_enum)]
    kind: Option<ResourceKind>,

    #[arg(long, global = true)]
    dash_segment_concurrency: Option<usize>,

    #[arg(long, global = true)]
    download_dir: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Login,
    Download {
        #[arg(required = true)]
        items: Vec<String>,
    },
    #[command(external_subcommand)]
    Direct(Vec<String>),
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
        Command::Download { items } | Command::Direct(items) => {
            runner::run_download(
                &mut config,
                &cli.config,
                cli.kind,
                cli.dash_segment_concurrency,
                cli.download_dir,
                items,
            )
            .await?;
        }
    }

    Ok(())
}

pub(crate) async fn download_resource(
    client: &TidalClient,
    config: &Config,
    resource: ParsedResource,
    download_root: &Path,
) -> Result<()> {
    match resource.kind {
        ResourceKind::Track => {
            let track = client.get_track(&resource.id).await?;
            download_single_track(client, config, track, download_root).await?;
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
    let tag_context = TrackTagContext::album(&album);
    remove_existing_path(&folder).await?;
    download_tracks_concurrently(
        client,
        config,
        album.tracks,
        folder,
        TrackNumbering::Album {
            disc_subdirectories,
        },
        tag_context,
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
        TrackTagContext::default(),
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
    tag_context: TrackTagContext,
) -> Result<()> {
    let concurrency = DEFAULT_DOWNLOAD_CONCURRENCY;
    let dash_segment_concurrency = config.downloads.dash_segment_concurrency.max(1);
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let cover_cache = new_cover_cache();
    let completed_tracks = Arc::new(AtomicUsize::new(0));
    let total_tracks = tracks.len();

    let downloads = stream::iter(tracks.into_iter().enumerate())
        .map(|(index, track)| {
            let semaphore = Arc::clone(&semaphore);
            let cover_cache = Arc::clone(&cover_cache);
            let completed_tracks = Arc::clone(&completed_tracks);
            let folder = folder.clone();
            let tag_context = tag_context.clone();
            async move {
                let _permit = match semaphore.acquire_owned().await {
                    Ok(permit) => permit,
                    Err(err) => {
                        return Ok(Some(MissingTrack::from_track(
                            index,
                            total_tracks,
                            &track,
                            format!("failed to acquire download slot: {err}"),
                        )));
                    }
                };
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
                let result = download_track_with_retries(
                    client,
                    &track,
                    &folder,
                    &cover_cache,
                    TrackDownloadOptions {
                        playlist_position,
                        disc_subdirectories,
                        dash_segment_concurrency,
                        progress_scope: TrackProgressScope {
                            index: index + 1,
                            total: total_tracks,
                        },
                        tag_context,
                    },
                )
                .await;

                let finished = completed_tracks.fetch_add(1, Ordering::SeqCst) + 1;
                match result {
                    Ok(TrackDownloadStatus::Saved) => {
                        println!("[global {finished}/{total_tracks}] Track complete");
                        Ok(None)
                    }
                    Ok(TrackDownloadStatus::SkippedUnavailable) => {
                        eprintln!("[global {finished}/{total_tracks}] Track skipped");
                        Ok(Some(MissingTrack::from_track(
                            index,
                            total_tracks,
                            &track,
                            "unavailable for streaming",
                        )))
                    }
                    Err(err) if is_rate_limited_error(&err) => Err(err),
                    Err(err) => {
                        eprintln!("[global {finished}/{total_tracks}] Track failed: {err:#}");
                        Ok(Some(MissingTrack::from_track(
                            index,
                            total_tracks,
                            &track,
                            format!("{err:#}"),
                        )))
                    }
                }
            }
        })
        .buffer_unordered(concurrency);

    let mut missing_tracks = Vec::new();
    futures_util::pin_mut!(downloads);
    while let Some(result) = downloads.next().await {
        match result {
            Ok(Some(missing_track)) => missing_tracks.push(missing_track),
            Ok(None) => {}
            Err(err) if is_rate_limited_error(&err) => return Err(err),
            Err(err) => return Err(err),
        }
    }

    print_missing_tracks(&missing_tracks);

    Ok(())
}

async fn download_single_track(
    client: &TidalClient,
    config: &Config,
    track: Track,
    folder: &Path,
) -> Result<()> {
    let cover_cache = new_cover_cache();
    match download_track_with_retries(
        client,
        &track,
        folder,
        &cover_cache,
        TrackDownloadOptions {
            playlist_position: None,
            disc_subdirectories: false,
            dash_segment_concurrency: config.downloads.dash_segment_concurrency,
            progress_scope: TrackProgressScope { index: 1, total: 1 },
            tag_context: TrackTagContext::default(),
        },
    )
    .await?
    {
        TrackDownloadStatus::Saved => println!("[global 1/1] Track complete"),
        TrackDownloadStatus::SkippedUnavailable => {
            eprintln!(
                "{}",
                red_bold(&format!(
                    "Missing track: {} - {} [{}] - unavailable for streaming",
                    track.artist, track.title, track.id
                ))
            );
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct TrackProgressScope {
    index: usize,
    total: usize,
}

#[derive(Debug, Clone)]
struct TrackDownloadOptions {
    playlist_position: Option<usize>,
    disc_subdirectories: bool,
    dash_segment_concurrency: usize,
    progress_scope: TrackProgressScope,
    tag_context: TrackTagContext,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TrackDownloadStatus {
    Saved,
    SkippedUnavailable,
}

#[derive(Debug)]
struct MissingTrack {
    index: usize,
    total: usize,
    id: String,
    artist: String,
    title: String,
    reason: String,
}

impl TrackProgressScope {
    fn label(self) -> String {
        track_scope(self.index.saturating_sub(1), self.total)
    }
}

impl MissingTrack {
    fn from_track(index: usize, total: usize, track: &Track, reason: impl Into<String>) -> Self {
        Self {
            index,
            total,
            id: track.id.clone(),
            artist: track.artist.clone(),
            title: track.title.clone(),
            reason: reason.into(),
        }
    }
}

async fn download_track_with_retries(
    client: &TidalClient,
    track: &Track,
    folder: &Path,
    cover_cache: &CoverCache,
    options: TrackDownloadOptions,
) -> Result<TrackDownloadStatus> {
    let mut attempt = 1usize;
    loop {
        match download_track(client, track, folder, cover_cache, options.clone()).await {
            Ok(status) => return Ok(status),
            Err(err) if is_rate_limited_error(&err) => return Err(err),
            Err(err) if attempt < TRACK_DOWNLOAD_MAX_ATTEMPTS => {
                eprintln!(
                    "{} Retrying track after failure (attempt {}/{}): {err:#}",
                    options.progress_scope.label(),
                    attempt + 1,
                    TRACK_DOWNLOAD_MAX_ATTEMPTS
                );
                crate::http::sleep_before_retry(attempt).await;
                attempt += 1;
            }
            Err(err) => return Err(err),
        }
    }
}

async fn download_track(
    client: &TidalClient,
    track: &Track,
    folder: &Path,
    cover_cache: &CoverCache,
    options: TrackDownloadOptions,
) -> Result<TrackDownloadStatus> {
    let progress_scope = options.progress_scope;
    if !track.allow_streaming {
        println!(
            "{} Skipping unavailable track {} ({})",
            progress_scope.label(),
            track.title,
            track.id
        );
        return Ok(TrackDownloadStatus::SkippedUnavailable);
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
    print_playback_selection(progress_scope, &info);

    let filename = track_filename(track, info.extension(), options.playlist_position);
    let target_folder = if options.disc_subdirectories {
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
    let progress_callback = track_progress_callback(progress_scope, track);
    match &info {
        DownloadInfo::Direct {
            url,
            encryption_key,
            native_flac,
            ..
        } if *native_flac => {
            download_to_file(
                client.http_client(),
                url,
                &path,
                encryption_key.as_deref(),
                Some(Arc::clone(&progress_callback)),
            )
            .await
        }
        DownloadInfo::Direct {
            url,
            encryption_key,
            ..
        } => {
            download_direct_to_flac_file(
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
                SegmentedDownload {
                    initialization_url,
                    media_url_template,
                    start_number: *start_number,
                    segment_count: *segment_count,
                    dash_segment_concurrency: options.dash_segment_concurrency,
                },
                &path,
                Some(Arc::clone(&progress_callback)),
            )
            .await
        }
    }
    .with_context(|| format!("failed to download {}", track.id))?;
    if let Err(err) = write_track_metadata(
        client.http_client(),
        track,
        &path,
        cover_cache,
        &options.tag_context,
        &progress_scope.label(),
    )
    .await
    {
        if is_rate_limited_error(&err) {
            return Err(err).context("rate limit while writing track metadata");
        }
        eprintln!(
            "{} Metadata skipped for {}: {err:#}",
            progress_scope.label(),
            track.id
        );
    }
    println!("{} Saved: {}", progress_scope.label(), path.display());
    Ok(TrackDownloadStatus::Saved)
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
        DownloadProgress::DirectEncodingFlac => {
            println!("{label} {title}: encoding source audio to native FLAC");
        }
        DownloadProgress::DirectEncodedFlac => {
            println!("{label} {title}: encoded to native FLAC");
        }
        DownloadProgress::DashInitialized => {
            println!("{label} {title}: DASH initialization downloaded");
        }
        DownloadProgress::DashSegment { downloaded, total } => {
            if should_log_dash_segment_progress(downloaded, total) {
                println!("{label} {title}: DASH segments {downloaded}/{total}");
            }
        }
        DownloadProgress::DashRemuxing => {
            println!("{label} {title}: remuxing DASH FLAC to native FLAC");
        }
        DownloadProgress::DashRemuxed { stream_copy } => {
            if stream_copy {
                println!("{label} {title}: remuxed to native FLAC");
            } else {
                println!("{label} {title}: encoded to native FLAC");
            }
        }
    })
}

fn print_playback_selection(progress_scope: TrackProgressScope, info: &DownloadInfo) {
    println!(
        "{}",
        green_bold(&format!(
            "{} [FLAC PATH] {}; output: {}",
            progress_scope.label(),
            quality_selection_label(info.quality()),
            info.output_path().label()
        ))
    );
}

fn quality_selection_label(quality: PlaybackQuality) -> &'static str {
    match quality {
        PlaybackQuality::HiResLossless => {
            "quality: tried HI_RES_LOSSLESS -> selected HI_RES_LOSSLESS"
        }
        PlaybackQuality::Lossless => "quality: tried HI_RES_LOSSLESS -> fallback selected LOSSLESS",
    }
}

fn green_bold(value: &str) -> String {
    format!("{ANSI_BOLD_GREEN}{value}{ANSI_RESET}")
}

fn red_bold(value: &str) -> String {
    format!("{ANSI_BOLD_RED}{value}{ANSI_RESET}")
}

fn print_missing_tracks(missing_tracks: &[MissingTrack]) {
    if missing_tracks.is_empty() {
        return;
    }

    eprintln!(
        "{}",
        red_bold(&format!("Missing {} track(s):", missing_tracks.len()))
    );
    for missing_track in missing_tracks {
        eprintln!("{}", red_bold(&format_missing_track(missing_track)));
    }
}

fn format_missing_track(missing_track: &MissingTrack) -> String {
    format!(
        "  {} {} - {} [{}] - {}",
        track_scope(missing_track.index, missing_track.total),
        missing_track.artist,
        missing_track.title,
        missing_track.id,
        missing_track.reason
    )
}

fn should_log_dash_segment_progress(downloaded: u32, total: u32) -> bool {
    downloaded == 1 || downloaded == total || downloaded.is_multiple_of(8)
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

    #[test]
    fn formats_missing_track_summary_line() {
        let missing_track = MissingTrack {
            index: 2,
            total: 12,
            id: "42".to_string(),
            artist: "Artist".to_string(),
            title: "Title".to_string(),
            reason: "network timeout".to_string(),
        };

        assert_eq!(
            format_missing_track(&missing_track),
            "  [3/12] Artist - Title [42] - network timeout"
        );
    }

    #[test]
    fn wraps_text_in_bold_red() {
        assert_eq!(red_bold("missing"), "\x1b[1;31mmissing\x1b[0m");
    }

    #[test]
    fn parses_direct_download_url() {
        let cli = Cli::try_parse_from(["tidaload", "https://tidal.com/track/526687566/u"]).unwrap();

        match cli.command {
            Command::Direct(items) => {
                assert_eq!(items, vec!["https://tidal.com/track/526687566/u"]);
            }
            command => panic!("expected direct download command, got {command:?}"),
        }
    }

    #[test]
    fn parses_direct_copied_album_url_with_user_suffix() {
        let cli = Cli::try_parse_from(["tidaload", "https://tidal.com/album/496439179/u"]).unwrap();

        match cli.command {
            Command::Direct(items) => {
                assert_eq!(items, vec!["https://tidal.com/album/496439179/u"]);
            }
            command => panic!("expected direct download command, got {command:?}"),
        }
    }

    #[test]
    fn parses_direct_raw_id_with_global_kind() {
        let cli = Cli::try_parse_from(["tidaload", "--kind", "track", "526687566"]).unwrap();

        assert!(matches!(cli.kind, Some(ResourceKind::Track)));
        match cli.command {
            Command::Direct(items) => assert_eq!(items, vec!["526687566"]),
            command => panic!("expected direct download command, got {command:?}"),
        }
    }

    #[test]
    fn rejects_track_download_concurrency_argument() {
        let err = Cli::try_parse_from([
            "tidaload",
            "--concurrency",
            "8",
            "https://tidal.com/playlist/36ea71a8-445e-41a4-82ab-6628c581535d",
        ])
        .unwrap_err();

        assert_eq!(err.kind(), clap::error::ErrorKind::UnknownArgument);
    }

    #[test]
    fn parses_direct_download_with_dash_segment_concurrency() {
        let cli = Cli::try_parse_from([
            "tidaload",
            "--dash-segment-concurrency",
            "12",
            "https://tidal.com/album/337502805/u",
        ])
        .unwrap();

        assert_eq!(cli.dash_segment_concurrency, Some(12));
        match cli.command {
            Command::Direct(items) => {
                assert_eq!(items, vec!["https://tidal.com/album/337502805/u"]);
            }
            command => panic!("expected direct download command, got {command:?}"),
        }
    }

    #[test]
    fn parses_direct_download_with_download_dir() {
        let cli = Cli::try_parse_from([
            "tidaload",
            "--download-dir",
            "/tmp/tidaload-downloads",
            "https://tidal.com/album/337502805/u",
        ])
        .unwrap();

        assert_eq!(
            cli.download_dir.as_deref(),
            Some(Path::new("/tmp/tidaload-downloads"))
        );
        match cli.command {
            Command::Direct(items) => {
                assert_eq!(items, vec!["https://tidal.com/album/337502805/u"]);
            }
            command => panic!("expected direct download command, got {command:?}"),
        }
    }

    #[test]
    fn keeps_legacy_download_subcommand() {
        let cli =
            Cli::try_parse_from(["tidaload", "download", "--kind", "track", "526687566"]).unwrap();

        assert!(matches!(cli.kind, Some(ResourceKind::Track)));
        match cli.command {
            Command::Download { items } => assert_eq!(items, vec!["526687566"]),
            command => panic!("expected legacy download command, got {command:?}"),
        }
    }
}
