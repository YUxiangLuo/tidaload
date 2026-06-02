#[cfg(not(target_os = "linux"))]
compile_error!("tidaload currently supports Linux only");

mod config;
mod doh;
mod download;
mod tidal;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use futures_util::stream::{self, StreamExt};
use mp4ameta::{Img, Tag};
use tokio::sync::{Mutex, Semaphore};

use crate::config::{Config, default_config_path, music_download_dir};
use crate::download::{
    download_segmented_to_file, download_to_file, remove_existing_path, sanitize_file_name,
};
use crate::tidal::{
    Album, DownloadInfo, ParsedResource, Playlist, ResourceKind, TidalClient, Track,
    cover_image_url, parse_resource,
};

type CoverCache = Arc<Mutex<HashMap<String, Arc<Vec<u8>>>>>;

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
    let semaphore = Arc::new(Semaphore::new(config.downloads.concurrency.max(1)));
    let cover_cache = Arc::new(Mutex::new(HashMap::new()));

    let results: Vec<Result<()>> = stream::iter(tracks.into_iter().enumerate())
        .map(|(index, track)| {
            let semaphore = Arc::clone(&semaphore);
            let cover_cache = Arc::clone(&cover_cache);
            let folder = folder.clone();
            async move {
                let _permit = semaphore.acquire_owned().await?;
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
                download_track(
                    client,
                    track,
                    &folder,
                    playlist_position,
                    disc_subdirectories,
                    &cover_cache,
                )
                .await
            }
        })
        .buffer_unordered(config.downloads.concurrency.max(1))
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
    download_track(client, track, folder, None, false, &cover_cache).await
}

async fn download_track(
    client: &TidalClient,
    track: Track,
    folder: &Path,
    playlist_position: Option<usize>,
    disc_subdirectories: bool,
    cover_cache: &CoverCache,
) -> Result<()> {
    if !track.allow_streaming {
        println!("Skipping unavailable track {} ({})", track.title, track.id);
        return Ok(());
    }

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

    println!("Downloading: {} - {}", track.artist, track.title);
    match &info {
        DownloadInfo::Direct {
            url,
            encryption_key,
            ..
        } => download_to_file(client.http_client(), url, &path, encryption_key.as_deref()).await,
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
            )
            .await
        }
    }
    .with_context(|| format!("failed to download {}", track.id))?;
    embed_cover_art(client.http_client(), &track, &path, cover_cache)
        .await
        .with_context(|| format!("failed to embed cover art for {}", track.id))?;
    println!("Saved: {}", path.display());
    Ok(())
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
