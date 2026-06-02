#[cfg(not(target_os = "linux"))]
compile_error!("tidaload currently supports Linux only");

mod config;
mod download;
mod tidal;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use futures_util::stream::{self, StreamExt};
use tokio::sync::Semaphore;

use crate::config::{Config, default_config_path, music_download_dir};
use crate::download::{download_to_file, remove_existing_path, sanitize_file_name};
use crate::tidal::{
    Album, ParsedResource, Playlist, ResourceKind, TidalClient, Track, parse_resource,
};

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

        #[arg(short, long)]
        quality: Option<u8>,

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
            quality,
            concurrency,
        } => {
            if let Some(quality) = quality {
                config.tidal.quality = quality.min(3);
            }
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
            download_track(client, config, track, download_root, None).await?;
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
        "Downloading album: {} - {} [{}] ({} tracks)",
        album.artist,
        album.title,
        album.id,
        album.tracks.len()
    );

    let year = album.year.as_deref().unwrap_or("Unknown Year");
    let folder = download_root.join(sanitize_file_name(&format!(
        "{} - {} ({})",
        album.artist, album.title, year
    )));
    remove_existing_path(&folder).await?;
    download_tracks_concurrently(client, config, album.tracks, folder, true).await
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
    download_tracks_concurrently(client, config, playlist.tracks, folder, true).await
}

async fn download_tracks_concurrently(
    client: &TidalClient,
    config: &Config,
    tracks: Vec<Track>,
    folder: PathBuf,
    include_track_number: bool,
) -> Result<()> {
    let semaphore = Arc::new(Semaphore::new(config.downloads.concurrency.max(1)));

    stream::iter(tracks.into_iter().enumerate())
        .map(|(index, track)| {
            let semaphore = Arc::clone(&semaphore);
            let folder = folder.clone();
            async move {
                let _permit = semaphore.acquire_owned().await?;
                download_track(client, config, track, &folder, Some(index + 1)).await
            }
        })
        .buffer_unordered(config.downloads.concurrency.max(1))
        .for_each(|result| async {
            if let Err(err) = result {
                eprintln!("download failed: {err:#}");
            }
        })
        .await;

    if !include_track_number {
        println!("Downloaded tracks to {}", folder.display());
    }
    Ok(())
}

async fn download_track(
    client: &TidalClient,
    config: &Config,
    track: Track,
    folder: &Path,
    playlist_position: Option<usize>,
) -> Result<()> {
    if !track.allow_streaming {
        println!("Skipping unavailable track {} ({})", track.title, track.id);
        return Ok(());
    }

    let info = client
        .get_download_info(&track.id, config.tidal.quality)
        .await
        .with_context(|| format!("failed to get playback info for {}", track.id))?;

    let filename = track_filename(&track, &info.extension, playlist_position);
    let path = folder.join(filename);
    remove_existing_path(&path).await?;

    println!("Downloading: {} - {}", track.artist, track.title);
    download_to_file(
        client.http_client(),
        &info.url,
        &path,
        info.encryption_key.as_deref(),
    )
    .await
    .with_context(|| format!("failed to download {}", track.id))?;
    println!("Saved: {}", path.display());
    Ok(())
}

fn track_filename(track: &Track, extension: &str, playlist_position: Option<usize>) -> String {
    let number = playlist_position
        .or_else(|| track.track_number.map(|n| n as usize))
        .map(|n| format!("{n:02}. "))
        .unwrap_or_default();
    let name = sanitize_file_name(&format!("{number}{} - {}", track.artist, track.title));
    format!("{name}.{extension}")
}
