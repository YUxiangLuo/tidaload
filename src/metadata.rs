use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use metaflac::Tag as FlacTag;
use metaflac::block::{BlockType, PictureType};
use mp4ameta::{Img, Tag as Mp4Tag};
use tokio::sync::{Mutex, OnceCell};

use crate::http::{HTTP_SHORT_REQUEST_TIMEOUT, is_rate_limited_error, send_bytes_with_retries};
use crate::tidal::{Album, Track, cover_image_url};

type CoverBytes = Arc<Vec<u8>>;
type CoverCell = Arc<OnceCell<CoverBytes>>;
pub type CoverCache = Arc<Mutex<HashMap<String, CoverCell>>>;

#[derive(Debug, Default, Clone)]
pub struct TrackTagContext {
    album_title: Option<String>,
    album_artist: Option<String>,
    album_year: Option<String>,
    track_total: Option<u64>,
    disc_total: Option<u64>,
}

#[derive(Debug, Clone)]
struct TrackMetadata {
    title: String,
    artist: String,
    album: Option<String>,
    album_artist: Option<String>,
    year: Option<String>,
    track_number: Option<u64>,
    track_total: Option<u64>,
    disc_number: Option<u64>,
    disc_total: Option<u64>,
    cover_uuid: Option<String>,
}

impl TrackTagContext {
    pub fn album(album: &Album) -> Self {
        Self {
            album_title: Some(album.title.clone()),
            album_artist: Some(album.artist.clone()),
            album_year: album.year.clone(),
            track_total: Some(album.tracks.len() as u64),
            disc_total: Some(album.disc_total),
        }
    }
}

impl TrackMetadata {
    fn from_track(track: &Track, context: &TrackTagContext) -> Self {
        let album = context
            .album_title
            .clone()
            .or_else(|| track.album_title.clone());
        let album_artist = context
            .album_artist
            .clone()
            .or_else(|| track.album_artist.clone())
            .or_else(|| Some(track.artist.clone()));

        Self {
            title: track.title.clone(),
            artist: track.artist.clone(),
            album,
            album_artist,
            year: context
                .album_year
                .clone()
                .or_else(|| track.album_year.clone()),
            track_number: track.track_number,
            track_total: context.track_total.or(track.album_track_total),
            disc_number: track.volume_number,
            disc_total: context.disc_total.or(track.album_disc_total),
            cover_uuid: track.cover_uuid.clone(),
        }
    }
}

pub fn new_cover_cache() -> CoverCache {
    Arc::new(Mutex::new(HashMap::new()))
}

pub async fn write_track_metadata(
    client: &reqwest::Client,
    track: &Track,
    path: &Path,
    cover_cache: &CoverCache,
    context: &TrackTagContext,
    progress_label: &str,
) -> Result<()> {
    let metadata = TrackMetadata::from_track(track, context);
    let cover = if let Some(cover_uuid) = metadata.cover_uuid.as_deref() {
        match cover_art_bytes(client, cover_uuid, cover_cache).await {
            Ok(cover) => Some(cover.as_ref().clone()),
            Err(err) if is_rate_limited_error(&err) => return Err(err),
            Err(err) => {
                eprintln!(
                    "{progress_label} Cover art skipped for {}: {err:#}",
                    track.id
                );
                None
            }
        }
    } else {
        None
    };

    let path = path.to_path_buf();

    tokio::task::spawn_blocking(move || write_track_metadata_for_path(&path, &metadata, cover))
        .await
        .context("track metadata writer task failed")?
}

fn write_track_metadata_for_path(
    path: &Path,
    metadata: &TrackMetadata,
    cover: Option<Vec<u8>>,
) -> Result<()> {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("flac") => write_flac_metadata(path, metadata, cover),
        Some("m4a" | "mp4") => write_mp4_metadata(path, metadata, cover),
        _ => Ok(()),
    }
}

fn write_flac_metadata(
    path: &Path,
    metadata: &TrackMetadata,
    cover: Option<Vec<u8>>,
) -> Result<()> {
    let mut tag = FlacTag::read_from_path(path)
        .with_context(|| format!("failed to read FLAC metadata from {}", path.display()))?;
    apply_flac_metadata(&mut tag, metadata, cover);
    tag.save()
        .with_context(|| format!("failed to write FLAC metadata to {}", path.display()))
}

fn apply_flac_metadata(tag: &mut FlacTag, metadata: &TrackMetadata, cover: Option<Vec<u8>>) {
    set_flac_vorbis(tag, "TITLE", Some(metadata.title.as_str()));
    set_flac_vorbis(tag, "ARTIST", Some(metadata.artist.as_str()));
    set_flac_vorbis(tag, "ALBUM", metadata.album.as_deref());
    set_flac_vorbis(tag, "ALBUMARTIST", metadata.album_artist.as_deref());
    set_flac_vorbis(tag, "ALBUM ARTIST", metadata.album_artist.as_deref());
    set_flac_vorbis(tag, "DATE", metadata.year.as_deref());
    set_flac_vorbis(tag, "YEAR", metadata.year.as_deref());
    set_flac_vorbis_string(tag, "TRACKNUMBER", metadata.track_number);
    set_flac_vorbis_string(tag, "TRACKTOTAL", metadata.track_total);
    set_flac_vorbis_string(tag, "TOTALTRACKS", metadata.track_total);
    set_flac_vorbis_string(tag, "DISCNUMBER", metadata.disc_number);
    set_flac_vorbis_string(tag, "DISCTOTAL", metadata.disc_total);
    set_flac_vorbis_string(tag, "TOTALDISCS", metadata.disc_total);

    if let Some(cover) = cover {
        tag.remove_blocks(BlockType::Picture);
        tag.add_picture("image/jpeg", PictureType::CoverFront, cover);
    }
}

fn set_flac_vorbis(tag: &mut FlacTag, key: &str, value: Option<&str>) {
    if let Some(value) = value.filter(|value| !value.is_empty()) {
        tag.set_vorbis(key, vec![value]);
    } else {
        tag.remove_vorbis(key);
    }
}

fn set_flac_vorbis_string(tag: &mut FlacTag, key: &str, value: Option<u64>) {
    if let Some(value) = value {
        tag.set_vorbis(key, vec![value.to_string()]);
    } else {
        tag.remove_vorbis(key);
    }
}

fn write_mp4_metadata(path: &Path, metadata: &TrackMetadata, cover: Option<Vec<u8>>) -> Result<()> {
    let mut tag = Mp4Tag::read_from_path(path)
        .with_context(|| format!("failed to read MP4 metadata from {}", path.display()))?;
    tag.set_title(metadata.title.clone());
    tag.set_artist(metadata.artist.clone());
    if let Some(album) = metadata.album.as_deref() {
        tag.set_album(album);
    } else {
        tag.remove_album();
    }
    if let Some(album_artist) = metadata.album_artist.as_deref() {
        tag.set_album_artist(album_artist);
    } else {
        tag.remove_album_artists();
    }
    if let Some(year) = metadata.year.as_deref() {
        tag.set_year(year);
    } else {
        tag.remove_year();
    }
    set_mp4_track(&mut tag, metadata.track_number, metadata.track_total);
    set_mp4_disc(&mut tag, metadata.disc_number, metadata.disc_total);

    if let Some(cover) = cover {
        tag.set_artwork(Img::jpeg(cover));
    }

    tag.write_to_path(path)
        .with_context(|| format!("failed to write MP4 metadata to {}", path.display()))
}

fn set_mp4_track(tag: &mut Mp4Tag, track_number: Option<u64>, track_total: Option<u64>) {
    match (u64_to_u16(track_number), u64_to_u16(track_total)) {
        (Some(track_number), Some(track_total)) => tag.set_track(track_number, track_total),
        (Some(track_number), None) => tag.set_track_number(track_number),
        (None, Some(track_total)) => tag.set_total_tracks(track_total),
        (None, None) => tag.remove_track(),
    }
}

fn set_mp4_disc(tag: &mut Mp4Tag, disc_number: Option<u64>, disc_total: Option<u64>) {
    match (u64_to_u16(disc_number), u64_to_u16(disc_total)) {
        (Some(disc_number), Some(disc_total)) => tag.set_disc(disc_number, disc_total),
        (Some(disc_number), None) => tag.set_disc_number(disc_number),
        (None, Some(disc_total)) => tag.set_total_discs(disc_total),
        (None, None) => tag.remove_disc(),
    }
}

fn u64_to_u16(value: Option<u64>) -> Option<u16> {
    value.and_then(|value| u16::try_from(value).ok())
}

async fn cover_art_bytes(
    client: &reqwest::Client,
    cover_uuid: &str,
    cover_cache: &CoverCache,
) -> Result<Arc<Vec<u8>>> {
    let cover_cell = {
        let mut cache = cover_cache.lock().await;
        Arc::clone(
            cache
                .entry(cover_uuid.to_string())
                .or_insert_with(|| Arc::new(OnceCell::new())),
        )
    };

    let cover_uuid = cover_uuid.to_string();
    let cover = cover_cell
        .get_or_try_init(|| async {
            download_cover_art_bytes(client, &cover_uuid)
                .await
                .map(Arc::new)
        })
        .await?;
    Ok(Arc::clone(cover))
}

async fn download_cover_art_bytes(client: &reqwest::Client, cover_uuid: &str) -> Result<Vec<u8>> {
    let url = cover_image_url(cover_uuid);
    let cover = client.get(&url).timeout(HTTP_SHORT_REQUEST_TIMEOUT);
    let cover = send_bytes_with_retries(cover)
        .await
        .with_context(|| format!("failed to request cover art {url}"))?;
    if !cover.status.is_success() {
        bail!("cover art request failed for {url}: {}", cover.status);
    }
    Ok(cover.body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn track_metadata_prefers_album_context() {
        let track = Track {
            id: "42".to_string(),
            title: "Track".to_string(),
            artist: "Track Artist".to_string(),
            album_title: Some("Original Album".to_string()),
            album_artist: Some("Original Album Artist".to_string()),
            album_year: Some("2020".to_string()),
            album_track_total: Some(10),
            album_disc_total: Some(1),
            track_number: Some(3),
            volume_number: Some(2),
            cover_uuid: Some("abcd-efgh".to_string()),
            allow_streaming: true,
        };
        let context = TrackTagContext {
            album_title: Some("Context Album".to_string()),
            album_artist: Some("Context Artist".to_string()),
            album_year: Some("2024".to_string()),
            track_total: Some(12),
            disc_total: Some(2),
        };

        let metadata = TrackMetadata::from_track(&track, &context);

        assert_eq!(metadata.title, "Track");
        assert_eq!(metadata.artist, "Track Artist");
        assert_eq!(metadata.album.as_deref(), Some("Context Album"));
        assert_eq!(metadata.album_artist.as_deref(), Some("Context Artist"));
        assert_eq!(metadata.year.as_deref(), Some("2024"));
        assert_eq!(metadata.track_number, Some(3));
        assert_eq!(metadata.track_total, Some(12));
        assert_eq!(metadata.disc_number, Some(2));
        assert_eq!(metadata.disc_total, Some(2));
        assert_eq!(metadata.cover_uuid.as_deref(), Some("abcd-efgh"));
    }

    #[test]
    fn applies_flac_metadata_with_player_compatibility_aliases() {
        let metadata = TrackMetadata {
            title: "Track".to_string(),
            artist: "Track Artist".to_string(),
            album: Some("Album".to_string()),
            album_artist: Some("Album Artist".to_string()),
            year: Some("2024".to_string()),
            track_number: Some(3),
            track_total: Some(12),
            disc_number: Some(1),
            disc_total: Some(2),
            cover_uuid: None,
        };
        let mut tag = FlacTag::new();

        apply_flac_metadata(&mut tag, &metadata, None);

        assert_eq!(single_vorbis(&tag, "TITLE"), Some("Track"));
        assert_eq!(single_vorbis(&tag, "ARTIST"), Some("Track Artist"));
        assert_eq!(single_vorbis(&tag, "ALBUM"), Some("Album"));
        assert_eq!(single_vorbis(&tag, "ALBUMARTIST"), Some("Album Artist"));
        assert_eq!(single_vorbis(&tag, "ALBUM ARTIST"), Some("Album Artist"));
        assert_eq!(single_vorbis(&tag, "DATE"), Some("2024"));
        assert_eq!(single_vorbis(&tag, "YEAR"), Some("2024"));
        assert_eq!(single_vorbis(&tag, "TRACKNUMBER"), Some("3"));
        assert_eq!(single_vorbis(&tag, "TRACKTOTAL"), Some("12"));
        assert_eq!(single_vorbis(&tag, "TOTALTRACKS"), Some("12"));
        assert_eq!(single_vorbis(&tag, "DISCNUMBER"), Some("1"));
        assert_eq!(single_vorbis(&tag, "DISCTOTAL"), Some("2"));
        assert_eq!(single_vorbis(&tag, "TOTALDISCS"), Some("2"));
    }

    fn single_vorbis<'a>(tag: &'a FlacTag, key: &str) -> Option<&'a str> {
        tag.get_vorbis(key)?.next()
    }
}
