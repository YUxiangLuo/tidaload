use std::path::Path;
use std::process::{Command, Output};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use aes::Aes128;
use anyhow::{Context, Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use cbc::cipher::{BlockDecryptMut, KeyIvInit, StreamCipher, block_padding::NoPadding};
use ctr::Ctr64BE;
use futures_util::{StreamExt, stream};
use tokio::fs;
use tokio::io::AsyncWriteExt;

use crate::http::{
    HTTP_MAX_ATTEMPTS, is_retryable_status, send_bytes_with_retries, sleep_before_retry,
};

type Aes128CbcDec = cbc::Decryptor<Aes128>;
type Aes128Ctr = Ctr64BE<Aes128>;
const DIRECT_PROGRESS_INTERVAL_BYTES: u64 = 2 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
pub enum DownloadProgress {
    DirectBytes { downloaded: u64, total: Option<u64> },
    DirectEncodingFlac,
    DirectEncodedFlac,
    DashInitialized,
    DashSegment { downloaded: u32, total: u32 },
    DashRemuxing,
    DashRemuxed { stream_copy: bool },
}

pub type ProgressCallback = Arc<dyn Fn(DownloadProgress) + Send + Sync>;

#[derive(Debug, Clone, Copy)]
pub struct SegmentedDownload<'a> {
    pub initialization_url: &'a str,
    pub media_url_template: &'a str,
    pub start_number: u32,
    pub segment_count: u32,
    pub dash_segment_concurrency: usize,
}

pub async fn download_to_file(
    client: &reqwest::Client,
    url: &str,
    path: &Path,
    encryption_key: Option<&str>,
    progress: Option<ProgressCallback>,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let tmp_path = path.with_extension("tidaload.part");
    let result = async {
        download_url_to_tmp(client, url, &tmp_path, progress.as_ref()).await?;

        if let Some(key) = encryption_key {
            let encrypted = fs::read(&tmp_path)
                .await
                .with_context(|| format!("failed to read {}", tmp_path.display()))?;
            let decrypted = decrypt_tidal_file(&encrypted, key)?;
            fs::write(&tmp_path, decrypted)
                .await
                .with_context(|| format!("failed to write {}", tmp_path.display()))?;
        }

        fs::rename(&tmp_path, path).await.with_context(|| {
            format!(
                "failed to move {} to {}",
                tmp_path.display(),
                path.display()
            )
        })
    }
    .await;

    if result.is_err() {
        let _ = fs::remove_file(&tmp_path).await;
    }

    result
}

pub async fn download_direct_to_flac_file(
    client: &reqwest::Client,
    url: &str,
    path: &Path,
    encryption_key: Option<&str>,
    progress: Option<ProgressCallback>,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let source_tmp_path = path.with_extension("tidaload.source.m4a");
    let source_part_path = source_tmp_path.with_extension("tidaload.part");
    let flac_tmp_path = path.with_extension("tidaload.part.flac");
    remove_existing_path(&source_tmp_path).await?;
    remove_existing_path(&source_part_path).await?;
    remove_existing_path(&flac_tmp_path).await?;

    let result = async {
        download_to_file(
            client,
            url,
            &source_tmp_path,
            encryption_key,
            progress.clone(),
        )
        .await?;

        if let Some(progress) = progress.as_ref() {
            progress(DownloadProgress::DirectEncodingFlac);
        }
        encode_audio_to_native_flac(&source_tmp_path, &flac_tmp_path).await?;
        if let Some(progress) = progress.as_ref() {
            progress(DownloadProgress::DirectEncodedFlac);
        }

        let _ = fs::remove_file(&source_tmp_path).await;
        fs::rename(&flac_tmp_path, path).await.with_context(|| {
            format!(
                "failed to move {} to {}",
                flac_tmp_path.display(),
                path.display()
            )
        })
    }
    .await;

    if result.is_err() {
        let _ = fs::remove_file(&source_tmp_path).await;
        let _ = fs::remove_file(&source_part_path).await;
        let _ = fs::remove_file(&flac_tmp_path).await;
    }

    result
}

pub async fn download_segmented_to_file(
    client: &reqwest::Client,
    download: SegmentedDownload<'_>,
    path: &Path,
    progress: Option<ProgressCallback>,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let fmp4_tmp_path = path.with_extension("tidaload.dash.m4a");
    let flac_tmp_path = path.with_extension("tidaload.part.flac");
    remove_existing_path(&fmp4_tmp_path).await?;
    remove_existing_path(&flac_tmp_path).await?;

    let result = async {
        let mut output = fs::File::create(&fmp4_tmp_path)
            .await
            .with_context(|| format!("failed to create {}", fmp4_tmp_path.display()))?;

        let initialization = fetch_url_bytes(client, download.initialization_url)
            .await
            .context("failed to download DASH initialization segment")?;
        output
            .write_all(&initialization)
            .await
            .with_context(|| format!("failed to write {}", fmp4_tmp_path.display()))?;
        if let Some(progress) = progress.as_ref() {
            progress(DownloadProgress::DashInitialized);
        }

        let segment_end = download
            .start_number
            .checked_add(download.segment_count)
            .ok_or_else(|| anyhow!("TIDAL DASH manifest has too many segments"))?;
        let downloaded_segments = Arc::new(AtomicU32::new(0));
        let dash_segment_concurrency = download.dash_segment_concurrency.max(1);
        let mut segments = stream::iter(download.start_number..segment_end)
            .map(|number| {
                let client = client.clone();
                let progress = progress.clone();
                let downloaded_segments = Arc::clone(&downloaded_segments);
                let segment_url = dash_segment_url(download.media_url_template, number);
                async move {
                    let bytes = fetch_url_bytes(&client, &segment_url)
                        .await
                        .with_context(|| format!("failed to download DASH segment {number}"))?;
                    if let Some(progress) = progress.as_ref() {
                        let downloaded = downloaded_segments.fetch_add(1, Ordering::SeqCst) + 1;
                        progress(DownloadProgress::DashSegment {
                            downloaded,
                            total: download.segment_count,
                        });
                    }
                    Ok::<Vec<u8>, anyhow::Error>(bytes)
                }
            })
            .buffered(dash_segment_concurrency);

        while let Some(segment) = segments.next().await {
            let segment = segment?;
            output
                .write_all(&segment)
                .await
                .with_context(|| format!("failed to write {}", fmp4_tmp_path.display()))?;
        }

        output.flush().await?;
        drop(output);

        if let Some(progress) = progress.as_ref() {
            progress(DownloadProgress::DashRemuxing);
        }
        let conversion = remux_fmp4_flac_to_flac(&fmp4_tmp_path, &flac_tmp_path).await?;
        if let Some(progress) = progress.as_ref() {
            progress(DownloadProgress::DashRemuxed {
                stream_copy: conversion == FlacConversion::StreamCopy,
            });
        }

        let _ = fs::remove_file(&fmp4_tmp_path).await;
        fs::rename(&flac_tmp_path, path).await.with_context(|| {
            format!(
                "failed to move {} to {}",
                flac_tmp_path.display(),
                path.display()
            )
        })
    }
    .await;

    if result.is_err() {
        let _ = fs::remove_file(&fmp4_tmp_path).await;
        let _ = fs::remove_file(&flac_tmp_path).await;
    }

    result
}

pub async fn remove_existing_path(path: &Path) -> Result<()> {
    match fs::metadata(path).await {
        Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(path)
            .await
            .with_context(|| format!("failed to remove existing directory {}", path.display())),
        Ok(_) => fs::remove_file(path)
            .await
            .with_context(|| format!("failed to remove existing file {}", path.display())),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => {
            Err(err).with_context(|| format!("failed to inspect existing path {}", path.display()))
        }
    }
}

async fn download_url_to_tmp(
    client: &reqwest::Client,
    url: &str,
    tmp_path: &Path,
    progress: Option<&ProgressCallback>,
) -> Result<()> {
    let mut attempt = 1usize;
    loop {
        let mut output = fs::File::create(tmp_path)
            .await
            .with_context(|| format!("failed to create {}", tmp_path.display()))?;

        match append_url_once(client, url, tmp_path, &mut output, progress).await {
            Ok(()) => {
                output.flush().await?;
                return Ok(());
            }
            Err(DownloadAttemptError::Retryable(err)) if attempt < HTTP_MAX_ATTEMPTS => {
                drop(output);
                eprintln!(
                    "retrying download after transient error (attempt {}): {err:#}",
                    attempt + 1
                );
                sleep_before_retry(attempt).await;
                attempt += 1;
            }
            Err(err) => return Err(err.into_error()),
        }
    }
}

async fn append_url_once(
    client: &reqwest::Client,
    url: &str,
    tmp_path: &Path,
    output: &mut fs::File,
    progress: Option<&ProgressCallback>,
) -> std::result::Result<(), DownloadAttemptError> {
    let response = match client
        .get(url)
        .send()
        .await
        .with_context(|| format!("failed to request {url}"))
    {
        Ok(response) => response,
        Err(err) => return Err(DownloadAttemptError::Retryable(err)),
    };
    if is_retryable_status(response.status()) {
        return Err(DownloadAttemptError::Retryable(anyhow!(
            "download request failed for {url}: {}",
            response.status()
        )));
    }
    let response = match response
        .error_for_status()
        .with_context(|| format!("download request failed for {url}"))
    {
        Ok(response) => response,
        Err(err) => return Err(DownloadAttemptError::Fatal(err)),
    };
    append_response_stream(response, tmp_path, output, progress)
        .await
        .map_err(DownloadAttemptError::Retryable)
}

enum DownloadAttemptError {
    Retryable(anyhow::Error),
    Fatal(anyhow::Error),
}

impl DownloadAttemptError {
    fn into_error(self) -> anyhow::Error {
        match self {
            Self::Retryable(err) | Self::Fatal(err) => err,
        }
    }
}

async fn append_response_stream(
    response: reqwest::Response,
    tmp_path: &Path,
    output: &mut fs::File,
    progress: Option<&ProgressCallback>,
) -> Result<()> {
    let total = response.content_length();
    let mut stream = response.bytes_stream();
    let mut downloaded = 0u64;
    let mut next_progress_at = DIRECT_PROGRESS_INTERVAL_BYTES;
    let mut last_progress_downloaded = 0u64;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("failed while reading download stream")?;
        downloaded += chunk.len() as u64;
        output
            .write_all(&chunk)
            .await
            .with_context(|| format!("failed to write {}", tmp_path.display()))?;
        if let Some(progress) = progress
            && downloaded >= next_progress_at
        {
            progress(DownloadProgress::DirectBytes { downloaded, total });
            last_progress_downloaded = downloaded;
            next_progress_at = downloaded + DIRECT_PROGRESS_INTERVAL_BYTES;
        }
    }

    if let Some(progress) = progress.filter(|_| downloaded != last_progress_downloaded) {
        progress(DownloadProgress::DirectBytes { downloaded, total });
    }

    Ok(())
}

async fn fetch_url_bytes(client: &reqwest::Client, url: &str) -> Result<Vec<u8>> {
    let response = send_bytes_with_retries(client.get(url))
        .await
        .with_context(|| format!("failed to request {url}"))?;
    if !response.status.is_success() {
        return Err(anyhow!(
            "download request failed for {url}: {}",
            response.status
        ));
    }
    Ok(response.body)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FlacConversion {
    StreamCopy,
    LosslessEncode,
}

async fn remux_fmp4_flac_to_flac(input: &Path, output: &Path) -> Result<FlacConversion> {
    let input = input.to_path_buf();
    let output = output.to_path_buf();

    tokio::task::spawn_blocking(move || remux_fmp4_flac_to_flac_blocking(&input, &output))
        .await
        .context("ffmpeg remux task failed")?
}

async fn encode_audio_to_native_flac(input: &Path, output: &Path) -> Result<()> {
    let input = input.to_path_buf();
    let output = output.to_path_buf();

    tokio::task::spawn_blocking(move || -> Result<()> {
        remove_file_if_exists(&output)?;
        run_ffmpeg_flac_encode(&input, &output)?;
        verify_native_flac(&output)
    })
    .await
    .context("ffmpeg FLAC encode task failed")?
}

fn remux_fmp4_flac_to_flac_blocking(input: &Path, output: &Path) -> Result<FlacConversion> {
    remove_file_if_exists(output)?;
    match run_ffmpeg_flac_remux(input, output) {
        Ok(()) => {
            verify_native_flac(output)?;
            Ok(FlacConversion::StreamCopy)
        }
        Err(remux_error) => {
            remove_file_if_exists(output)?;
            run_ffmpeg_flac_encode(input, output).with_context(|| {
                format!(
                    "ffmpeg FLAC stream-copy remux failed; fallback lossless encode also failed; stream-copy error: {remux_error:#}"
                )
            })?;
            verify_native_flac(output)?;
            Ok(FlacConversion::LosslessEncode)
        }
    }
}

fn run_ffmpeg_flac_remux(input: &Path, output: &Path) -> Result<()> {
    let command = Command::new("ffmpeg")
        .arg("-hide_banner")
        .arg("-nostdin")
        .arg("-y")
        .arg("-i")
        .arg(input)
        .arg("-map")
        .arg("0:a:0")
        .arg("-vn")
        .arg("-sn")
        .arg("-dn")
        .arg("-c:a")
        .arg("copy")
        .arg(output)
        .output()
        .context("failed to run ffmpeg for FLAC remux")?;
    ensure_command_success("ffmpeg FLAC remux", command)
}

fn run_ffmpeg_flac_encode(input: &Path, output: &Path) -> Result<()> {
    let command = Command::new("ffmpeg")
        .arg("-hide_banner")
        .arg("-nostdin")
        .arg("-y")
        .arg("-i")
        .arg(input)
        .arg("-map")
        .arg("0:a:0")
        .arg("-vn")
        .arg("-sn")
        .arg("-dn")
        .arg("-c:a")
        .arg("flac")
        .arg("-compression_level")
        .arg("8")
        .arg(output)
        .output()
        .context("failed to run ffmpeg for FLAC encode")?;
    ensure_command_success("ffmpeg FLAC encode", command)
}

fn verify_native_flac(path: &Path) -> Result<()> {
    let output = Command::new("ffprobe")
        .arg("-hide_banner")
        .arg("-v")
        .arg("error")
        .arg("-select_streams")
        .arg("a:0")
        .arg("-show_entries")
        .arg("stream=codec_name")
        .arg("-show_entries")
        .arg("format=format_name")
        .arg("-of")
        .arg("default=nw=1")
        .arg(path)
        .output()
        .context("failed to run ffprobe for FLAC verification")?;
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    ensure_command_success("ffprobe FLAC verification", output)?;

    if !stdout.contains("codec_name=flac") || !stdout.contains("format_name=flac") {
        return Err(anyhow!(
            "ffprobe did not identify {} as native FLAC: {stdout}",
            path.display()
        ));
    }
    Ok(())
}

fn ensure_command_success(name: &str, output: Output) -> Result<()> {
    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    Err(anyhow!(
        "{name} failed with status {}: stderr: {}; stdout: {}",
        output.status,
        stderr.trim(),
        stdout.trim()
    ))
}

fn remove_file_if_exists(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("failed to remove {}", path.display())),
    }
}

fn dash_segment_url(media_url_template: &str, number: u32) -> String {
    media_url_template.replace("$Number$", &number.to_string())
}

fn decrypt_tidal_file(data: &[u8], encryption_key: &str) -> Result<Vec<u8>> {
    let master_key = STANDARD
        .decode("UIlTTEMmmLfGowo/UC60x2H45W6MdGgTRfo/umg4754=")
        .context("invalid TIDAL master key")?;
    let security_token = STANDARD
        .decode(encryption_key)
        .context("invalid TIDAL encryption key")?;

    if security_token.len() < 32 {
        return Err(anyhow!("invalid TIDAL security token"));
    }

    let (iv, encrypted_token) = security_token.split_at(16);
    let mut token_buf = encrypted_token.to_vec();
    let decrypted_token = Aes128CbcDec::new_from_slices(&master_key, iv)
        .map_err(|_| anyhow!("failed to initialize TIDAL token decryptor"))?
        .decrypt_padded_mut::<NoPadding>(&mut token_buf)
        .map_err(|_| anyhow!("failed to decrypt TIDAL security token"))?;

    if decrypted_token.len() < 24 {
        return Err(anyhow!("invalid decrypted TIDAL security token"));
    }

    let (key, rest) = decrypted_token.split_at(16);
    let nonce = &rest[..8];
    let mut decrypted = data.to_vec();
    Aes128Ctr::new_from_slices(key, nonce)
        .map_err(|_| anyhow!("failed to initialize TIDAL stream decryptor"))?
        .apply_keystream(&mut decrypted);

    Ok(decrypted)
}

pub fn sanitize_file_name(value: &str) -> String {
    let mut cleaned = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => cleaned.push('_'),
            c if c.is_control() => cleaned.push('_'),
            c => cleaned.push(c),
        }
    }

    let cleaned = cleaned.trim().trim_matches('.').to_string();
    if cleaned.is_empty() {
        "Unknown".to_string()
    } else {
        cleaned
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_dash_segment_url() {
        assert_eq!(
            dash_segment_url("https://audio.example/$Number$.mp4?token=x", 42),
            "https://audio.example/42.mp4?token=x"
        );
    }
}
