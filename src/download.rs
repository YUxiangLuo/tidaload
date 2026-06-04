use std::path::{Path, PathBuf};
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
    HTTP_MAX_ATTEMPTS, is_rate_limited_error, is_retryable_status, rate_limit_error,
    send_bytes_with_retries, sleep_before_retry,
};

type Aes128CbcDec = cbc::Decryptor<Aes128>;
type Aes128Ctr = Ctr64BE<Aes128>;
const DIRECT_PROGRESS_INTERVAL_BYTES: u64 = 2 * 1024 * 1024;

struct TempPathGuard {
    paths: Vec<PathBuf>,
}

impl TempPathGuard {
    fn new(paths: impl IntoIterator<Item = PathBuf>) -> Self {
        Self {
            paths: paths.into_iter().collect(),
        }
    }
}

impl Drop for TempPathGuard {
    fn drop(&mut self) {
        for path in &self.paths {
            let _ = std::fs::remove_file(path);
        }
    }
}

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
    let _tmp_guard = TempPathGuard::new([tmp_path.clone()]);
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
    let _tmp_guard = TempPathGuard::new([
        source_tmp_path.clone(),
        source_part_path.clone(),
        flac_tmp_path.clone(),
    ]);
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
    let _tmp_guard = TempPathGuard::new([fmp4_tmp_path.clone(), flac_tmp_path.clone()]);
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
            Err(DownloadAttemptError::Retryable(err)) if is_rate_limited_error(&err) => {
                return Err(err);
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
    let status = response.status();
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return Err(DownloadAttemptError::Retryable(rate_limit_error(url)));
    }
    if is_retryable_status(status) {
        return Err(DownloadAttemptError::Retryable(anyhow!(
            "download request failed for {url}: {}",
            status
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
    if response.status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return Err(rate_limit_error(url));
    }
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
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread::JoinHandle;

    use super::*;

    #[test]
    fn builds_dash_segment_url() {
        assert_eq!(
            dash_segment_url("https://audio.example/$Number$.mp4?token=x", 42),
            "https://audio.example/42.mp4?token=x"
        );
    }

    #[test]
    fn temp_path_guard_removes_registered_files_on_drop() -> Result<()> {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "tidaload-temp-guard-test-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir)?;
        let path = dir.join("track.tidaload.part");
        std::fs::write(&path, b"partial")?;

        {
            let _guard = TempPathGuard::new([path.clone()]);
        }

        assert!(!path.exists());
        std::fs::remove_dir_all(dir)?;
        Ok(())
    }

    #[tokio::test]
    async fn direct_download_returns_rate_limit_error_for_http_429() -> Result<()> {
        let server = TestHttpServer::new(vec![TestResponse::new(
            429,
            "Too Many Requests",
            b"limited".to_vec(),
        )])?;
        let client = reqwest::Client::builder().no_proxy().build()?;
        let path = unique_temp_path("limited.flac")?;

        let err = download_to_file(&client, &server.url("/track.flac"), &path, None, None)
            .await
            .unwrap_err();

        assert!(crate::http::is_rate_limited_error(&err));
        assert!(!path.exists());
        assert_eq!(server.join(), vec!["/track.flac"]);
        Ok(())
    }

    #[tokio::test]
    async fn dash_segment_fetch_retries_transient_server_error() -> Result<()> {
        let server = TestHttpServer::new(vec![
            TestResponse::new(503, "Service Unavailable", Vec::new()),
            TestResponse::new(200, "OK", b"segment".to_vec()),
        ])?;
        let client = reqwest::Client::builder().no_proxy().build()?;

        let bytes = fetch_url_bytes(&client, &server.url("/segments/1.m4s")).await?;

        assert_eq!(bytes, b"segment");
        assert_eq!(server.join(), vec!["/segments/1.m4s", "/segments/1.m4s"]);
        Ok(())
    }

    struct TestResponse {
        status: u16,
        reason: &'static str,
        body: Vec<u8>,
    }

    impl TestResponse {
        fn new(status: u16, reason: &'static str, body: Vec<u8>) -> Self {
            Self {
                status,
                reason,
                body,
            }
        }
    }

    struct TestHttpServer {
        base_url: String,
        handle: JoinHandle<Vec<String>>,
    }

    impl TestHttpServer {
        fn new(responses: Vec<TestResponse>) -> Result<Self> {
            let listener = TcpListener::bind(("127.0.0.1", 0))?;
            let addr = listener.local_addr()?;
            let handle = std::thread::spawn(move || {
                let mut paths = Vec::new();
                for response in responses {
                    let (mut stream, _) = listener.accept().expect("test HTTP accept failed");
                    stream
                        .set_read_timeout(Some(std::time::Duration::from_secs(2)))
                        .expect("test HTTP timeout failed");
                    let path = read_request_path(&mut stream);
                    paths.push(path);

                    let headers = format!(
                        "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        response.status,
                        response.reason,
                        response.body.len()
                    );
                    stream
                        .write_all(headers.as_bytes())
                        .expect("test HTTP headers write failed");
                    stream
                        .write_all(&response.body)
                        .expect("test HTTP body write failed");
                }
                paths
            });

            Ok(Self {
                base_url: format!("http://{addr}"),
                handle,
            })
        }

        fn url(&self, path: &str) -> String {
            format!("{}{}", self.base_url, path)
        }

        fn join(self) -> Vec<String> {
            self.handle.join().expect("test HTTP thread panicked")
        }
    }

    fn read_request_path(stream: &mut std::net::TcpStream) -> String {
        let mut request = Vec::new();
        let mut chunk = [0u8; 1024];
        loop {
            match stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(read) => {
                    request.extend_from_slice(&chunk[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                Err(_) => break,
            }
        }

        String::from_utf8_lossy(&request)
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .unwrap_or("/")
            .to_string()
    }

    fn unique_temp_path(name: &str) -> Result<PathBuf> {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        Ok(std::env::temp_dir().join(format!(
            "tidaload-download-test-{}-{unique}-{name}",
            std::process::id()
        )))
    }
}
