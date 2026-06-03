use std::fmt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use reqwest::StatusCode;
use roxmltree::{Document, Node};
use serde::Deserialize;
use serde_json::Value;
use tokio::time::{Duration, sleep};

use crate::config::TidalConfig;
use crate::doh::DohResolver;
use crate::http::{
    HTTP_SHORT_REQUEST_TIMEOUT, configure_http_client, send_json_with_retries,
    send_text_with_retries,
};

const BASE: &str = "https://api.tidalhifi.com/v1";
const SESSION_URL: &str = "https://api.tidal.com/v1/sessions";
const AUTH_URL: &str = "https://auth.tidal.com/v1/oauth2";
const SCOPE: &str = "r_usr+w_usr+w_sub";

const CLIENT_ID_B64: &str = "ZlgySnhkbW50WldLMGl4VA==";
const CLIENT_SECRET_B64: &str = "MU5tNUFmREFqeHJnSkZKYktOV0xlQXlLR1ZHbUlOdVhQUExIVlhBdnhBZz0=";

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum ResourceKind {
    Track,
    Album,
    Playlist,
}

#[derive(Debug, Clone)]
pub struct ParsedResource {
    pub kind: ResourceKind,
    pub id: String,
}

#[derive(Debug, Clone)]
pub struct Album {
    pub id: String,
    pub title: String,
    pub artist: String,
    pub year: Option<String>,
    pub disc_total: u64,
    pub tracks: Vec<Track>,
}

#[derive(Debug, Clone)]
pub struct Playlist {
    pub id: String,
    pub title: String,
    pub tracks: Vec<Track>,
}

#[derive(Debug, Clone)]
pub struct Track {
    pub id: String,
    pub title: String,
    pub artist: String,
    pub album_title: Option<String>,
    pub album_artist: Option<String>,
    pub album_year: Option<String>,
    pub album_track_total: Option<u64>,
    pub album_disc_total: Option<u64>,
    pub track_number: Option<u64>,
    pub volume_number: Option<u64>,
    pub cover_uuid: Option<String>,
    pub allow_streaming: bool,
}

#[derive(Debug, Clone)]
pub enum DownloadInfo {
    Direct {
        url: String,
        extension: String,
        encryption_key: Option<String>,
        native_flac: bool,
        quality: PlaybackQuality,
    },
    Segmented {
        initialization_url: String,
        media_url_template: String,
        start_number: u32,
        segment_count: u32,
        extension: String,
        quality: PlaybackQuality,
    },
}

impl DownloadInfo {
    pub fn extension(&self) -> &str {
        match self {
            Self::Direct { extension, .. } | Self::Segmented { extension, .. } => extension,
        }
    }

    pub fn quality(&self) -> PlaybackQuality {
        match self {
            Self::Direct { quality, .. } | Self::Segmented { quality, .. } => *quality,
        }
    }

    pub fn output_path(&self) -> OutputPath {
        match self {
            Self::Direct {
                native_flac: true, ..
            } => OutputPath::DirectNative,
            Self::Direct { .. } => OutputPath::DirectEncode,
            Self::Segmented { .. } => OutputPath::DashRemux,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlaybackQuality {
    HiResLossless,
    Lossless,
}

impl PlaybackQuality {
    pub fn as_tidal_param(self) -> &'static str {
        match self {
            Self::HiResLossless => "HI_RES_LOSSLESS",
            Self::Lossless => "LOSSLESS",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputPath {
    DashRemux,
    DirectNative,
    DirectEncode,
}

impl OutputPath {
    pub fn label(self) -> &'static str {
        match self {
            Self::DashRemux => "DASH/fMP4 FLAC -> ffmpeg stream-copy remux -> native .flac",
            Self::DirectNative => "direct native FLAC -> native .flac",
            Self::DirectEncode => "direct non-FLAC -> ffmpeg FLAC encode -> native .flac",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlaybackFallbackReason {
    MissingManifest,
    MqaSource,
    UnsupportedDashCodec,
}

impl PlaybackFallbackReason {
    fn label(self) -> &'static str {
        match self {
            Self::MissingManifest => "missing manifest",
            Self::MqaSource => "MQA source",
            Self::UnsupportedDashCodec => "unsupported DASH codec",
        }
    }
}

#[derive(Debug)]
struct PlaybackFallbackError {
    quality: PlaybackQuality,
    reason: PlaybackFallbackReason,
    message: String,
}

impl PlaybackFallbackError {
    fn new(quality: PlaybackQuality, reason: PlaybackFallbackReason, message: String) -> Self {
        Self {
            quality,
            reason,
            message,
        }
    }
}

impl fmt::Display for PlaybackFallbackError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} fallback candidate for {}: {}",
            self.reason.label(),
            self.quality.as_tidal_param(),
            self.message
        )
    }
}

impl std::error::Error for PlaybackFallbackError {}

fn playback_fallback_error(
    quality: PlaybackQuality,
    reason: PlaybackFallbackReason,
    message: String,
) -> anyhow::Error {
    PlaybackFallbackError::new(quality, reason, message).into()
}

fn lossless_fallback_allowed(error: &anyhow::Error) -> bool {
    error.downcast_ref::<PlaybackFallbackError>().is_some()
}

#[derive(Debug, Deserialize)]
struct DeviceAuthorization {
    #[serde(rename = "deviceCode")]
    device_code: String,
    #[serde(rename = "verificationUriComplete")]
    verification_uri_complete: String,
}

#[derive(Debug, Deserialize)]
struct LoginUser {
    #[serde(rename = "userId")]
    user_id: u64,
    #[serde(rename = "countryCode")]
    country_code: String,
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    user: LoginUser,
    access_token: String,
    refresh_token: Option<String>,
    expires_in: f64,
}

pub struct TidalClient {
    client: reqwest::Client,
    client_id: String,
    client_secret: String,
    pub config: TidalConfig,
}

impl TidalClient {
    pub fn new(config: TidalConfig) -> Result<Self> {
        let client_id = decode_constant(CLIENT_ID_B64)?;
        let client_secret = decode_constant(CLIENT_SECRET_B64)?;
        let client = tidal_client_builder()?
            .build()
            .context("failed to build HTTP client")?;

        Ok(Self {
            client,
            client_id,
            client_secret,
            config,
        })
    }

    pub fn http_client(&self) -> &reqwest::Client {
        &self.client
    }

    pub async fn ensure_login(&mut self) -> Result<bool> {
        if self.config.access_token.is_empty() {
            bail!("not logged in; run `tidaload login` first");
        }

        if self.config.token_expiry - now_secs() < 86_400.0 {
            self.refresh_access_token().await?;
            Ok(true)
        } else {
            self.login_by_access_token().await?;
            Ok(false)
        }
    }

    pub async fn login_device_flow(&mut self) -> Result<()> {
        let auth = self.get_device_code().await?;
        let login_url = if auth.verification_uri_complete.starts_with("http://")
            || auth.verification_uri_complete.starts_with("https://")
        {
            auth.verification_uri_complete
        } else {
            format!("https://{}", auth.verification_uri_complete)
        };

        println!("Open this URL to log into TIDAL:");
        println!("{login_url}");

        let _ = webbrowser::open(&login_url);

        let start = now_secs();
        loop {
            if now_secs() - start > 600.0 {
                bail!("timed out waiting for TIDAL login");
            }

            match self.poll_auth_status(&auth.device_code).await? {
                Some(token) => {
                    self.apply_token_response(token);
                    return Ok(());
                }
                None => sleep(Duration::from_secs(4)).await,
            }
        }
    }

    pub async fn get_track(&self, id: &str) -> Result<Track> {
        let value = self.api_get(&format!("tracks/{id}"), &[]).await?;
        Ok(track_from_value(&value))
    }

    pub async fn get_album(&self, id: &str) -> Result<Album> {
        let mut album = self.api_get(&format!("albums/{id}"), &[]).await?;
        let items = self.get_all_items("albums", id, item_count(&album)).await?;
        album["tracks"] = Value::Array(items);
        album_from_value(&album)
    }

    pub async fn get_playlist(&self, id: &str) -> Result<Playlist> {
        let mut playlist = self.api_get(&format!("playlists/{id}"), &[]).await?;
        let items = self
            .get_all_items("playlists", id, item_count(&playlist))
            .await?;
        playlist["tracks"] = Value::Array(items);
        playlist_from_value(&playlist)
    }

    pub async fn get_download_info(&self, track_id: &str) -> Result<DownloadInfo> {
        match self
            .request_download_info(track_id, PlaybackQuality::HiResLossless)
            .await
        {
            Ok(info) => Ok(info),
            Err(hi_res_error) if lossless_fallback_allowed(&hi_res_error) => {
                self.request_download_info(track_id, PlaybackQuality::Lossless)
                    .await
                    .with_context(|| {
                        format!(
                            "failed to get HI_RES_LOSSLESS playback info; fallback LOSSLESS also failed; HI_RES_LOSSLESS error: {hi_res_error:#}"
                        )
                    })
            }
            Err(hi_res_error) => {
                Err(hi_res_error).context("failed to get HI_RES_LOSSLESS playback info")
            }
        }
    }

    async fn request_download_info(
        &self,
        track_id: &str,
        quality: PlaybackQuality,
    ) -> Result<DownloadInfo> {
        let resp = self
            .api_get(
                &format!("tracks/{track_id}/playbackinfopostpaywall"),
                &[
                    ("audioquality", quality.as_tidal_param().to_string()),
                    ("playbackmode", "STREAM".to_string()),
                    ("assetpresentation", "FULL".to_string()),
                ],
            )
            .await?;

        let Some(manifest) = resp.get("manifest").and_then(Value::as_str) else {
            let message = resp
                .get("userMessage")
                .and_then(Value::as_str)
                .unwrap_or("TIDAL did not return a playback manifest");
            return Err(playback_fallback_error(
                quality,
                PlaybackFallbackReason::MissingManifest,
                message.to_string(),
            ));
        };

        parse_manifest(manifest, quality)
    }

    async fn get_all_items(
        &self,
        media_type: &str,
        id: &str,
        expected: usize,
    ) -> Result<Vec<Value>> {
        let mut out = Vec::new();
        let mut offset = 0usize;

        loop {
            let resp = self
                .api_get(
                    &format!("{media_type}/{id}/items"),
                    &[("offset", offset.to_string())],
                )
                .await?;

            let page = resp
                .get("items")
                .and_then(Value::as_array)
                .ok_or_else(|| anyhow!("TIDAL {media_type}/{id}/items response has no items"))?;

            for entry in page {
                if let Some(item) = entry.get("item") {
                    out.push(item.clone());
                }
            }

            if page.len() < 100 || out.len() >= expected {
                break;
            }
            offset += 100;
        }

        Ok(out)
    }

    async fn login_by_access_token(&mut self) -> Result<()> {
        let resp = self
            .client
            .get(SESSION_URL)
            .bearer_auth(&self.config.access_token)
            .timeout(HTTP_SHORT_REQUEST_TIMEOUT);
        let resp = send_text_with_retries(resp)
            .await
            .context("failed to verify TIDAL access token")?;

        if !resp.status.is_success() {
            bail!("TIDAL login failed: {}", resp.status);
        }

        let body: Value =
            serde_json::from_str(&resp.body).context("invalid TIDAL session response")?;
        let user_id = body
            .get("userId")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("TIDAL session response missing userId"))?;
        if !self.config.user_id.is_empty() && self.config.user_id != user_id.to_string() {
            bail!(
                "TIDAL user id mismatch: {} vs {}",
                user_id,
                self.config.user_id
            );
        }

        self.config.user_id = user_id.to_string();
        if let Some(country_code) = body.get("countryCode").and_then(Value::as_str) {
            self.config.country_code = country_code.to_string();
        }

        Ok(())
    }

    async fn get_device_code(&self) -> Result<DeviceAuthorization> {
        let resp = self
            .client
            .post(format!("{AUTH_URL}/device_authorization"))
            .form(&[("client_id", self.client_id.as_str()), ("scope", SCOPE)])
            .timeout(HTTP_SHORT_REQUEST_TIMEOUT);
        let resp = send_text_with_retries(resp)
            .await
            .context("failed to request TIDAL device authorization")?;

        let status = resp.status;
        let body = resp.body;
        if !status.is_success() {
            bail!("TIDAL device authorization failed: {status} {body}");
        }

        serde_json::from_str(&body).context("invalid TIDAL device authorization response")
    }

    async fn poll_auth_status(&self, device_code: &str) -> Result<Option<TokenResponse>> {
        let resp = self
            .client
            .post(format!("{AUTH_URL}/token"))
            .basic_auth(&self.client_id, Some(&self.client_secret))
            .form(&[
                ("client_id", self.client_id.as_str()),
                ("device_code", device_code),
                ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                ("scope", SCOPE),
            ])
            .timeout(HTTP_SHORT_REQUEST_TIMEOUT);
        let resp = send_json_with_retries::<Value>(resp)
            .await
            .context("failed to poll TIDAL auth status")?;

        let status = resp.status;
        let body = resp.body;
        if status.is_success() {
            return serde_json::from_value(body)
                .map(Some)
                .context("invalid TIDAL token response");
        }

        if status == StatusCode::BAD_REQUEST
            && body.get("sub_status").and_then(Value::as_i64) == Some(1002)
        {
            return Ok(None);
        }

        bail!("TIDAL auth failed: {body}");
    }

    async fn refresh_access_token(&mut self) -> Result<()> {
        if self.config.refresh_token.is_empty() {
            bail!("refresh token missing; run `tidaload login` again");
        }

        let resp = self
            .client
            .post(format!("{AUTH_URL}/token"))
            .basic_auth(&self.client_id, Some(&self.client_secret))
            .form(&[
                ("client_id", self.client_id.as_str()),
                ("refresh_token", self.config.refresh_token.as_str()),
                ("grant_type", "refresh_token"),
                ("scope", SCOPE),
            ])
            .timeout(HTTP_SHORT_REQUEST_TIMEOUT);
        let resp = send_json_with_retries::<Value>(resp)
            .await
            .context("failed to refresh TIDAL access token")?;

        let status = resp.status;
        let body = resp.body;
        if !status.is_success() {
            bail!("TIDAL token refresh failed: {body}");
        }

        let access_token = body
            .get("access_token")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("TIDAL refresh response missing access_token"))?;
        let expires_in = body
            .get("expires_in")
            .and_then(Value::as_f64)
            .ok_or_else(|| anyhow!("TIDAL refresh response missing expires_in"))?;

        self.config.access_token = access_token.to_string();
        self.config.token_expiry = now_secs() + expires_in;
        Ok(())
    }

    async fn api_get(&self, path: &str, params: &[(&str, String)]) -> Result<Value> {
        if self.config.country_code.is_empty() {
            bail!("TIDAL country_code missing; run `tidaload login` again");
        }

        let url = format!("{BASE}/{path}");
        let mut query = vec![
            ("countryCode", self.config.country_code.clone()),
            ("limit", "100".to_string()),
        ];
        query.extend(params.iter().map(|(k, v)| (*k, v.clone())));

        let resp = self
            .client
            .get(&url)
            .bearer_auth(&self.config.access_token)
            .query(&query)
            .timeout(HTTP_SHORT_REQUEST_TIMEOUT);
        let resp = send_text_with_retries(resp)
            .await
            .with_context(|| format!("failed to request TIDAL API {path}"))?;

        let status = resp.status;
        let body = resp.body;
        if !status.is_success() {
            bail!("TIDAL API request failed for {path}: {status} {body}");
        }

        serde_json::from_str(&body).with_context(|| format!("invalid TIDAL API JSON for {path}"))
    }

    fn apply_token_response(&mut self, token: TokenResponse) {
        self.config.user_id = token.user.user_id.to_string();
        self.config.country_code = token.user.country_code;
        self.config.access_token = token.access_token;
        if let Some(refresh_token) = token.refresh_token {
            self.config.refresh_token = refresh_token;
        }
        self.config.token_expiry = now_secs() + token.expires_in;
    }
}

fn tidal_client_builder() -> Result<reqwest::ClientBuilder> {
    let doh_resolver = DohResolver::new()?;

    Ok(configure_http_client(reqwest::Client::builder())
        .user_agent(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:83.0) Gecko/20100101 Firefox/83.0",
        )
        .dns_resolver(Arc::new(doh_resolver)))
}

pub fn parse_resource(input: &str, fallback_kind: Option<ResourceKind>) -> Result<ParsedResource> {
    if let Ok(url) = url::Url::parse(input) {
        if let Some(host) = url.host_str()
            && !host.ends_with("tidal.com")
        {
            bail!("not a TIDAL URL: {input}");
        }

        let parts: Vec<&str> = url.path_segments().map(|s| s.collect()).unwrap_or_default();
        for (idx, part) in parts.iter().enumerate() {
            let kind = match *part {
                "track" => Some(ResourceKind::Track),
                "album" => Some(ResourceKind::Album),
                "playlist" => Some(ResourceKind::Playlist),
                _ => None,
            };

            if let Some(kind) = kind {
                let id = parts
                    .get(idx + 1)
                    .ok_or_else(|| anyhow!("missing TIDAL id in URL: {input}"))?;
                return Ok(ParsedResource {
                    kind,
                    id: id.to_string(),
                });
            }
        }

        bail!("could not find track, album, or playlist id in URL: {input}");
    }

    let kind = fallback_kind.ok_or_else(|| {
        anyhow!("raw ids need --kind track, --kind album, or --kind playlist: {input}")
    })?;
    Ok(ParsedResource {
        kind,
        id: input.to_string(),
    })
}

fn parse_manifest(manifest: &str, quality: PlaybackQuality) -> Result<DownloadInfo> {
    let decoded = STANDARD
        .decode(manifest)
        .context("failed to base64-decode TIDAL manifest")?;
    let decoded = std::str::from_utf8(&decoded).context("TIDAL manifest is not valid UTF-8")?;
    if decoded.trim_start().starts_with('<') {
        return parse_dash_manifest(decoded, quality);
    }

    let value: Value = serde_json::from_str(decoded).context("failed to parse TIDAL manifest")?;

    let url = value
        .get("urls")
        .and_then(Value::as_array)
        .and_then(|urls| urls.first())
        .and_then(Value::as_str)
        .ok_or_else(|| restriction_error(&value))?;

    let codec = value
        .get("codecs")
        .and_then(Value::as_str)
        .unwrap_or("m4a")
        .to_ascii_lowercase();
    if codec == "mqa" {
        return Err(playback_fallback_error(
            quality,
            PlaybackFallbackReason::MqaSource,
            format!(
                "TIDAL returned MQA for {}; refusing MQA source",
                quality.as_tidal_param()
            ),
        ));
    }
    let native_flac = codec == "flac";

    let encryption_key = if value
        .get("encryptionType")
        .and_then(Value::as_str)
        .unwrap_or("NONE")
        == "NONE"
    {
        None
    } else {
        value
            .get("keyId")
            .and_then(Value::as_str)
            .map(ToString::to_string)
    };

    Ok(DownloadInfo::Direct {
        url: url.to_string(),
        extension: "flac".to_string(),
        encryption_key,
        native_flac,
        quality,
    })
}

fn parse_dash_manifest(manifest: &str, quality: PlaybackQuality) -> Result<DownloadInfo> {
    let document = Document::parse(manifest).context("failed to parse TIDAL DASH manifest XML")?;
    let representation = flac_representation(&document, quality)?;
    let template = segment_template_for(representation)
        .ok_or_else(|| anyhow!("TIDAL DASH manifest missing SegmentTemplate"))?;
    let initialization_url = template
        .attribute("initialization")
        .ok_or_else(|| anyhow!("TIDAL DASH manifest missing initialization URL"))?;
    let media_url_template = template
        .attribute("media")
        .ok_or_else(|| anyhow!("TIDAL DASH manifest missing media URL template"))?;
    let start_number = template
        .attribute("startNumber")
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(1);
    let segment_count = count_dash_segments(template)?;

    Ok(DownloadInfo::Segmented {
        initialization_url: initialization_url.to_string(),
        media_url_template: media_url_template.to_string(),
        start_number,
        segment_count,
        extension: "flac".to_string(),
        quality,
    })
}

fn flac_representation<'a, 'input>(
    document: &'a Document<'input>,
    quality: PlaybackQuality,
) -> Result<Node<'a, 'input>> {
    let mut saw_representation = false;
    let mut unsupported_codecs = Vec::new();

    for representation in document
        .descendants()
        .filter(|node| xml_tag_name(*node, "Representation"))
    {
        saw_representation = true;
        let codecs = representation
            .attribute("codecs")
            .unwrap_or_default()
            .to_ascii_lowercase();
        if codecs == "flac" {
            return Ok(representation);
        }
        unsupported_codecs.push(codecs);
    }

    if !saw_representation {
        bail!("TIDAL DASH manifest missing Representation");
    }

    let codecs = unsupported_codecs
        .into_iter()
        .filter(|codecs| !codecs.is_empty())
        .collect::<Vec<_>>()
        .join(", ");
    Err(playback_fallback_error(
        quality,
        PlaybackFallbackReason::UnsupportedDashCodec,
        format!("unsupported TIDAL DASH codec: {codecs}"),
    ))
}

fn segment_template_for<'a, 'input>(representation: Node<'a, 'input>) -> Option<Node<'a, 'input>> {
    representation
        .descendants()
        .find(|node| xml_tag_name(*node, "SegmentTemplate"))
        .or_else(|| {
            representation.ancestors().find_map(|ancestor| {
                ancestor
                    .children()
                    .find(|node| xml_tag_name(*node, "SegmentTemplate"))
            })
        })
}

fn count_dash_segments(template: Node<'_, '_>) -> Result<u32> {
    let mut count = 0u32;

    for segment in template
        .descendants()
        .filter(|node| xml_tag_name(*node, "S"))
    {
        let repeat = segment
            .attribute("r")
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0);
        if repeat < 0 {
            bail!("unsupported open-ended TIDAL DASH segment repeat");
        }
        count = count
            .checked_add(repeat as u32 + 1)
            .ok_or_else(|| anyhow!("TIDAL DASH manifest has too many segments"))?;
    }

    if count == 0 {
        bail!("TIDAL DASH manifest has no media segments");
    }

    Ok(count)
}

fn xml_tag_name(node: Node<'_, '_>, name: &str) -> bool {
    node.is_element() && node.tag_name().name() == name
}

fn restriction_error(value: &Value) -> anyhow::Error {
    let code = value
        .get("restrictions")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .and_then(|item| item.get("code"))
        .and_then(Value::as_str)
        .unwrap_or("TIDAL stream is restricted");
    anyhow!("{code}")
}

fn album_from_value(value: &Value) -> Result<Album> {
    let title = string_field(value, "title", "Unknown Album");
    let artist = artists(value)
        .or_else(|| nested_string(value, &["artist", "name"]))
        .unwrap_or_else(|| "Unknown Artist".to_string());
    let year = string_opt(value, "releaseDate").and_then(|date| year_from_date(&date));
    let album_cover_uuid = string_opt(value, "cover").filter(|cover| !cover.is_empty());
    let tracks = value
        .get("tracks")
        .and_then(Value::as_array)
        .unwrap_or(&Vec::new())
        .iter()
        .map(|value| {
            let mut track = track_from_value(value);
            if track.cover_uuid.is_none() {
                track.cover_uuid.clone_from(&album_cover_uuid);
            }
            track
        })
        .collect();

    Ok(Album {
        id: id_string(value),
        title,
        artist,
        year,
        disc_total: value
            .get("numberOfVolumes")
            .and_then(Value::as_u64)
            .unwrap_or(1),
        tracks,
    })
}

fn playlist_from_value(value: &Value) -> Result<Playlist> {
    let title = string_field(value, "title", "Unknown Playlist");
    let tracks = value
        .get("tracks")
        .and_then(Value::as_array)
        .unwrap_or(&Vec::new())
        .iter()
        .map(track_from_value)
        .collect();

    Ok(Playlist {
        id: id_string(value),
        title,
        tracks,
    })
}

fn track_from_value(value: &Value) -> Track {
    let mut title = string_field(value, "title", "Unknown Track");
    if let Some(version) = string_opt(value, "version").filter(|v| !v.is_empty()) {
        title = format!("{title} ({version})");
    }

    let artist = artists(value)
        .or_else(|| nested_string(value, &["artist", "name"]))
        .unwrap_or_else(|| "Unknown Artist".to_string());

    Track {
        id: id_string(value),
        title,
        artist,
        album_title: track_album_title(value),
        album_artist: track_album_artist(value),
        album_year: track_album_year(value),
        album_track_total: nested_u64(value, &["album", "numberOfTracks"]),
        album_disc_total: nested_u64(value, &["album", "numberOfVolumes"]),
        track_number: value.get("trackNumber").and_then(Value::as_u64),
        volume_number: value.get("volumeNumber").and_then(Value::as_u64),
        cover_uuid: track_cover_uuid(value),
        allow_streaming: value
            .get("allowStreaming")
            .and_then(Value::as_bool)
            .unwrap_or(true),
    }
}

pub fn cover_image_url(cover_uuid: &str) -> String {
    format!(
        "https://resources.tidal.com/images/{}/1280x1280.jpg",
        cover_uuid.replace('-', "/")
    )
}

fn item_count(value: &Value) -> usize {
    value
        .get("numberOfTracks")
        .and_then(Value::as_u64)
        .unwrap_or(100) as usize
}

fn id_string(value: &Value) -> String {
    value
        .get("id")
        .and_then(|id| {
            id.as_str()
                .map(ToString::to_string)
                .or_else(|| id.as_u64().map(|id| id.to_string()))
        })
        .or_else(|| {
            value
                .get("uuid")
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
        .unwrap_or_else(|| "unknown".to_string())
}

fn artists(value: &Value) -> Option<String> {
    let artists = value.get("artists")?.as_array()?;
    let names: Vec<&str> = artists
        .iter()
        .filter_map(|artist| artist.get("name").and_then(Value::as_str))
        .collect();
    (!names.is_empty()).then(|| names.join(", "))
}

fn track_album_title(value: &Value) -> Option<String> {
    nested_string(value, &["album", "title"]).filter(|title| !title.is_empty())
}

fn track_album_artist(value: &Value) -> Option<String> {
    nested_value(value, &["album"]).and_then(|album| {
        artists(album)
            .or_else(|| nested_string(album, &["artist", "name"]))
            .filter(|artist| !artist.is_empty())
    })
}

fn track_album_year(value: &Value) -> Option<String> {
    nested_string(value, &["album", "releaseDate"]).and_then(|date| year_from_date(&date))
}

fn year_from_date(date: &str) -> Option<String> {
    let year: String = date.chars().take(4).collect();
    (!year.is_empty()).then_some(year)
}

fn string_field(value: &Value, field: &str, fallback: &str) -> String {
    string_opt(value, field).unwrap_or_else(|| fallback.to_string())
}

fn string_opt(value: &Value, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn nested_string(value: &Value, keys: &[&str]) -> Option<String> {
    nested_value(value, keys)?.as_str().map(ToString::to_string)
}

fn nested_u64(value: &Value, keys: &[&str]) -> Option<u64> {
    nested_value(value, keys)?.as_u64()
}

fn nested_value<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a Value> {
    let mut current = value;
    for key in keys {
        current = current.get(*key)?;
    }
    Some(current)
}

fn track_cover_uuid(value: &Value) -> Option<String> {
    nested_string(value, &["album", "cover"])
        .or_else(|| string_opt(value, "cover"))
        .filter(|cover| !cover.is_empty())
}

fn decode_constant(value: &str) -> Result<String> {
    let bytes = STANDARD.decode(value).context("invalid base64 constant")?;
    String::from_utf8(bytes).context("invalid UTF-8 constant")
}

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_tidal_track_url_with_slug() {
        let parsed = parse_resource("https://tidal.com/track/528146825/u", None).unwrap();
        assert!(matches!(parsed.kind, ResourceKind::Track));
        assert_eq!(parsed.id, "528146825");
    }

    #[test]
    fn parses_tidal_browse_album_url() {
        let parsed = parse_resource("https://tidal.com/browse/album/147569387", None).unwrap();
        assert!(matches!(parsed.kind, ResourceKind::Album));
        assert_eq!(parsed.id, "147569387");
    }

    #[test]
    fn parses_tidal_album_url_with_slug() {
        let parsed = parse_resource("https://tidal.com/album/524851236/u", None).unwrap();
        assert!(matches!(parsed.kind, ResourceKind::Album));
        assert_eq!(parsed.id, "524851236");
    }

    #[test]
    fn parses_copied_tidal_album_url_with_user_suffix() {
        let parsed = parse_resource("https://tidal.com/album/496439179/u", None).unwrap();
        assert!(matches!(parsed.kind, ResourceKind::Album));
        assert_eq!(parsed.id, "496439179");
    }

    #[test]
    fn parses_tidal_playlist_url_with_uuid() {
        let parsed = parse_resource(
            "https://tidal.com/playlist/36ea71a8-445e-41a4-82ab-6628c581535d",
            None,
        )
        .unwrap();
        assert!(matches!(parsed.kind, ResourceKind::Playlist));
        assert_eq!(parsed.id, "36ea71a8-445e-41a4-82ab-6628c581535d");
    }

    #[test]
    fn builds_track_metadata_for_album_downloads() {
        let track = track_from_value(&serde_json::json!({
            "id": 42,
            "title": "Example",
            "artist": {"name": "Artist"},
            "trackNumber": 7,
            "volumeNumber": 2,
            "album": {
                "title": "Album",
                "artist": {"name": "Album Artist"},
                "releaseDate": "2024-03-01",
                "numberOfTracks": 12,
                "numberOfVolumes": 2,
                "cover": "abcd-efgh"
            },
            "allowStreaming": true
        }));

        assert_eq!(track.id, "42");
        assert_eq!(track.album_title.as_deref(), Some("Album"));
        assert_eq!(track.album_artist.as_deref(), Some("Album Artist"));
        assert_eq!(track.album_year.as_deref(), Some("2024"));
        assert_eq!(track.album_track_total, Some(12));
        assert_eq!(track.album_disc_total, Some(2));
        assert_eq!(track.track_number, Some(7));
        assert_eq!(track.volume_number, Some(2));
        assert_eq!(track.cover_uuid.as_deref(), Some("abcd-efgh"));
        assert!(track.allow_streaming);
    }

    #[test]
    fn album_tracks_fall_back_to_album_cover() {
        let album = album_from_value(&serde_json::json!({
            "id": 7,
            "title": "Example Album",
            "artist": {"name": "Artist"},
            "cover": "12345678-abcd-efgh-ijkl-987654321000",
            "numberOfVolumes": 1,
            "tracks": [{
                "id": 42,
                "title": "Example",
                "artist": {"name": "Artist"},
                "trackNumber": 1,
                "allowStreaming": true
            }]
        }))
        .unwrap();

        assert_eq!(
            album.tracks[0].cover_uuid.as_deref(),
            Some("12345678-abcd-efgh-ijkl-987654321000")
        );
    }

    #[test]
    fn builds_tidal_cover_image_url() {
        assert_eq!(
            cover_image_url("12345678-abcd-efgh-ijkl-987654321000"),
            "https://resources.tidal.com/images/12345678/abcd/efgh/ijkl/987654321000/1280x1280.jpg"
        );
    }

    #[test]
    fn parses_direct_flac_manifest_as_flac_file() {
        let manifest = serde_json::json!({
            "urls": ["https://audio.example/track.flac"],
            "codecs": "flac",
            "encryptionType": "NONE"
        })
        .to_string();
        let encoded = STANDARD.encode(manifest);

        let info = parse_manifest(&encoded, PlaybackQuality::Lossless).unwrap();
        assert_eq!(info.quality(), PlaybackQuality::Lossless);
        assert_eq!(info.output_path(), OutputPath::DirectNative);
        match info {
            DownloadInfo::Direct {
                url,
                extension,
                encryption_key,
                native_flac,
                ..
            } => {
                assert_eq!(url, "https://audio.example/track.flac");
                assert_eq!(extension, "flac");
                assert!(encryption_key.is_none());
                assert!(native_flac);
            }
            DownloadInfo::Segmented { .. } => panic!("expected direct download info"),
        }
    }

    #[test]
    fn parses_direct_mp4a_manifest_as_flac_output_needing_encode() {
        let manifest = serde_json::json!({
            "urls": ["https://audio.example/track.m4a"],
            "codecs": "mp4a.40.2",
            "encryptionType": "NONE"
        })
        .to_string();
        let encoded = STANDARD.encode(manifest);

        let info = parse_manifest(&encoded, PlaybackQuality::Lossless).unwrap();
        assert_eq!(info.quality(), PlaybackQuality::Lossless);
        assert_eq!(info.output_path(), OutputPath::DirectEncode);
        match info {
            DownloadInfo::Direct {
                url,
                extension,
                native_flac,
                ..
            } => {
                assert_eq!(url, "https://audio.example/track.m4a");
                assert_eq!(extension, "flac");
                assert!(!native_flac);
            }
            DownloadInfo::Segmented { .. } => panic!("expected direct download info"),
        }
    }

    #[test]
    fn rejects_direct_mqa_manifest() {
        let manifest = serde_json::json!({
            "urls": ["https://audio.example/track.flac"],
            "codecs": "mqa",
            "encryptionType": "NONE"
        })
        .to_string();
        let encoded = STANDARD.encode(manifest);

        let err = parse_manifest(&encoded, PlaybackQuality::HiResLossless).unwrap_err();
        assert!(err.to_string().contains("refusing MQA source"));
        assert!(lossless_fallback_allowed(&err));
    }

    #[test]
    fn malformed_manifest_is_not_lossless_fallback_candidate() {
        let err = parse_manifest("not base64", PlaybackQuality::HiResLossless).unwrap_err();
        assert!(!lossless_fallback_allowed(&err));
    }

    #[test]
    fn unsupported_dash_codec_is_lossless_fallback_candidate() {
        let xml = r#"
            <MPD>
              <Period>
                <AdaptationSet>
                  <Representation codecs="mqa">
                    <SegmentTemplate
                      initialization="https://audio.example/init.mp4"
                      media="https://audio.example/$Number$.mp4">
                      <SegmentTimeline>
                        <S d="100"/>
                      </SegmentTimeline>
                    </SegmentTemplate>
                  </Representation>
                </AdaptationSet>
              </Period>
            </MPD>
        "#;
        let encoded = STANDARD.encode(xml);

        let err = parse_manifest(&encoded, PlaybackQuality::HiResLossless).unwrap_err();
        assert!(err.to_string().contains("unsupported TIDAL DASH codec"));
        assert!(lossless_fallback_allowed(&err));
    }

    #[test]
    fn missing_hi_res_manifest_is_lossless_fallback_candidate() {
        let err = playback_fallback_error(
            PlaybackQuality::HiResLossless,
            PlaybackFallbackReason::MissingManifest,
            "quality unavailable".to_string(),
        );

        assert!(lossless_fallback_allowed(&err));
    }

    #[test]
    fn parses_flac_dash_manifest() {
        let xml = r#"
            <MPD>
              <Period>
                <AdaptationSet>
                  <Representation codecs="flac">
                    <SegmentTemplate
                      initialization="https://audio.example/init.mp4?token=a&amp;b=1"
                      media="https://audio.example/$Number$.mp4?token=a&amp;b=1"
                      startNumber="3">
                      <SegmentTimeline>
                        <S d="100" r="2"/>
                        <S d="50"/>
                      </SegmentTimeline>
                    </SegmentTemplate>
                  </Representation>
                </AdaptationSet>
              </Period>
            </MPD>
        "#;
        let encoded = STANDARD.encode(xml);

        let info = parse_manifest(&encoded, PlaybackQuality::HiResLossless).unwrap();
        assert_eq!(info.quality(), PlaybackQuality::HiResLossless);
        assert_eq!(info.output_path(), OutputPath::DashRemux);
        match info {
            DownloadInfo::Segmented {
                initialization_url,
                media_url_template,
                start_number,
                segment_count,
                extension,
                ..
            } => {
                assert_eq!(
                    initialization_url,
                    "https://audio.example/init.mp4?token=a&b=1"
                );
                assert_eq!(
                    media_url_template,
                    "https://audio.example/$Number$.mp4?token=a&b=1"
                );
                assert_eq!(start_number, 3);
                assert_eq!(segment_count, 4);
                assert_eq!(extension, "flac");
            }
            DownloadInfo::Direct { .. } => panic!("expected segmented download info"),
        }
    }

    #[test]
    fn parses_dash_manifest_with_shared_template_and_single_quoted_attributes() {
        let xml = r#"
            <MPD xmlns='urn:mpeg:dash:schema:mpd:2011'>
              <Period>
                <AdaptationSet>
                  <SegmentTemplate
                    initialization='https://audio.example/init.mp4?token=a&amp;b=1'
                    media='https://audio.example/$Number$.mp4?token=a&amp;b=1'
                    startNumber='7'>
                    <SegmentTimeline>
                      <S d='100' r='1'/>
                    </SegmentTimeline>
                  </SegmentTemplate>
                  <Representation id='aac' codecs='mp4a.40.2'/>
                  <Representation id='flac' codecs='flac'/>
                </AdaptationSet>
              </Period>
            </MPD>
        "#;
        let encoded = STANDARD.encode(xml);

        let info = parse_manifest(&encoded, PlaybackQuality::HiResLossless).unwrap();
        assert_eq!(info.quality(), PlaybackQuality::HiResLossless);
        assert_eq!(info.output_path(), OutputPath::DashRemux);
        match info {
            DownloadInfo::Segmented {
                initialization_url,
                media_url_template,
                start_number,
                segment_count,
                extension,
                ..
            } => {
                assert_eq!(
                    initialization_url,
                    "https://audio.example/init.mp4?token=a&b=1"
                );
                assert_eq!(
                    media_url_template,
                    "https://audio.example/$Number$.mp4?token=a&b=1"
                );
                assert_eq!(start_number, 7);
                assert_eq!(segment_count, 2);
                assert_eq!(extension, "flac");
            }
            DownloadInfo::Direct { .. } => panic!("expected segmented download info"),
        }
    }
}
