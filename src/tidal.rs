use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::Value;
use tokio::time::{Duration, sleep};

use crate::config::TidalConfig;

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
    pub track_number: Option<u64>,
    pub allow_streaming: bool,
}

#[derive(Debug, Clone)]
pub struct DownloadInfo {
    pub url: String,
    pub extension: String,
    pub encryption_key: Option<String>,
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
        let client = tidal_client_builder()
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

    pub async fn get_download_info(&self, track_id: &str, quality: u8) -> Result<DownloadInfo> {
        let mut target_quality = quality.min(3);

        loop {
            let audio_quality = match target_quality {
                0 => "LOW",
                1 => "HIGH",
                2 => "LOSSLESS",
                _ => "HI_RES",
            };

            let resp = self
                .api_get(
                    &format!("tracks/{track_id}/playbackinfopostpaywall"),
                    &[
                        ("audioquality", audio_quality.to_string()),
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
                bail!("{message}");
            };

            match parse_manifest(manifest) {
                Ok(info) => return Ok(info),
                Err(err) if target_quality > 0 => {
                    eprintln!(
                        "failed to parse manifest at quality {target_quality}: {err}; retrying lower quality"
                    );
                    target_quality -= 1;
                }
                Err(err) => return Err(err),
            }
        }
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
            .send()
            .await
            .context("failed to verify TIDAL access token")?;

        if !resp.status().is_success() {
            bail!("TIDAL login failed: {}", resp.status());
        }

        let body: Value = resp
            .json()
            .await
            .context("invalid TIDAL session response")?;
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
            .send()
            .await
            .context("failed to request TIDAL device authorization")?;

        let status = resp.status();
        let body = resp
            .text()
            .await
            .context("failed to read TIDAL auth response")?;
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
            .send()
            .await
            .context("failed to poll TIDAL auth status")?;

        let status = resp.status();
        let body: Value = resp
            .json()
            .await
            .context("invalid TIDAL auth poll response")?;
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
            .send()
            .await
            .context("failed to refresh TIDAL access token")?;

        let status = resp.status();
        let body: Value = resp
            .json()
            .await
            .context("invalid TIDAL refresh response")?;
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
            .send()
            .await
            .with_context(|| format!("failed to request TIDAL API {path}"))?;

        let status = resp.status();
        let body = resp
            .text()
            .await
            .context("failed to read TIDAL API response")?;
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

fn tidal_client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .user_agent(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:83.0) Gecko/20100101 Firefox/83.0",
        )
        // In this workspace, the native resolver intermittently returns no addresses
        // for TIDAL CloudFront hosts even when direct HTTPS access works. These mappings
        // preserve the original hostname for TLS/SNI and only bypass DNS.
        .resolve(
            "auth.tidal.com",
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(3, 169, 173, 64)), 443),
        )
        .resolve(
            "api.tidalhifi.com",
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(3, 169, 173, 102)), 443),
        )
        .resolve(
            "api.tidal.com",
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(3, 169, 173, 10)), 443),
        )
}

pub fn parse_resource(input: &str, fallback_kind: Option<ResourceKind>) -> Result<ParsedResource> {
    if let Ok(url) = url::Url::parse(input) {
        if let Some(host) = url.host_str() {
            if !host.ends_with("tidal.com") {
                bail!("not a TIDAL URL: {input}");
            }
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

fn parse_manifest(manifest: &str) -> Result<DownloadInfo> {
    let decoded = STANDARD
        .decode(manifest)
        .context("failed to base64-decode TIDAL manifest")?;
    let value: Value =
        serde_json::from_slice(&decoded).context("failed to parse TIDAL manifest")?;

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
    let extension = if codec == "flac" || codec == "mqa" {
        "flac"
    } else {
        "m4a"
    };

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

    Ok(DownloadInfo {
        url: url.to_string(),
        extension: extension.to_string(),
        encryption_key,
    })
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
    let year = string_opt(value, "releaseDate").map(|date| date.chars().take(4).collect());
    let tracks = value
        .get("tracks")
        .and_then(Value::as_array)
        .unwrap_or(&Vec::new())
        .iter()
        .map(track_from_value)
        .collect();

    Ok(Album {
        id: id_string(value),
        title,
        artist,
        year,
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
        track_number: value.get("trackNumber").and_then(Value::as_u64),
        allow_streaming: value
            .get("allowStreaming")
            .and_then(Value::as_bool)
            .unwrap_or(true),
    }
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
    let mut current = value;
    for key in keys {
        current = current.get(*key)?;
    }
    current.as_str().map(ToString::to_string)
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
}
