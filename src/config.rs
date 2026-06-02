use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub tidal: TidalConfig,
    pub downloads: DownloadsConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TidalConfig {
    pub user_id: String,
    pub country_code: String,
    pub access_token: String,
    pub refresh_token: String,
    pub token_expiry: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DownloadsConfig {
    pub concurrency: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            tidal: TidalConfig::default(),
            downloads: DownloadsConfig::default(),
        }
    }
}

impl Default for TidalConfig {
    fn default() -> Self {
        Self {
            user_id: String::new(),
            country_code: String::new(),
            access_token: String::new(),
            refresh_token: String::new(),
            token_expiry: 0.0,
        }
    }
}

impl Default for DownloadsConfig {
    fn default() -> Self {
        Self { concurrency: 4 }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            let config = Self::default();
            config.save(path)?;
            return Ok(config);
        }

        let contents = fs::read_to_string(path)
            .with_context(|| format!("failed to read config {}", path.display()))?;
        toml::from_str(&contents).with_context(|| format!("failed to parse {}", path.display()))
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        let contents = toml::to_string_pretty(self).context("failed to serialize config")?;
        fs::write(path, contents).with_context(|| format!("failed to write {}", path.display()))
    }
}

pub fn default_config_path() -> PathBuf {
    dirs::config_dir()
        .or_else(dirs::home_dir)
        .unwrap_or_else(|| PathBuf::from("."))
        .join("tidaload")
        .join("config.toml")
}

pub fn music_download_dir() -> Result<PathBuf> {
    if let Some(path) = dirs::audio_dir() {
        return Ok(path);
    }

    dirs::home_dir()
        .map(|home| home.join("Music"))
        .context("failed to resolve the user's Music directory")
}
