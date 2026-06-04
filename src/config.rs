use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

pub const DEFAULT_DOWNLOAD_CONCURRENCY: usize = 2;
pub const DEFAULT_DASH_SEGMENT_CONCURRENCY: usize = 8;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
    pub dash_segment_concurrency: usize,
    pub download_dir: Option<PathBuf>,
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
        Self {
            dash_segment_concurrency: DEFAULT_DASH_SEGMENT_CONCURRENCY,
            download_dir: None,
        }
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
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(path)
            .with_context(|| format!("failed to open {}", path.display()))?;

        file.set_permissions(fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to set permissions on {}", path.display()))?;
        file.set_len(0)
            .with_context(|| format!("failed to truncate {}", path.display()))?;
        file.write_all(contents.as_bytes())
            .with_context(|| format!("failed to write {}", path.display()))?;
        file.sync_all()
            .with_context(|| format!("failed to sync {}", path.display()))
    }
}

pub fn default_config_path() -> PathBuf {
    dirs::config_dir()
        .or_else(dirs::home_dir)
        .unwrap_or_else(|| PathBuf::from("."))
        .join("tidaload")
        .join("config.toml")
}

pub fn music_download_dir(download_dir: Option<&Path>) -> Result<PathBuf> {
    if let Some(path) = download_dir {
        return expand_user_path(path);
    }

    if let Some(path) = dirs::audio_dir() {
        return Ok(path);
    }

    dirs::home_dir()
        .map(|home| home.join("Music"))
        .context("failed to resolve the user's Music directory")
}

fn expand_user_path(path: &Path) -> Result<PathBuf> {
    let Some(path_str) = path.to_str() else {
        return Ok(path.to_path_buf());
    };

    if path_str == "~" {
        return dirs::home_dir().context("failed to resolve the user's home directory");
    }

    if let Some(rest) = path_str.strip_prefix("~/") {
        return dirs::home_dir()
            .map(|home| home.join(rest))
            .context("failed to resolve the user's home directory");
    }

    Ok(path.to_path_buf())
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn save_sets_owner_only_permissions() -> Result<()> {
        let unique = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "tidaload-config-test-{}-{unique}",
            std::process::id()
        ));
        let path = dir.join("config.toml");
        fs::create_dir_all(&dir)?;
        fs::write(&path, "old config")?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644))?;

        Config::default().save(&path)?;

        let mode = fs::metadata(&path)?.permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);

        fs::remove_dir_all(dir)?;
        Ok(())
    }

    #[test]
    fn uses_configured_download_dir() -> Result<()> {
        let path = Path::new("/tmp/tidaload-downloads");

        assert_eq!(music_download_dir(Some(path))?, path);
        Ok(())
    }

    #[test]
    fn expands_tilde_download_dir() -> Result<()> {
        let Some(home) = dirs::home_dir() else {
            return Ok(());
        };

        assert_eq!(
            music_download_dir(Some(Path::new("~/TIDAL")))?,
            home.join("TIDAL")
        );
        Ok(())
    }

    #[test]
    fn ignores_legacy_download_concurrency_config() -> Result<()> {
        let config: Config = toml::from_str(
            r#"
            [downloads]
            concurrency = 99
            dash_segment_concurrency = 4
            "#,
        )?;

        assert_eq!(config.downloads.dash_segment_concurrency, 4);
        Ok(())
    }
}
