use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};

use crate::config::{Config, DEFAULT_DASH_SEGMENT_CONCURRENCY, music_download_dir};
use crate::http::is_rate_limited_error;
use crate::tidal::{ResourceKind, TidalClient, parse_resource};

const RATE_LIMIT_COOLDOWN: Duration = Duration::from_secs(10 * 60);
const ANSI_BOLD_RED: &str = "\x1b[1;31m";
const ANSI_RESET: &str = "\x1b[0m";

pub async fn run_download(
    config: &mut Config,
    config_path: &Path,
    kind: Option<ResourceKind>,
    dash_segment_concurrency: Option<usize>,
    download_dir: Option<PathBuf>,
    items: Vec<String>,
) -> Result<()> {
    if let Some(remaining) =
        rate_limit_cooldown_remaining(config.downloads.last_429_at, current_time_secs())
    {
        eprintln!(
            "{}",
            red_bold(&format!(
                "HTTP 429 cooldown active. Try again in {}.",
                format_duration(remaining)
            ))
        );
        bail!("TIDAL HTTP 429 cooldown is active");
    }

    if let Some(dash_segment_concurrency) = dash_segment_concurrency {
        config.downloads.dash_segment_concurrency = dash_segment_concurrency.max(1);
    } else if config.downloads.dash_segment_concurrency == 0 {
        config.downloads.dash_segment_concurrency = DEFAULT_DASH_SEGMENT_CONCURRENCY;
    }

    let mut client = TidalClient::new(config.tidal.clone())?;
    let changed = client.ensure_login().await?;
    if changed {
        config.tidal = client.config.clone();
        config.save(config_path)?;
    }

    let resources = items
        .iter()
        .map(|item| parse_resource(item, kind))
        .collect::<Result<Vec<_>>>()?;
    let download_root = music_download_dir(
        download_dir
            .as_deref()
            .or(config.downloads.download_dir.as_deref()),
    )?;

    for resource in resources {
        match crate::download_resource(&client, config, resource, &download_root).await {
            Ok(()) => {}
            Err(err) if is_rate_limited_error(&err) => {
                record_rate_limit_and_warn(config, config_path)?;
                return Err(err).context("stopped after TIDAL returned HTTP 429");
            }
            Err(err) => return Err(err),
        }
    }

    Ok(())
}

fn record_rate_limit_and_warn(config: &mut Config, config_path: &Path) -> Result<()> {
    config.downloads.last_429_at = current_time_secs();
    config.save(config_path).with_context(|| {
        format!(
            "failed to save HTTP 429 timestamp to {}",
            config_path.display()
        )
    })?;
    eprintln!(
        "{}",
        red_bold(&format!(
            "HTTP 429 rate limit detected. Saved cooldown timestamp to {}. Temporary files were cleaned; exiting. Try again after 10 minutes.",
            config_path.display()
        ))
    );
    Ok(())
}

fn rate_limit_cooldown_remaining(last_429_at: f64, now: f64) -> Option<Duration> {
    if !last_429_at.is_finite() || last_429_at <= 0.0 {
        return None;
    }

    let elapsed = now - last_429_at;
    if elapsed >= RATE_LIMIT_COOLDOWN.as_secs_f64() {
        return None;
    }

    let remaining = if elapsed.is_sign_negative() {
        RATE_LIMIT_COOLDOWN
    } else {
        Duration::from_secs_f64(RATE_LIMIT_COOLDOWN.as_secs_f64() - elapsed)
    };
    Some(remaining)
}

fn format_duration(duration: Duration) -> String {
    let mut seconds = duration.as_secs();
    if duration.subsec_nanos() > 0 {
        seconds += 1;
    }

    let minutes = seconds / 60;
    let seconds = seconds % 60;
    if minutes == 0 {
        format!("{seconds}s")
    } else {
        format!("{minutes}m {seconds}s")
    }
}

fn current_time_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs_f64())
        .unwrap_or_default()
}

fn red_bold(value: &str) -> String {
    format!("{ANSI_BOLD_RED}{value}{ANSI_RESET}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limit_cooldown_blocks_recent_429() {
        assert_eq!(
            rate_limit_cooldown_remaining(1_000.0, 1_060.0),
            Some(Duration::from_secs(540))
        );
    }

    #[test]
    fn rate_limit_cooldown_expires_after_ten_minutes() {
        assert_eq!(rate_limit_cooldown_remaining(1_000.0, 1_600.0), None);
    }

    #[test]
    fn rate_limit_cooldown_handles_future_clock_skew() {
        assert_eq!(
            rate_limit_cooldown_remaining(1_000.0, 900.0),
            Some(RATE_LIMIT_COOLDOWN)
        );
    }

    #[test]
    fn formats_cooldown_duration() {
        assert_eq!(format_duration(Duration::from_secs(9)), "9s");
        assert_eq!(format_duration(Duration::from_secs(601)), "10m 1s");
    }

    #[test]
    fn wraps_text_in_bold_red() {
        assert_eq!(red_bold("missing"), "\x1b[1;31mmissing\x1b[0m");
    }
}
