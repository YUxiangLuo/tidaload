use std::path::Path;

use aes::Aes128;
use anyhow::{Context, Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use cbc::cipher::{BlockDecryptMut, KeyIvInit, StreamCipher, block_padding::NoPadding};
use ctr::Ctr64BE;
use futures_util::StreamExt;
use tokio::fs;
use tokio::io::AsyncWriteExt;

type Aes128CbcDec = cbc::Decryptor<Aes128>;
type Aes128Ctr = Ctr64BE<Aes128>;

pub async fn download_to_file(
    client: &reqwest::Client,
    url: &str,
    path: &Path,
    encryption_key: Option<&str>,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("failed to request {url}"))?
        .error_for_status()
        .with_context(|| format!("download request failed for {url}"))?;

    let tmp_path = path.with_extension("tidaload.part");
    let mut output = fs::File::create(&tmp_path)
        .await
        .with_context(|| format!("failed to create {}", tmp_path.display()))?;
    let mut stream = response.bytes_stream();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("failed while reading download stream")?;
        output
            .write_all(&chunk)
            .await
            .with_context(|| format!("failed to write {}", tmp_path.display()))?;
    }
    output.flush().await?;
    drop(output);

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
