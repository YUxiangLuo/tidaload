use std::future::Future;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use reqwest::{Response, StatusCode};
use serde::de::DeserializeOwned;
use tokio::time::sleep;

pub const HTTP_MAX_ATTEMPTS: usize = 3;
pub const HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub const HTTP_READ_TIMEOUT: Duration = Duration::from_secs(60);
pub const HTTP_SHORT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

const RETRY_BASE_DELAY: Duration = Duration::from_millis(500);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(4);

pub fn configure_http_client(builder: reqwest::ClientBuilder) -> reqwest::ClientBuilder {
    builder
        .connect_timeout(HTTP_CONNECT_TIMEOUT)
        .read_timeout(HTTP_READ_TIMEOUT)
}

#[derive(Debug)]
pub struct ResponseBody<T> {
    pub status: StatusCode,
    pub body: T,
}

pub async fn send_text_with_retries(
    request: reqwest::RequestBuilder,
) -> Result<ResponseBody<String>> {
    send_body_with_retries(request, |response| response.text(), "response body").await
}

pub async fn send_bytes_with_retries(
    request: reqwest::RequestBuilder,
) -> Result<ResponseBody<Vec<u8>>> {
    let response =
        send_body_with_retries(request, |response| response.bytes(), "response body").await?;
    Ok(ResponseBody {
        status: response.status,
        body: response.body.to_vec(),
    })
}

pub async fn send_json_with_retries<T>(request: reqwest::RequestBuilder) -> Result<ResponseBody<T>>
where
    T: DeserializeOwned,
{
    let response = send_text_with_retries(request).await?;
    let body = serde_json::from_str(&response.body).context("failed to parse JSON response")?;
    Ok(ResponseBody {
        status: response.status,
        body,
    })
}

async fn send_body_with_retries<T, F, Fut>(
    request: reqwest::RequestBuilder,
    read_body: F,
    body_kind: &str,
) -> Result<ResponseBody<T>>
where
    F: Fn(Response) -> Fut + Copy,
    Fut: Future<Output = reqwest::Result<T>>,
{
    let original = request
        .try_clone()
        .ok_or_else(|| anyhow!("HTTP request body cannot be retried"))?;
    let mut request = request;
    let mut attempt = 1usize;

    loop {
        let response = match request.send().await {
            Ok(response) => response,
            Err(err) if attempt < HTTP_MAX_ATTEMPTS && is_retryable_error(&err) => {
                eprintln!(
                    "retrying HTTP request after network error (attempt {})",
                    attempt + 1
                );
                sleep_before_retry(attempt).await;
                request = original
                    .try_clone()
                    .ok_or_else(|| anyhow!("HTTP request body cannot be retried"))?;
                attempt += 1;
                continue;
            }
            Err(err) => return Err(err).context("HTTP request failed"),
        };

        let status = response.status();
        if attempt < HTTP_MAX_ATTEMPTS && is_retryable_status(status) {
            eprintln!(
                "retrying HTTP request after {status} (attempt {})",
                attempt + 1
            );
            sleep_before_retry(attempt).await;
            request = original
                .try_clone()
                .ok_or_else(|| anyhow!("HTTP request body cannot be retried"))?;
            attempt += 1;
            continue;
        }

        match read_body(response).await {
            Ok(body) => return Ok(ResponseBody { status, body }),
            Err(err) if attempt < HTTP_MAX_ATTEMPTS && is_retryable_error(&err) => {
                eprintln!(
                    "retrying HTTP request after {body_kind} read error (attempt {})",
                    attempt + 1
                );
                sleep_before_retry(attempt).await;
                request = original
                    .try_clone()
                    .ok_or_else(|| anyhow!("HTTP request body cannot be retried"))?;
                attempt += 1;
            }
            Err(err) => return Err(err).context("failed to read HTTP response body"),
        }
    }
}

pub fn is_retryable_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::REQUEST_TIMEOUT
            | StatusCode::TOO_MANY_REQUESTS
            | StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    )
}

pub async fn sleep_before_retry(attempt: usize) {
    sleep(retry_delay(attempt)).await;
}

fn is_retryable_error(err: &reqwest::Error) -> bool {
    err.is_timeout() || err.is_connect()
}

fn retry_delay(attempt: usize) -> Duration {
    let multiplier = 1u32 << attempt.saturating_sub(1).min(3);
    (RETRY_BASE_DELAY * multiplier).min(RETRY_MAX_DELAY)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_retryable_status_codes() {
        assert!(is_retryable_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(is_retryable_status(StatusCode::SERVICE_UNAVAILABLE));
        assert!(!is_retryable_status(StatusCode::UNAUTHORIZED));
        assert!(!is_retryable_status(StatusCode::NOT_FOUND));
    }

    #[test]
    fn retry_delay_is_capped() {
        assert_eq!(retry_delay(1), Duration::from_millis(500));
        assert_eq!(retry_delay(2), Duration::from_secs(1));
        assert_eq!(retry_delay(99), RETRY_MAX_DELAY);
    }
}
