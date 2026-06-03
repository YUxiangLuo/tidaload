use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use serde::Deserialize;

use crate::http::{HTTP_SHORT_REQUEST_TIMEOUT, configure_http_client, send_text_with_retries};

const DOH_ENDPOINT: &str = "https://dns.google/resolve";
const MIN_TTL: Duration = Duration::from_secs(30);
const MAX_TTL: Duration = Duration::from_secs(300);

#[derive(Debug, Clone)]
pub struct DohResolver {
    client: reqwest::Client,
    cache: Arc<Mutex<HashMap<String, CacheEntry>>>,
}

#[derive(Debug, Clone)]
struct CacheEntry {
    addrs: Vec<SocketAddr>,
    expires_at: Instant,
}

#[derive(Debug, Deserialize)]
struct DnsResponse {
    #[serde(rename = "Status")]
    status: u16,
    #[serde(rename = "Answer", default)]
    answers: Vec<DnsAnswer>,
}

#[derive(Debug, Deserialize)]
struct DnsAnswer {
    #[serde(rename = "type")]
    record_type: u16,
    #[serde(rename = "TTL")]
    ttl: Option<u64>,
    data: String,
}

impl DohResolver {
    pub fn new() -> Result<Self> {
        let client = configure_http_client(reqwest::Client::builder())
            .user_agent("tidaload-doh/0.1")
            .resolve_to_addrs(
                "dns.google",
                &[
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)), 443),
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(8, 8, 4, 4)), 443),
                ],
            )
            .build()
            .context("failed to build DoH client")?;

        Ok(Self {
            client,
            cache: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    async fn resolve_host(&self, host: String) -> Result<Vec<SocketAddr>> {
        if let Ok(ip) = host.parse::<IpAddr>() {
            return Ok(vec![SocketAddr::new(ip, 0)]);
        }

        if let Some(addrs) = self.cached(&host)? {
            return Ok(addrs);
        }

        let request = self
            .client
            .get(DOH_ENDPOINT)
            .query(&[("name", host.as_str()), ("type", "A")])
            .timeout(HTTP_SHORT_REQUEST_TIMEOUT);
        let response = send_text_with_retries(request)
            .await
            .with_context(|| format!("failed to query DoH for {host}"))?;
        if !response.status.is_success() {
            bail!("DoH request failed for {host}: {}", response.status);
        }
        let response: DnsResponse = serde_json::from_str(&response.body)
            .with_context(|| format!("failed to parse DoH response for {host}"))?;

        if response.status != 0 {
            bail!("DoH returned status {} for {host}", response.status);
        }

        let mut ttl = MAX_TTL;
        let mut addrs = Vec::new();
        for answer in response.answers {
            if let Some(answer_ttl) = answer.ttl {
                ttl = ttl.min(ttl_bounds(answer_ttl));
            }
            if answer.record_type != 1 {
                continue;
            }
            let ip = answer
                .data
                .parse::<Ipv4Addr>()
                .with_context(|| format!("invalid A record for {host}: {}", answer.data))?;
            addrs.push(SocketAddr::new(IpAddr::V4(ip), 0));
        }

        if addrs.is_empty() {
            bail!("DoH returned no A records for {host}");
        }

        self.insert_cache(host, addrs.clone(), ttl)?;
        Ok(addrs)
    }

    fn cached(&self, host: &str) -> Result<Option<Vec<SocketAddr>>> {
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| anyhow!("DoH cache lock poisoned"))?;

        match cache.get(host) {
            Some(entry) if entry.expires_at > Instant::now() => Ok(Some(entry.addrs.clone())),
            Some(_) => {
                cache.remove(host);
                Ok(None)
            }
            None => Ok(None),
        }
    }

    fn insert_cache(&self, host: String, addrs: Vec<SocketAddr>, ttl: Duration) -> Result<()> {
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| anyhow!("DoH cache lock poisoned"))?;
        cache.insert(
            host,
            CacheEntry {
                addrs,
                expires_at: Instant::now() + ttl,
            },
        );
        Ok(())
    }
}

impl Resolve for DohResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let resolver = self.clone();
        let host = name.as_str().trim_end_matches('.').to_ascii_lowercase();
        Box::pin(async move {
            let addrs = resolver.resolve_host(host).await?;
            let addrs: Addrs = Box::new(addrs.into_iter());
            Ok(addrs)
        })
    }
}

fn ttl_bounds(ttl: u64) -> Duration {
    let ttl = Duration::from_secs(ttl);
    ttl.clamp(MIN_TTL, MAX_TTL)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamps_ttl() {
        assert_eq!(ttl_bounds(1), MIN_TTL);
        assert_eq!(ttl_bounds(60), Duration::from_secs(60));
        assert_eq!(ttl_bounds(999), MAX_TTL);
    }
}
