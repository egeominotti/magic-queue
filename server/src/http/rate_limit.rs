//! HTTP API rate limiting middleware.
//!
//! Implements a simple fixed-window rate limiter per client IP address.
//! Configurable via environment variables:
//! - RATE_LIMIT_REQUESTS: Max requests per window (default: 1000)
//! - RATE_LIMIT_WINDOW_SECS: Window duration in seconds (default: 60)

use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    body::Body,
    extract::ConnectInfo,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};
use dashmap::DashMap;
use parking_lot::RwLock;

/// Rate limiter state shared across all requests.
#[derive(Clone)]
pub struct RateLimiter {
    /// Per-IP request counts
    requests: Arc<DashMap<IpAddr, AtomicU64>>,
    /// Current window start time
    window_start: Arc<RwLock<Instant>>,
    /// Maximum requests per window
    max_requests: u64,
    /// Window duration
    window_duration: Duration,
}

impl RateLimiter {
    /// Create a new rate limiter from environment configuration.
    pub fn from_env() -> Self {
        let max_requests = std::env::var("RATE_LIMIT_REQUESTS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000);

        let window_secs = std::env::var("RATE_LIMIT_WINDOW_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(60);

        Self {
            requests: Arc::new(DashMap::new()),
            window_start: Arc::new(RwLock::new(Instant::now())),
            max_requests,
            window_duration: Duration::from_secs(window_secs),
        }
    }

    /// Check if rate limiting is enabled (max_requests > 0).
    pub fn is_enabled(&self) -> bool {
        self.max_requests > 0
    }

    /// Check if a request from the given IP should be allowed.
    /// Returns (allowed, remaining, reset_after_secs).
    pub fn check(&self, ip: IpAddr) -> (bool, u64, u64) {
        let now = Instant::now();

        // Check if we need to reset the window
        {
            let window_start = *self.window_start.read();
            if now.duration_since(window_start) >= self.window_duration {
                // Reset window
                let mut w = self.window_start.write();
                if now.duration_since(*w) >= self.window_duration {
                    *w = now;
                    self.requests.clear();
                }
            }
        }

        // Get or create counter for this IP
        let counter = self.requests.entry(ip).or_insert_with(|| AtomicU64::new(0));
        let current = counter.fetch_add(1, Ordering::Relaxed) + 1;

        let remaining = self.max_requests.saturating_sub(current);
        let window_start = *self.window_start.read();
        let elapsed = now.duration_since(window_start).as_secs();
        let reset_after = self.window_duration.as_secs().saturating_sub(elapsed);

        (current <= self.max_requests, remaining, reset_after)
    }
}

/// Rate limiting middleware for Axum.
pub async fn rate_limit_middleware(
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    request: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    // Get rate limiter from extensions (set by the layer)
    let rate_limiter = request
        .extensions()
        .get::<RateLimiter>()
        .cloned()
        .unwrap_or_else(RateLimiter::from_env);

    // Skip if rate limiting is disabled
    if !rate_limiter.is_enabled() {
        return Ok(next.run(request).await);
    }

    let ip = addr.ip();
    let (allowed, remaining, reset_after) = rate_limiter.check(ip);

    if !allowed {
        tracing::warn!(ip = %ip, "Rate limit exceeded");
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }

    let mut response = next.run(request).await;

    // Add rate limit headers
    response.headers_mut().insert(
        "X-RateLimit-Remaining",
        remaining.to_string().parse().unwrap(),
    );
    response.headers_mut().insert(
        "X-RateLimit-Reset",
        reset_after.to_string().parse().unwrap(),
    );

    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn test_rate_limiter_allows_under_limit() {
        let limiter = RateLimiter {
            requests: Arc::new(DashMap::new()),
            window_start: Arc::new(RwLock::new(Instant::now())),
            max_requests: 10,
            window_duration: Duration::from_secs(60),
        };

        let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

        for _ in 0..10 {
            let (allowed, _, _) = limiter.check(ip);
            assert!(allowed);
        }
    }

    #[test]
    fn test_rate_limiter_blocks_over_limit() {
        let limiter = RateLimiter {
            requests: Arc::new(DashMap::new()),
            window_start: Arc::new(RwLock::new(Instant::now())),
            max_requests: 5,
            window_duration: Duration::from_secs(60),
        };

        let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

        for _ in 0..5 {
            let (allowed, _, _) = limiter.check(ip);
            assert!(allowed);
        }

        let (allowed, remaining, _) = limiter.check(ip);
        assert!(!allowed);
        assert_eq!(remaining, 0);
    }

    #[test]
    fn test_rate_limiter_tracks_ips_separately() {
        let limiter = RateLimiter {
            requests: Arc::new(DashMap::new()),
            window_start: Arc::new(RwLock::new(Instant::now())),
            max_requests: 2,
            window_duration: Duration::from_secs(60),
        };

        let ip1 = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
        let ip2 = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2));

        // Both IPs can make requests independently
        limiter.check(ip1);
        limiter.check(ip1);
        let (allowed1, _, _) = limiter.check(ip1);
        assert!(!allowed1); // IP1 is over limit

        let (allowed2, _, _) = limiter.check(ip2);
        assert!(allowed2); // IP2 still has quota
    }
}
