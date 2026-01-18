//! Key-Value storage operations for flashQ.
//!
//! Provides Redis-like KV storage with:
//! - SET/GET/DEL operations
//! - TTL support with automatic expiration
//! - MGET/MSET for batch operations
//! - KEYS pattern matching
//! - INCR/DECR for atomic counters
//! - PostgreSQL persistence (optional)

use parking_lot::RwLock;
use serde_json::Value;

use super::manager::QueueManager;
use super::types::{now_ms, GxHashMap};

/// Maximum key length (1KB)
pub const MAX_KEY_LENGTH: usize = 1024;
/// Maximum value size (1MB)
pub const MAX_VALUE_SIZE: usize = 1_048_576;
/// Maximum keys per MSET/MGET operation
pub const MAX_BATCH_SIZE: usize = 1000;

/// KV entry with optional expiration
#[derive(Debug, Clone)]
pub struct KvValue {
    pub value: Value,
    pub expires_at: Option<u64>, // Timestamp in ms, None = no expiration
}

impl KvValue {
    pub fn new(value: Value, ttl: Option<u64>) -> Self {
        let expires_at = ttl.map(|t| now_ms() + t);
        Self { value, expires_at }
    }

    pub fn is_expired(&self) -> bool {
        self.expires_at.map(|exp| now_ms() > exp).unwrap_or(false)
    }

    pub fn remaining_ttl(&self) -> i64 {
        match self.expires_at {
            Some(exp) => {
                let now = now_ms();
                if now > exp {
                    0
                } else {
                    (exp - now) as i64
                }
            }
            None => -1, // No TTL set
        }
    }
}

/// Type alias for KV store
pub type KvStore = RwLock<GxHashMap<String, KvValue>>;

impl QueueManager {
    // === Core KV Operations ===

    /// Set a key-value pair with optional TTL
    pub fn kv_set(&self, key: String, value: Value, ttl: Option<u64>) -> Result<(), String> {
        // Validate key
        if key.is_empty() {
            return Err("Key cannot be empty".into());
        }
        if key.len() > MAX_KEY_LENGTH {
            return Err(format!("Key too long ({} > {} bytes)", key.len(), MAX_KEY_LENGTH));
        }

        // Validate value size (rough estimate)
        let value_str = value.to_string();
        if value_str.len() > MAX_VALUE_SIZE {
            return Err(format!(
                "Value too large ({} > {} bytes)",
                value_str.len(),
                MAX_VALUE_SIZE
            ));
        }

        let entry = KvValue::new(value, ttl);
        self.kv_store.write().insert(key, entry);
        Ok(())
    }

    /// Get a value by key (returns None if expired or not found)
    pub fn kv_get(&self, key: &str) -> Option<Value> {
        let store = self.kv_store.read();
        match store.get(key) {
            Some(entry) if !entry.is_expired() => Some(entry.value.clone()),
            _ => None,
        }
    }

    /// Delete a key, returns true if key existed
    pub fn kv_del(&self, key: &str) -> bool {
        self.kv_store.write().remove(key).is_some()
    }

    /// Get multiple values by keys
    pub fn kv_mget(&self, keys: &[String]) -> Vec<Option<Value>> {
        let store = self.kv_store.read();
        keys.iter()
            .take(MAX_BATCH_SIZE)
            .map(|key| match store.get(key) {
                Some(entry) if !entry.is_expired() => Some(entry.value.clone()),
                _ => None,
            })
            .collect()
    }

    /// Set multiple key-value pairs
    pub fn kv_mset(&self, entries: Vec<(String, Value, Option<u64>)>) -> Result<usize, String> {
        if entries.len() > MAX_BATCH_SIZE {
            return Err(format!(
                "Too many entries ({} > {})",
                entries.len(),
                MAX_BATCH_SIZE
            ));
        }

        let mut store = self.kv_store.write();
        let mut count = 0;

        for (key, value, ttl) in entries {
            // Validate each entry
            if key.is_empty() || key.len() > MAX_KEY_LENGTH {
                continue;
            }
            let value_str = value.to_string();
            if value_str.len() > MAX_VALUE_SIZE {
                continue;
            }

            store.insert(key, KvValue::new(value, ttl));
            count += 1;
        }

        Ok(count)
    }

    /// Check if a key exists (and is not expired)
    pub fn kv_exists(&self, key: &str) -> bool {
        let store = self.kv_store.read();
        match store.get(key) {
            Some(entry) => !entry.is_expired(),
            None => false,
        }
    }

    /// Set TTL on an existing key
    pub fn kv_expire(&self, key: &str, ttl: u64) -> bool {
        let mut store = self.kv_store.write();
        if let Some(entry) = store.get_mut(key) {
            if entry.is_expired() {
                return false;
            }
            entry.expires_at = Some(now_ms() + ttl);
            true
        } else {
            false
        }
    }

    /// Get remaining TTL for a key
    /// Returns: milliseconds remaining, -1 if no TTL, -2 if key doesn't exist
    pub fn kv_ttl(&self, key: &str) -> i64 {
        let store = self.kv_store.read();
        match store.get(key) {
            Some(entry) if !entry.is_expired() => entry.remaining_ttl(),
            _ => -2, // Key doesn't exist (or expired)
        }
    }

    /// List keys matching a pattern (simple glob: * matches any, ? matches one char)
    pub fn kv_keys(&self, pattern: Option<&str>) -> Vec<String> {
        let store = self.kv_store.read();
        let now = now_ms();

        store
            .iter()
            .filter(|(_, entry)| !entry.expires_at.map(|exp| now > exp).unwrap_or(false))
            .filter(|(key, _)| match pattern {
                Some(p) => glob_match(p, key),
                None => true,
            })
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// Increment a numeric value atomically
    /// If key doesn't exist, creates it with value = by
    /// Returns error if value is not a number
    pub fn kv_incr(&self, key: &str, by: i64) -> Result<i64, String> {
        let mut store = self.kv_store.write();

        match store.get_mut(key) {
            Some(entry) if !entry.is_expired() => {
                // Try to increment existing value
                let current = match &entry.value {
                    Value::Number(n) => n.as_i64().ok_or("Value is not an integer")?,
                    Value::Null => 0,
                    _ => return Err("Value is not a number".into()),
                };
                let new_value = current + by;
                entry.value = Value::Number(new_value.into());
                Ok(new_value)
            }
            _ => {
                // Key doesn't exist or is expired, create new
                store.insert(
                    key.to_string(),
                    KvValue::new(Value::Number(by.into()), None),
                );
                Ok(by)
            }
        }
    }

    // === Cleanup ===

    /// Remove expired keys from the KV store
    /// Called by background task
    pub(crate) fn cleanup_expired_kv(&self) {
        let now = now_ms();
        let mut store = self.kv_store.write();
        store.retain(|_, entry| !entry.expires_at.map(|exp| now > exp).unwrap_or(false));
    }

    /// Get KV store statistics
    #[allow(dead_code)]
    pub fn kv_stats(&self) -> (usize, usize) {
        let store = self.kv_store.read();
        let now = now_ms();
        let total = store.len();
        let with_ttl = store
            .values()
            .filter(|e| e.expires_at.is_some() && !e.expires_at.map(|exp| now > exp).unwrap_or(false))
            .count();
        (total, with_ttl)
    }
}

/// Simple glob pattern matching (* = any chars, ? = one char)
fn glob_match(pattern: &str, text: &str) -> bool {
    let mut p_chars = pattern.chars().peekable();
    let mut t_chars = text.chars().peekable();

    while let Some(p) = p_chars.next() {
        match p {
            '*' => {
                // * matches zero or more characters
                if p_chars.peek().is_none() {
                    return true; // * at end matches everything
                }
                // Try matching rest of pattern at each position
                let rest_pattern: String = p_chars.collect();
                let mut remaining: String = t_chars.collect();
                loop {
                    if glob_match(&rest_pattern, &remaining) {
                        return true;
                    }
                    if remaining.is_empty() {
                        return false;
                    }
                    remaining = remaining[1..].to_string();
                }
            }
            '?' => {
                // ? matches exactly one character
                if t_chars.next().is_none() {
                    return false;
                }
            }
            c => {
                // Literal character must match
                if t_chars.next() != Some(c) {
                    return false;
                }
            }
        }
    }

    // Pattern exhausted, text must also be exhausted
    t_chars.next().is_none()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_match_literal() {
        assert!(glob_match("hello", "hello"));
        assert!(!glob_match("hello", "world"));
        assert!(!glob_match("hello", "hell"));
        assert!(!glob_match("hello", "helloo"));
    }

    #[test]
    fn test_glob_match_star() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("*", ""));
        assert!(glob_match("hello*", "hello"));
        assert!(glob_match("hello*", "helloworld"));
        assert!(glob_match("*world", "helloworld"));
        assert!(glob_match("*world", "world"));
        assert!(glob_match("h*d", "helloworld"));
        assert!(!glob_match("h*d", "hello"));
    }

    #[test]
    fn test_glob_match_question() {
        assert!(glob_match("?", "a"));
        assert!(!glob_match("?", ""));
        assert!(!glob_match("?", "ab"));
        assert!(glob_match("h?llo", "hello"));
        assert!(glob_match("h?llo", "hallo"));
        assert!(!glob_match("h?llo", "hllo"));
    }

    #[test]
    fn test_glob_match_combined() {
        assert!(glob_match("user:*", "user:123"));
        assert!(glob_match("user:*:profile", "user:123:profile"));
        assert!(glob_match("cache:???", "cache:abc"));
        assert!(!glob_match("cache:???", "cache:abcd"));
    }
}
