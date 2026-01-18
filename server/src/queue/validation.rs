//! Input validation functions and constants for job queue operations.
//!
//! Contains validation logic for queue names, job data size limits,
//! and DoS prevention measures.

use serde_json::Value;

/// Maximum job data size in bytes (10MB) for AI/ML workloads (embeddings, images, etc.)
pub const MAX_JOB_DATA_SIZE: usize = 10_485_760;

/// Maximum queue name length
pub const MAX_QUEUE_NAME_LENGTH: usize = 256;

/// Maximum batch size to prevent DoS attacks
pub const MAX_BATCH_SIZE: usize = 1000;

/// Validate queue name - must be alphanumeric, underscores, hyphens, or dots
#[inline]
pub fn validate_queue_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("Queue name cannot be empty".to_string());
    }
    if name.len() > MAX_QUEUE_NAME_LENGTH {
        return Err(format!(
            "Queue name too long (max {} chars)",
            MAX_QUEUE_NAME_LENGTH
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '.')
    {
        return Err(
            "Queue name must contain only alphanumeric characters, underscores, hyphens, or dots"
                .to_string(),
        );
    }
    Ok(())
}

/// Estimate JSON size without serialization (faster than serde_json::to_string)
/// Returns an approximate size in bytes - may be slightly larger than actual serialized size
#[inline]
pub fn estimate_json_size(value: &Value) -> usize {
    match value {
        Value::Null => 4, // "null"
        Value::Bool(b) => {
            if *b {
                4
            } else {
                5
            }
        } // "true" or "false"
        Value::Number(n) => {
            // Estimate number length (most are < 20 chars)
            // Integers (i64, u64) are typically < 20 chars, floats slightly longer
            if n.is_f64() && !n.is_i64() && !n.is_u64() {
                24 // floating point with decimal
            } else {
                20 // integer
            }
        }
        Value::String(s) => s.len() + 2, // quotes + content (doesn't count escapes, conservative)
        Value::Array(arr) => {
            // brackets + commas + contents
            2 + arr.len().saturating_sub(1) + arr.iter().map(estimate_json_size).sum::<usize>()
        }
        Value::Object(obj) => {
            // braces + colons + commas + keys + values
            2 + obj.len().saturating_sub(1)
                + obj
                    .iter()
                    .map(|(k, v)| {
                        k.len() + 3 + estimate_json_size(v) // "key": value
                    })
                    .sum::<usize>()
        }
    }
}

/// Validate job data size using fast estimation (avoids full serialization)
#[inline]
pub fn validate_job_data(data: &Value) -> Result<(), String> {
    let estimated_size = estimate_json_size(data);
    if estimated_size > MAX_JOB_DATA_SIZE {
        return Err(format!(
            "Job data too large (~{} bytes, max {} bytes)",
            estimated_size, MAX_JOB_DATA_SIZE
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_validate_queue_name_empty() {
        assert!(validate_queue_name("").is_err());
    }

    #[test]
    fn test_validate_queue_name_valid() {
        assert!(validate_queue_name("my-queue").is_ok());
        assert!(validate_queue_name("my_queue").is_ok());
        assert!(validate_queue_name("my.queue").is_ok());
        assert!(validate_queue_name("queue123").is_ok());
    }

    #[test]
    fn test_validate_queue_name_invalid_chars() {
        assert!(validate_queue_name("my queue").is_err());
        assert!(validate_queue_name("my/queue").is_err());
        assert!(validate_queue_name("my@queue").is_err());
    }

    #[test]
    fn test_estimate_json_size_basic() {
        assert_eq!(estimate_json_size(&json!(null)), 4);
        assert_eq!(estimate_json_size(&json!(true)), 4);
        assert_eq!(estimate_json_size(&json!(false)), 5);
    }

    #[test]
    fn test_estimate_json_size_string() {
        assert_eq!(estimate_json_size(&json!("hello")), 7); // 5 + 2 quotes
    }

    #[test]
    fn test_validate_job_data_small() {
        let data = json!({"key": "value"});
        assert!(validate_job_data(&data).is_ok());
    }
}
