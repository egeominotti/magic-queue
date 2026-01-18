//! Pub/Sub module for flashQ.
//!
//! Redis-like publish/subscribe messaging system.

use dashmap::DashMap;
use gxhash::GxBuildHasher;
use serde_json::Value;
use tokio::sync::mpsc;

use super::kv::glob_match;

/// Message sent to subscribers
#[derive(Debug, Clone)]
pub struct PubSubMessage {
    pub channel: String,
    pub message: Value,
    pub pattern: Option<String>, // Set if matched via pattern subscription
}

/// Subscriber handle - wraps an mpsc sender
pub type SubscriberSender = mpsc::UnboundedSender<PubSubMessage>;
pub type SubscriberReceiver = mpsc::UnboundedReceiver<PubSubMessage>;

/// Subscriber entry with sender and subscribed channels/patterns
#[derive(Debug)]
pub struct Subscriber {
    pub sender: SubscriberSender,
    pub channels: Vec<String>,
    pub patterns: Vec<String>,
}

/// Pub/Sub system
pub struct PubSub {
    /// Channel -> list of subscriber senders
    channels: DashMap<String, Vec<SubscriberSender>, GxBuildHasher>,
    /// Pattern -> list of subscriber senders
    patterns: DashMap<String, Vec<SubscriberSender>, GxBuildHasher>,
}

impl Default for PubSub {
    fn default() -> Self {
        Self::new()
    }
}

impl PubSub {
    pub fn new() -> Self {
        Self {
            channels: DashMap::with_hasher(GxBuildHasher::default()),
            patterns: DashMap::with_hasher(GxBuildHasher::default()),
        }
    }

    /// Publish a message to a channel.
    /// Returns the number of subscribers that received the message.
    pub fn publish(&self, channel: &str, message: Value) -> usize {
        let mut count = 0;

        // Send to exact channel subscribers
        if let Some(mut subscribers) = self.channels.get_mut(channel) {
            // Remove disconnected subscribers (send fails)
            subscribers.retain(|sender| {
                let msg = PubSubMessage {
                    channel: channel.to_string(),
                    message: message.clone(),
                    pattern: None,
                };
                if sender.send(msg).is_ok() {
                    count += 1;
                    true
                } else {
                    false
                }
            });
        }

        // Send to pattern subscribers
        for mut entry in self.patterns.iter_mut() {
            let pattern = entry.key();
            if glob_match(pattern, channel) {
                let pattern_str = pattern.clone();
                entry.value_mut().retain(|sender| {
                    let msg = PubSubMessage {
                        channel: channel.to_string(),
                        message: message.clone(),
                        pattern: Some(pattern_str.clone()),
                    };
                    if sender.send(msg).is_ok() {
                        count += 1;
                        true
                    } else {
                        false
                    }
                });
            }
        }

        count
    }

    /// Subscribe to channels.
    /// Returns a receiver for incoming messages.
    pub fn subscribe(&self, channels: Vec<String>) -> SubscriberReceiver {
        let (tx, rx) = mpsc::unbounded_channel();

        for channel in channels {
            self.channels
                .entry(channel)
                .or_insert_with(Vec::new)
                .push(tx.clone());
        }

        rx
    }

    /// Subscribe to patterns.
    /// Returns a receiver for incoming messages.
    pub fn psubscribe(&self, patterns: Vec<String>) -> SubscriberReceiver {
        let (tx, rx) = mpsc::unbounded_channel();

        for pattern in patterns {
            self.patterns
                .entry(pattern)
                .or_insert_with(Vec::new)
                .push(tx.clone());
        }

        rx
    }

    /// Unsubscribe from channels.
    /// Note: This removes ALL subscribers for those channels - caller should track their own sender.
    pub fn unsubscribe(&self, channels: &[String], sender: &SubscriberSender) {
        for channel in channels {
            if let Some(mut subscribers) = self.channels.get_mut(channel) {
                // Remove this specific sender by checking if it's the same channel
                subscribers.retain(|s| !s.same_channel(sender));
            }
        }
    }

    /// Unsubscribe from patterns.
    pub fn punsubscribe(&self, patterns: &[String], sender: &SubscriberSender) {
        for pattern in patterns {
            if let Some(mut subscribers) = self.patterns.get_mut(pattern) {
                subscribers.retain(|s| !s.same_channel(sender));
            }
        }
    }

    /// List active channels (optionally matching a pattern).
    pub fn channels(&self, pattern: Option<&str>) -> Vec<String> {
        self.channels
            .iter()
            .filter(|entry| !entry.value().is_empty())
            .filter(|entry| pattern.map(|p| glob_match(p, entry.key())).unwrap_or(true))
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Count subscribers for each channel.
    pub fn numsub(&self, channels: &[String]) -> Vec<(String, usize)> {
        channels
            .iter()
            .map(|channel| {
                let count = self.channels.get(channel).map(|s| s.len()).unwrap_or(0);
                (channel.clone(), count)
            })
            .collect()
    }

    /// Cleanup empty channels (called periodically).
    pub fn cleanup(&self) {
        self.channels.retain(|_, v| !v.is_empty());
        self.patterns.retain(|_, v| !v.is_empty());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_publish_subscribe() {
        let pubsub = PubSub::new();

        // Subscribe to channel
        let mut rx = pubsub.subscribe(vec!["events".to_string()]);

        // Publish message
        let count = pubsub.publish("events", serde_json::json!({"type": "test"}));
        assert_eq!(count, 1);

        // Receive message
        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.channel, "events");
        assert_eq!(msg.message, serde_json::json!({"type": "test"}));
        assert!(msg.pattern.is_none());
    }

    #[tokio::test]
    async fn test_pattern_subscribe() {
        let pubsub = PubSub::new();

        // Subscribe to pattern
        let mut rx = pubsub.psubscribe(vec!["events:*".to_string()]);

        // Publish to matching channel
        let count = pubsub.publish("events:user", serde_json::json!({"user": "john"}));
        assert_eq!(count, 1);

        // Receive message
        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.channel, "events:user");
        assert_eq!(msg.pattern, Some("events:*".to_string()));
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        let pubsub = PubSub::new();

        let mut rx1 = pubsub.subscribe(vec!["chat".to_string()]);
        let mut rx2 = pubsub.subscribe(vec!["chat".to_string()]);

        let count = pubsub.publish("chat", serde_json::json!("hello"));
        assert_eq!(count, 2);

        let msg1 = rx1.recv().await.unwrap();
        let msg2 = rx2.recv().await.unwrap();
        assert_eq!(msg1.message, serde_json::json!("hello"));
        assert_eq!(msg2.message, serde_json::json!("hello"));
    }

    #[tokio::test]
    async fn test_channels_list() {
        let pubsub = PubSub::new();

        let _rx1 = pubsub.subscribe(vec!["events:a".to_string()]);
        let _rx2 = pubsub.subscribe(vec!["events:b".to_string()]);
        let _rx3 = pubsub.subscribe(vec!["logs".to_string()]);

        let all = pubsub.channels(None);
        assert_eq!(all.len(), 3);

        let events = pubsub.channels(Some("events:*"));
        assert_eq!(events.len(), 2);
    }

    #[tokio::test]
    async fn test_numsub() {
        let pubsub = PubSub::new();

        let _rx1 = pubsub.subscribe(vec!["chat".to_string()]);
        let _rx2 = pubsub.subscribe(vec!["chat".to_string()]);
        let _rx3 = pubsub.subscribe(vec!["alerts".to_string()]);

        let counts = pubsub.numsub(&[
            "chat".to_string(),
            "alerts".to_string(),
            "missing".to_string(),
        ]);
        assert_eq!(
            counts,
            vec![
                ("chat".to_string(), 2),
                ("alerts".to_string(), 1),
                ("missing".to_string(), 0),
            ]
        );
    }
}
