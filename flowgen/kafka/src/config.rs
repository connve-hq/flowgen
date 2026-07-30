use flowgen_core::config::ConfigExt;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

fn default_brokers() -> String {
    "localhost:9092".to_string()
}

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Produce {
    pub name: String,
    #[serde(default)]
    pub credentials_path: Option<PathBuf>,
    #[serde(default = "default_brokers")]
    pub brokers: String,
    pub topic: String,
    pub message_key: Option<String>,
    #[serde(default, with = "humantime_serde")]
    pub ack_timeout: Option<Duration>,
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl ConfigExt for Produce {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_produce_default() {
        let config = Produce::default();
        assert_eq!(config.name, String::new());
        assert_eq!(config.brokers, String::new());
        assert_eq!(config.topic, String::new());
        assert_eq!(config.credentials_path, None);
    }

    #[test]
    fn test_produce_creation() {
        let config = Produce {
            name: "test_producer".to_string(),
            brokers: "kafka:9092".to_string(),
            topic: "test-topic".to_string(),
            message_key: Some("key-{{id}}".to_string()),
            credentials_path: Some(PathBuf::from("/path/to/kafka.creds")),
            ..Default::default()
        };
        assert_eq!(config.name, "test_producer");
        assert_eq!(config.brokers, "kafka:9092");
        assert_eq!(config.topic, "test-topic");
        assert_eq!(config.message_key, Some("key-{{id}}".to_string()));
    }

    #[test]
    fn test_produce_serialization() {
        let config = Produce {
            name: "serial_producer".to_string(),
            brokers: "broker1:9092,broker2:9092".to_string(),
            topic: "serial-topic".to_string(),
            ..Default::default()
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: Produce = serde_json::from_str(&json).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_produce_clone() {
        let config = Produce {
            name: "clone_producer".to_string(),
            brokers: "clone:9092".to_string(),
            topic: "clone-topic".to_string(),
            ..Default::default()
        };
        let cloned = config.clone();
        assert_eq!(config, cloned);
    }
}
