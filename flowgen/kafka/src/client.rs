use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;
use std::path::PathBuf;

pub const DEFAULT_KAFKA_BROKERS: &str = "localhost:9092";

#[derive(serde::Deserialize, Debug, Clone, PartialEq, Default)]
pub struct Credentials {
    pub sasl: Option<SaslCredentials>,
    pub ssl: Option<SslCredentials>,
}

#[derive(serde::Deserialize, Debug, Clone, PartialEq)]
pub struct SaslCredentials {
    pub username: String,
    pub password: String,
    #[serde(default = "default_sasl_mechanism")]
    pub mechanism: String,
}

fn default_sasl_mechanism() -> String {
    "SCRAM-SHA-256".to_string()
}

#[derive(serde::Deserialize, Debug, Clone, PartialEq)]
pub struct SslCredentials {
    pub ca_location: Option<PathBuf>,
    pub certificate_location: Option<PathBuf>,
    pub key_location: Option<PathBuf>,
    pub key_password: Option<String>,
}

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Error reading credentials file '{path}': {source}")]
    ReadCredentials {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Error parsing credentials file: {source}")]
    ParseCredentials {
        #[source]
        source: serde_json::Error,
    },
    #[error("Error creating Kafka producer: {source}")]
    CreateProducer {
        #[source]
        source: rdkafka::error::KafkaError,
    },
    #[error("No authentication credentials provided")]
    NoCredentials,
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
}

pub struct Client {
    credentials_path: Option<PathBuf>,
    brokers: Option<String>,
    pub producer: Option<FutureProducer>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("credentials_path", &self.credentials_path)
            .field("brokers", &self.brokers)
            .field("producer", &self.producer.as_ref().map(|_| "FutureProducer"))
            .finish()
    }
}

impl flowgen_core::client::Client for Client {
    type Error = Error;

    async fn connect(mut self) -> Result<Self, Error> {
        let brokers = self
            .brokers
            .clone()
            .unwrap_or_else(|| DEFAULT_KAFKA_BROKERS.to_string());

        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", &brokers);
        client_config.set("message.timeout.ms", "5000");

        if let Some(path) = &self.credentials_path {
            let credentials: Credentials =
                serde_json::from_str(
                    &std::fs::read_to_string(path)
                        .map_err(|e| Error::ReadCredentials {
                            path: path.clone(),
                            source: e,
                        })?,
                )
                .map_err(|e| Error::ParseCredentials { source: e })?;

            if let Some(sasl) = &credentials.sasl {
                client_config.set("sasl.mechanism", &sasl.mechanism);
                client_config.set("sasl.username", &sasl.username);
                client_config.set("sasl.password", &sasl.password);
                client_config.set("security.protocol", "SASL_PLAINTEXT");
            }

            if let Some(ssl) = &credentials.ssl {
                if let Some(ca) = &ssl.ca_location {
                    client_config.set(
                        "ssl.ca.location",
                        ca.to_string_lossy().as_ref(),
                    );
                }
                if let Some(cert) = &ssl.certificate_location {
                    client_config.set(
                        "ssl.certificate.location",
                        cert.to_string_lossy().as_ref(),
                    );
                }
                if let Some(key) = &ssl.key_location {
                    client_config.set(
                        "ssl.key.location",
                        key.to_string_lossy().as_ref(),
                    );
                }
                if let Some(pwd) = &ssl.key_password {
                    client_config.set("ssl.key.password", pwd);
                }
                client_config.set("security.protocol", "SSL");
            }
        }

        let producer: FutureProducer = client_config
            .create()
            .map_err(|e| Error::CreateProducer { source: e })?;

        self.producer = Some(producer);
        Ok(self)
    }
}

impl Client {
    pub fn new(credentials_path: Option<PathBuf>, brokers: Option<String>) -> Self {
        Self {
            credentials_path,
            brokers,
            producer: None,
        }
    }
}
