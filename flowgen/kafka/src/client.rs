use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
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
    #[error("Error creating Kafka admin client: {source}")]
    CreateAdminClient {
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
    pub admin_client: Option<AdminClient<DefaultClientContext>>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("credentials_path", &self.credentials_path)
            .field("brokers", &self.brokers)
            .field(
                "producer",
                &self.producer.as_ref().map(|_| "FutureProducer"),
            )
            .field(
                "admin_client",
                &self.admin_client.as_ref().map(|_| "AdminClient"),
            )
            .finish()
    }
}

/// Applies SASL/SSL credentials to a `ClientConfig`.
fn apply_credentials(config: &mut ClientConfig, path: &PathBuf) -> Result<(), Error> {
    let credentials: Credentials =
        serde_json::from_str(&std::fs::read_to_string(path).map_err(|e| {
            Error::ReadCredentials {
                path: path.clone(),
                source: e,
            }
        })?)
        .map_err(|e| Error::ParseCredentials { source: e })?;

    if let Some(sasl) = &credentials.sasl {
        config.set("sasl.mechanism", &sasl.mechanism);
        config.set("sasl.username", &sasl.username);
        config.set("sasl.password", &sasl.password);
        config.set("security.protocol", "SASL_PLAINTEXT");
    }

    if let Some(ssl) = &credentials.ssl {
        if let Some(ca) = &ssl.ca_location {
            config.set("ssl.ca.location", ca.to_string_lossy().as_ref());
        }
        if let Some(cert) = &ssl.certificate_location {
            config.set("ssl.certificate.location", cert.to_string_lossy().as_ref());
        }
        if let Some(key) = &ssl.key_location {
            config.set("ssl.key.location", key.to_string_lossy().as_ref());
        }
        if let Some(pwd) = &ssl.key_password {
            config.set("ssl.key.password", pwd);
        }
        config.set("security.protocol", "SSL");
    }

    Ok(())
}

/// Builds a base `ClientConfig` from broker string and optional credentials path.
pub fn build_base_config(
    credentials_path: &Option<PathBuf>,
    brokers: &str,
) -> Result<ClientConfig, Error> {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", brokers);
    config.set("message.timeout.ms", "5000");

    if let Some(path) = credentials_path {
        apply_credentials(&mut config, path)?;
    }

    Ok(config)
}

impl flowgen_core::client::Client for Client {
    type Error = Error;

    async fn connect(mut self) -> Result<Self, Error> {
        let brokers = self
            .brokers
            .clone()
            .unwrap_or_else(|| DEFAULT_KAFKA_BROKERS.to_string());

        let config = build_base_config(&self.credentials_path, &brokers)?;

        let producer: FutureProducer = config
            .create()
            .map_err(|e| Error::CreateProducer { source: e })?;

        let admin_client: AdminClient<DefaultClientContext> = config
            .create()
            .map_err(|e| Error::CreateAdminClient { source: e })?;

        self.producer = Some(producer);
        self.admin_client = Some(admin_client);
        Ok(self)
    }
}

impl Client {
    pub fn new(credentials_path: Option<PathBuf>, brokers: Option<String>) -> Self {
        Self {
            credentials_path,
            brokers,
            producer: None,
            admin_client: None,
        }
    }
}
