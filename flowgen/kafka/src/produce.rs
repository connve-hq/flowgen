use flowgen_core::client::Client;
use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use futures_util::future;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureRecord, Producer as RdkafkaProducer};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, Instrument};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ProduceResult {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Error sending event to channel: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Error building event: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Kafka client error: {source}")]
    ClientAuth {
        #[source]
        source: crate::client::Error,
    },
    #[error("Produce error: {source}")]
    Produce {
        #[source]
        source: rdkafka::error::KafkaError,
    },
    #[error("Delivery error: {status:?}")]
    Delivery {
        status: rdkafka::types::RDKafkaRespErr,
    },
    #[error("Topic '{topic}' does not exist on the Kafka cluster")]
    TopicNotFound { topic: String },
    #[error("Topic creation error for '{topic}': {source}")]
    TopicCreation {
        topic: String,
        #[source]
        source: rdkafka::error::KafkaError,
    },
    #[error("Metadata fetch error: {source}")]
    MetadataFetch {
        #[source]
        source: rdkafka::error::KafkaError,
    },
    #[error("JSON serialization error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("Config template rendering error: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("Arrow serialization error: {source}")]
    Arrow {
        #[source]
        source: arrow::error::ArrowError,
    },
    #[error("Client is missing or not initialized")]
    MissingClient,
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error(
        "Client registry type mismatch -- same credentials used with incompatible client types"
    )]
    ClientRegistryMismatch,
}

fn serialize_event_to_bytes(event: &Event) -> Result<Vec<u8>, Error> {
    match &event.data {
        EventData::ArrowRecordBatch(data) => {
            let mut buffer = Vec::new();
            let mut stream_writer =
                arrow::ipc::writer::StreamWriter::try_new(&mut buffer, &data.schema())
                    .map_err(|e| Error::Arrow { source: e })?;
            stream_writer
                .write(data)
                .map_err(|e| Error::Arrow { source: e })?;
            stream_writer
                .finish()
                .map_err(|e| Error::Arrow { source: e })?;
            Ok(buffer)
        }
        EventData::Avro(data) => Ok(data.raw_bytes.clone()),
        EventData::Json(data) => {
            serde_json::to_vec(data).map_err(|e| Error::SerdeJson { source: e })
        }
        EventData::Bytes(bytes) => Ok(bytes.to_vec()),
    }
}

/// Patches a UUID v7 id into the "event.id" field of the render context
/// when the incoming event has no id, so templates like "{{event.id}}" in
/// `message_key` always resolve to a value.
fn ensure_event_id(event_value: &mut serde_json::Value) {
    let id_is_null = event_value
        .get("event")
        .and_then(|e| e.get("id"))
        .is_none_or(|id| id.is_null());
    if id_is_null {
        if let Some(event_obj) = event_value.get_mut("event").and_then(|e| e.as_object_mut()) {
            event_obj.insert(
                "id".to_string(),
                serde_json::json!(uuid::Uuid::now_v7().to_string()),
            );
        }
    }
}

/// Ensures the configured topic exists.
///
/// When `create_or_update` is `true` the topic is created with default
/// settings (1 partition, replication factor 1) if it does not already
/// exist.  When `false` an error is returned if the topic is absent from
/// the cluster.
async fn setup_topic(
    producer: &rdkafka::producer::FutureProducer,
    credentials_path: &Option<std::path::PathBuf>,
    brokers: &str,
    topic: &str,
    create_or_update: bool,
) -> Result<(), Error> {
    let metadata = producer
        .client()
        .fetch_metadata(Some(topic), std::time::Duration::from_secs(10))
        .map_err(|e| Error::MetadataFetch { source: e })?;

    let exists = metadata
        .topics()
        .iter()
        .any(|t| t.name() == topic && t.error().is_none());

    if exists {
        return Ok(());
    }

    if create_or_update {
        let config = crate::client::build_base_config(credentials_path, brokers)
            .map_err(|e| Error::ClientAuth { source: e })?;
        let admin_client: AdminClient<DefaultClientContext> =
            config.create().map_err(|e| Error::ClientAuth {
                source: crate::client::Error::CreateAdminClient { source: e },
            })?;

        let new_topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1));
        admin_client
            .create_topics(&[new_topic], &AdminOptions::new())
            .await
            .map_err(|e| Error::TopicCreation {
                topic: topic.to_string(),
                source: e,
            })?;
        Ok(())
    } else {
        Err(Error::TopicNotFound {
            topic: topic.to_string(),
        })
    }
}

pub struct EventHandler {
    producer: Arc<rdkafka::producer::FutureProducer>,
    task_id: usize,
    tx: Option<Sender<Event>>,
    config: Arc<super::config::Produce>,
    task_type: &'static str,
}

impl EventHandler {
    #[tracing::instrument(skip(self, event), name = "task.handle", fields(duration_ms = tracing::field::Empty))]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();

        flowgen_core::event::with_event_context(&Arc::clone(&event), async move {
            let mut event_value = serde_json::value::Value::try_from(event.as_ref())
                .map_err(|source| Error::EventBuilder { source })?;

            // Incoming events do not always carry an id (e.g. NATS messages
            // without a Nats-Msg-Id header, generate tasks). Patch a UUID v7
            // fallback into the render context so templates like
            // "{{event.id}}" always resolve to a value.
            ensure_event_id(&mut event_value);

            let config = self
                .config
                .render(&event_value)
                .map_err(|source| Error::ConfigRender { source })?;

            let payload = serialize_event_to_bytes(event.as_ref())?;

            let message_key = match &config.message_key {
                Some(key_template) => {
                    let rendered =
                        flowgen_core::config::render_template(key_template, &event_value)
                            .map_err(|source| Error::ConfigRender { source })?;
                    Some(rendered)
                }
                None => None,
            };

            let record = FutureRecord::to(&config.topic)
                .payload(&payload)
                .key(message_key.as_deref().unwrap_or(""));

            let (partition, offset) = self
                .producer
                .send(record, Duration::from_secs(30))
                .await
                .map_err(|(e, _)| Error::Produce { source: e })?;

            let result = ProduceResult {
                topic: config.topic.clone(),
                partition,
                offset,
            };

            let result_json =
                serde_json::to_value(&result).map_err(|e| Error::SerdeJson { source: e })?;

            let mut e = EventBuilder::new()
                .subject(self.config.name.clone())
                .data(EventData::Json(result_json))
                .task_id(self.task_id)
                .task_type(self.task_type)
                .build()
                .map_err(|source| Error::EventBuilder { source })?;

            match self.tx {
                None => {
                    if let Some(arc) = completion_tx_arc.as_ref() {
                        arc.signal_completion(e.data_as_json().ok());
                    }
                }
                Some(_) => {
                    e.completion_tx = completion_tx_arc.clone();
                }
            }

            e.send_with_logging(self.tx.as_ref())
                .context("topic", &config.topic)
                .context("partition", partition)
                .context("offset", offset)
                .await
                .map_err(|source| Error::SendMessage { source })?;

            Ok(())
        })
        .await
    }
}

#[derive(Debug)]
pub struct Producer {
    config: Arc<super::config::Produce>,
    rx: Receiver<Event>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    task_type: &'static str,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Producer {
    type Error = Error;
    type EventHandler = EventHandler;

    async fn init(&self) -> Result<EventHandler, Error> {
        let init_config = self
            .config
            .render(&serde_json::json!({}))
            .map_err(|source| Error::ConfigRender { source })?;

        let kafka_key = flowgen_core::client_registry::ClientKeyBuilder::new(self.task_type)
            .field("credentials_path", &init_config.credentials_path)
            .field("brokers", &init_config.brokers)
            .build();
        let producer = self
            .task_context
            .client_registry
            .get_or_init(kafka_key, || {
                let credentials_path = init_config.credentials_path.clone();
                let brokers = init_config.brokers.clone();
                async move {
                    let client = crate::client::Client::new(credentials_path, Some(brokers));
                    client
                        .connect()
                        .await
                        .map_err(|source| Error::ClientAuth { source })?
                        .producer
                        .ok_or(Error::MissingClient)
                }
            })
            .await
            .map_err(|e| match e {
                flowgen_core::client_registry::Error::Init { source } => source,
                flowgen_core::client_registry::Error::TypeMismatch => Error::ClientRegistryMismatch,
            })?;

        setup_topic(
            &producer,
            &init_config.credentials_path,
            &init_config.brokers,
            &init_config.topic,
            init_config.create_or_update,
        )
        .await?;

        let event_handler = EventHandler {
            producer: Arc::clone(&producer),
            task_id: self.task_id,
            tx: self.tx.clone(),
            config: Arc::clone(&self.config),
            task_type: self.task_type,
        };

        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Self::Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(
            retry_config.init_strategy(self.task_context.startup_delay),
            || async {
                match self.init().await {
                    Ok(handler) => Ok(handler),
                    Err(e) => {
                        let is_retriable =
                            !matches!(&e, Error::ConfigRender { .. } | Error::MissingClient);

                        if is_retriable {
                            error!(error = %e, "Failed to initialize kafka producer");
                            Err(tokio_retry::RetryError::transient(e))
                        } else {
                            error!(error = %e, "Non-retriable error");
                            Err(tokio_retry::RetryError::permanent(e))
                        }
                    }
                }
            },
        )
        .await
        {
            Ok(handler) => Arc::new(handler),
            Err(e) => {
                return Err(e);
            }
        };

        let mut handlers = Vec::new();

        loop {
            if self.task_context.cancellation_token.is_cancelled() {
                future::join_all(handlers).await;
                return Ok(());
            }

            match self.rx.recv().await {
                Some(event) => {
                    let event_handler = Arc::clone(&event_handler);
                    let retry_strategy = retry_config.strategy();
                    let handle = tokio::spawn(
                        async move {
                            let result = tokio_retry::Retry::spawn(retry_strategy, || async {
                                match event_handler.handle(event.clone()).await {
                                    Ok(result) => Ok(result),
                                    Err(e) => {
                                        error!(error = %e, "Failed to produce message");
                                        Err(tokio_retry::RetryError::transient(e))
                                    }
                                }
                            })
                            .await;

                            if let Err(err) = result {
                                error!(error = %err, "Failed to produce message after all retry attempts");
                                let mut error_event = event.clone();
                                error_event.error = Some(err.to_string());
                                if let Some(ref tx) = event_handler.tx {
                                    tx.send(error_event).await.ok();
                                } else if let Some(arc) = event.completion_tx.as_ref() {
                                    arc.signal_completion_with_error(err.to_string());
                                }
                            }
                        }
                        .instrument(tracing::Span::current()),
                    );
                    handlers.push(handle);
                }
                None => {
                    future::join_all(handlers).await;
                    return Ok(());
                }
            }
        }
    }
}

#[derive(Default)]
pub struct ProducerBuilder {
    config: Option<Arc<super::config::Produce>>,
    rx: Option<Receiver<Event>>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    task_type: Option<&'static str>,
}

impl ProducerBuilder {
    pub fn new() -> ProducerBuilder {
        ProducerBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::Produce>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    pub fn sender(mut self, sender: Sender<Event>) -> Self {
        self.tx = Some(sender);
        self
    }

    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = task_id;
        self
    }

    pub fn task_context(
        mut self,
        task_context: Arc<flowgen_core::task::context::TaskContext>,
    ) -> Self {
        self.task_context = Some(task_context);
        self
    }

    pub fn task_type(mut self, task_type: &'static str) -> Self {
        self.task_type = Some(task_type);
        self
    }

    pub async fn build(self) -> Result<Producer, Error> {
        Ok(Producer {
            config: self
                .config
                .ok_or_else(|| Error::MissingBuilderAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingBuilderAttribute("receiver".to_string()))?,
            tx: self.tx,
            task_id: self.task_id,
            task_context: self
                .task_context
                .ok_or_else(|| Error::MissingBuilderAttribute("task_context".to_string()))?,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingBuilderAttribute("task_type".to_string()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use serde_json::{Map, Value};
    use std::sync::Arc as StdArc;
    use tokio::sync::mpsc;

    fn create_mock_task_context() -> Arc<flowgen_core::task::context::TaskContext> {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Producer Test".to_string()),
        );
        let task_manager = Arc::new(
            flowgen_core::task::manager::TaskManagerBuilder::new()
                .build()
                .unwrap(),
        );
        let cache = Arc::new(flowgen_core::cache::memory::MemoryCache::new())
            as Arc<dyn flowgen_core::cache::Cache>;
        Arc::new(
            flowgen_core::task::context::TaskContextBuilder::new()
                .flow_name("test-flow".to_string())
                .flow_labels(Some(labels))
                .task_manager(task_manager)
                .cache(cache)
                .build()
                .unwrap(),
        )
    }

    // ------------------------------------------------------------------
    // Error display
    // ------------------------------------------------------------------

    #[test]
    fn test_error_display() {
        assert_eq!(
            Error::TopicNotFound { topic: "x".into() }.to_string(),
            "Topic 'x' does not exist on the Kafka cluster"
        );
        assert_eq!(
            Error::MissingClient.to_string(),
            "Client is missing or not initialized"
        );
        assert_eq!(
            Error::MissingBuilderAttribute("foo".into()).to_string(),
            "Missing required builder attribute: foo"
        );
        assert_eq!(
            Error::ClientRegistryMismatch.to_string(),
            "Client registry type mismatch -- same credentials used with incompatible client types"
        );
    }

    // ------------------------------------------------------------------
    // ProduceResult round-trip
    // ------------------------------------------------------------------

    #[test]
    fn test_produce_result_round_trip() {
        let r = ProduceResult {
            topic: "t".into(),
            partition: 2,
            offset: 99,
        };
        let json = serde_json::to_value(&r).unwrap();
        let back: ProduceResult = serde_json::from_value(json).unwrap();
        assert_eq!(back.topic, "t");
        assert_eq!(back.partition, 2);
        assert_eq!(back.offset, 99);
    }

    // ------------------------------------------------------------------
    // serialize_event_to_bytes
    // ------------------------------------------------------------------

    #[test]
    fn test_serialize_json() {
        let event = Event {
            data: EventData::Json(serde_json::json!({"hello": "world"})),
            subject: "s".into(),
            id: None,
            timestamp: 0,
            task_id: 0,
            task_type: "",
            meta: None,
            error: None,
            completion_tx: None,
        };
        let bytes = serialize_event_to_bytes(&event).unwrap();
        let parsed: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed, serde_json::json!({"hello": "world"}));
    }

    #[test]
    fn test_serialize_bytes() {
        let event = Event {
            data: EventData::Bytes(bytes::Bytes::from(&b"raw data"[..])),
            subject: "s".into(),
            id: None,
            timestamp: 0,
            task_id: 0,
            task_type: "",
            meta: None,
            error: None,
            completion_tx: None,
        };
        let bytes = serialize_event_to_bytes(&event).unwrap();
        assert_eq!(bytes, b"raw data");
    }

    #[test]
    fn test_serialize_arrow() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let batch = RecordBatch::try_new(
            StdArc::new(schema),
            vec![StdArc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let event = Event {
            data: EventData::ArrowRecordBatch(batch),
            subject: "s".into(),
            id: None,
            timestamp: 0,
            task_id: 0,
            task_type: "",
            meta: None,
            error: None,
            completion_tx: None,
        };
        let bytes = serialize_event_to_bytes(&event).unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_serialize_avro() {
        let data = EventData::Avro(flowgen_core::event::AvroData {
            schema: r#"{"type":"record","name":"r","fields":[{"name":"x","type":"int"}]}"#.into(),
            raw_bytes: vec![0x01],
        });
        let event = Event {
            data,
            subject: "s".into(),
            id: None,
            timestamp: 0,
            task_id: 0,
            task_type: "",
            meta: None,
            error: None,
            completion_tx: None,
        };
        let bytes = serialize_event_to_bytes(&event).unwrap();
        assert_eq!(bytes, vec![0x01]);
    }

    // ------------------------------------------------------------------
    // ProducerBuilder
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_producer_builder_success() {
        let config = Arc::new(super::super::config::Produce {
            name: "test_kafka_producer".to_string(),
            brokers: "localhost:9092".to_string(),
            topic: "test-topic".to_string(),
            ..Default::default()
        });
        let (tx, rx) = mpsc::channel(100);

        let producer = ProducerBuilder::new()
            .config(config.clone())
            .receiver(rx)
            .sender(tx.clone())
            .task_id(1)
            .task_type("test")
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(producer.is_ok());

        let p = producer.unwrap();
        assert_eq!(p.config.name, "test_kafka_producer");
        assert!(p.tx.is_some());
    }

    #[tokio::test]
    async fn test_producer_builder_without_sender() {
        let config = Arc::new(super::super::config::Produce {
            name: "leaf".to_string(),
            brokers: "localhost:9092".to_string(),
            topic: "leaf-topic".to_string(),
            ..Default::default()
        });
        let (_tx, rx) = mpsc::channel(100);
        let producer = ProducerBuilder::new()
            .config(config)
            .receiver(rx)
            .task_id(2)
            .task_type("leaf")
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(producer.is_ok());
        assert!(producer.unwrap().tx.is_none());
    }

    #[tokio::test]
    async fn test_producer_builder_missing_each_field() {
        let full_config = || {
            Arc::new(super::super::config::Produce {
                name: "t".into(),
                brokers: "b:9092".into(),
                topic: "t".into(),
                ..Default::default()
            })
        };
        let ctx = create_mock_task_context();

        // Missing config
        let (_, rx) = mpsc::channel(10);
        let e = ProducerBuilder::new()
            .receiver(rx)
            .task_context(ctx.clone())
            .build()
            .await
            .unwrap_err();
        assert!(matches!(e, Error::MissingBuilderAttribute(attr) if attr == "config"));

        // Missing receiver
        let (tx, _) = mpsc::channel(10);
        let e = ProducerBuilder::new()
            .config(full_config())
            .sender(tx)
            .task_context(ctx.clone())
            .build()
            .await
            .unwrap_err();
        assert!(matches!(e, Error::MissingBuilderAttribute(attr) if attr == "receiver"));

        // Missing task_context
        let (_, rx) = mpsc::channel(10);
        let e = ProducerBuilder::new()
            .config(full_config())
            .receiver(rx)
            .build()
            .await
            .unwrap_err();
        assert!(matches!(e, Error::MissingBuilderAttribute(attr) if attr == "task_context"));
    }

    // ------------------------------------------------------------------
    // Config create_or_update field
    // ------------------------------------------------------------------

    #[test]
    fn test_config_create_or_update_default() {
        let config = super::super::config::Produce::default();
        assert!(!config.create_or_update);
    }

    #[test]
    fn test_config_create_or_update_round_trip() {
        let config = super::super::config::Produce {
            name: "test".into(),
            brokers: "b:9092".into(),
            topic: "t".into(),
            create_or_update: true,
            ..Default::default()
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: super::super::config::Produce = serde_json::from_str(&json).unwrap();
        assert!(deserialized.create_or_update);
    }

    // ------------------------------------------------------------------
    // message_key id fallback
    // ------------------------------------------------------------------

    #[test]
    fn test_ensure_event_id_patches_null_id() {
        let mut event_value = serde_json::json!({
            "event": { "id": null, "subject": "s", "data": 42 }
        });
        ensure_event_id(&mut event_value);
        let id = event_value["event"]["id"].as_str().unwrap();
        assert!(!id.is_empty());
        uuid::Uuid::parse_str(id).is_ok().then_some(()).unwrap();
    }

    #[test]
    fn test_ensure_event_id_preserves_existing_id() {
        let mut event_value = serde_json::json!({
            "event": { "id": "existing-id", "subject": "s" }
        });
        ensure_event_id(&mut event_value);
        assert_eq!(event_value["event"]["id"], "existing-id");
    }

    #[test]
    fn test_message_key_template_resolves_fallback_id() {
        let mut event_value = serde_json::json!({
            "event": { "id": null, "subject": "s", "data": 42 }
        });
        ensure_event_id(&mut event_value);
        let rendered =
            flowgen_core::config::render_template("key-{{event.id}}", &event_value).unwrap();
        let id = event_value["event"]["id"].as_str().unwrap();
        assert_eq!(rendered, format!("key-{id}"));
        assert_ne!(rendered, "key-");
    }
}
