use flowgen_core::client::Client;
use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use futures_util::future;
use rdkafka::producer::FutureRecord;
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
            let event_value = serde_json::value::Value::try_from(event.as_ref())
                .map_err(|source| Error::EventBuilder { source })?;
            let config = self
                .config
                .render(&event_value)
                .map_err(|source| Error::ConfigRender { source })?;

            let payload = serialize_event_to_bytes(event.as_ref())?;

            let message_key = match &config.message_key {
                Some(key_template) => {
                    let rendered = flowgen_core::config::render_template(key_template, &event_value)
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

        let kafka_key = flowgen_core::client_registry::ClientKey::new(&(
            &init_config.credentials_path,
            &init_config.brokers,
        ));
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
                flowgen_core::client_registry::Error::TypeMismatch => {
                    Error::ClientRegistryMismatch
                }
            })?;

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
    use serde_json::{Map, Value};
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

    #[tokio::test]
    async fn test_producer_builder() {
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

        let (_tx2, rx2) = mpsc::channel(100);
        let result = ProducerBuilder::new()
            .receiver(rx2)
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(_)
        ));
    }
}
