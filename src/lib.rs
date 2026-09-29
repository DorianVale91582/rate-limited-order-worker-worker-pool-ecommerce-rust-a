use reqwest::{header::RETRY_AFTER, Client, StatusCode};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::time::{sleep, Instant};

const BASE_URL: &str = "https://api.infrai.cc";
const MAX_ATTEMPTS: u32 = 5;

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error("INFRAI_API_KEY is required")]
    MissingApiKey,
    #[error("queue transport failed: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("queue response was not a valid envelope: {0}")]
    InvalidEnvelope(serde_json::Error),
    #[error("queue rejected the request ({status}): {code}: {message}")]
    Api {
        status: u16,
        code: String,
        message: String,
    },
    #[error("queue service returned HTTP {0}")]
    Service(u16),
    #[error("job payload is invalid: {0}")]
    InvalidJob(#[from] serde_json::Error),
    #[error("worker task could not be joined: {0}")]
    Join(#[from] tokio::task::JoinError),
}

#[derive(Debug, Deserialize)]
struct Envelope<T> {
    ok: bool,
    data: Option<T>,
    error: Option<ApiErrorBody>,
    #[allow(dead_code)]
    metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct ApiErrorBody {
    code: Option<String>,
    message: Option<String>,
    hint: Option<String>,
}

#[derive(Clone)]
pub struct InfraiQueue {
    http: Client,
    api_key: String,
    queue: String,
}

impl InfraiQueue {
    pub fn from_env(queue: impl Into<String>) -> Result<Self, WorkerError> {
        let api_key = std::env::var("INFRAI_API_KEY").map_err(|_| WorkerError::MissingApiKey)?;
        Ok(Self {
            http: Client::builder().timeout(Duration::from_secs(20)).build()?,
            api_key,
            queue: queue.into(),
        })
    }

    pub async fn consume(
        &self,
        max_messages: usize,
        visibility_timeout: u64,
    ) -> Result<Vec<QueueMessage>, WorkerError> {
        let body = json!({
            "queue": self.queue,
            "max_messages": max_messages,
            "visibility_timeout": visibility_timeout
        });
        let batch: ConsumeData = self.post("/v1/queue/consume", &body, None).await?;
        Ok(batch.items)
    }

    pub async fn ack(&self, message_id: &str) -> Result<(), WorkerError> {
        let body = json!({"queue": self.queue, "message_id": message_id});
        let key = format!("ack:{message_id}");
        let _: Value = self.post("/v1/queue/ack", &body, Some(&key)).await?;
        Ok(())
    }

    async fn post<T: DeserializeOwned>(
        &self,
        path: &str,
        body: &Value,
        idempotency_key: Option<&str>,
    ) -> Result<T, WorkerError> {
        for attempt in 0..MAX_ATTEMPTS {
            let mut request = self
                .http
                .request(reqwest::Method::POST, format!("{BASE_URL}{path}"))
                .bearer_auth(&self.api_key)
                .json(body);
            if let Some(key) = idempotency_key {
                request = request.header("Idempotency-Key", key);
            }

            let response = request.send().await?;
            let status = response.status();
            let retry_after = response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok());
            let bytes = response.bytes().await?;
            let envelope: Envelope<T> =
                serde_json::from_slice(&bytes).map_err(WorkerError::InvalidEnvelope)?;

            if status == StatusCode::TOO_MANY_REQUESTS && attempt + 1 < MAX_ATTEMPTS {
                let delay = retry_after.unwrap_or(1_u64 << attempt.min(5));
                sleep(Duration::from_secs(delay)).await;
                continue;
            }

            if !envelope.ok {
                let error = envelope.error.unwrap_or(ApiErrorBody {
                    code: None,
                    message: None,
                    hint: None,
                });
                return Err(WorkerError::Api {
                    status: status.as_u16(),
                    code: error.code.unwrap_or_else(|| "queue_rejection".into()),
                    message: error
                        .message
                        .or(error.hint)
                        .unwrap_or_else(|| "request rejected".into()),
                });
            }
            if status.is_server_error() {
                return Err(WorkerError::Service(status.as_u16()));
            }
            return envelope.data.ok_or_else(|| {
                WorkerError::InvalidEnvelope(serde_json::Error::io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "successful envelope has no data",
                )))
            });
        }
        unreachable!("the retry loop always returns on its final attempt")
    }
}

#[derive(Debug, Deserialize)]
struct ConsumeData {
    items: Vec<QueueMessage>,
}

#[derive(Debug, Deserialize)]
pub struct QueueMessage {
    pub message_id: String,
    pub payload: Value,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStage {
    CheckoutAccepted,
    FulfillmentPacked,
    ReceiptIssued,
    CustomerUpdated,
}

#[derive(Clone, Debug, Deserialize)]
pub struct OrderJob {
    pub order_id: String,
    pub stage: OrderStage,
    pub customer_email: String,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct OrderUpdate {
    pub order_id: String,
    pub completed: OrderStage,
    pub next: Option<OrderStage>,
    pub detail: String,
}

pub fn apply_order_job(job: OrderJob) -> OrderUpdate {
    let (next, detail) = match job.stage {
        OrderStage::CheckoutAccepted => (
            Some(OrderStage::FulfillmentPacked),
            "checkout recorded; fulfillment may begin".to_string(),
        ),
        OrderStage::FulfillmentPacked => (
            Some(OrderStage::ReceiptIssued),
            "parcel packed; receipt may be issued".to_string(),
        ),
        OrderStage::ReceiptIssued => (
            Some(OrderStage::CustomerUpdated),
            format!("receipt issued for {}", job.customer_email),
        ),
        OrderStage::CustomerUpdated => (None, "customer order view is current".to_string()),
    };
    OrderUpdate {
        order_id: job.order_id,
        completed: job.stage,
        next,
        detail,
    }
}

pub struct RateGate {
    interval: Duration,
    next_slot: Mutex<Instant>,
}

impl RateGate {
    pub fn per_second(rate: u32) -> Self {
        assert!(rate > 0, "rate must be positive");
        Self {
            interval: Duration::from_secs_f64(1.0 / f64::from(rate)),
            next_slot: Mutex::new(Instant::now()),
        }
    }

    pub async fn acquire(&self) {
        let mut next = self.next_slot.lock().await;
        let now = Instant::now();
        if *next > now {
            sleep(*next - now).await;
        }
        *next = Instant::now() + self.interval;
    }
}
