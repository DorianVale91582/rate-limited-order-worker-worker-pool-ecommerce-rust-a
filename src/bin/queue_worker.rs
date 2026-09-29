use rate_limited_order_worker::{apply_order_job, InfraiQueue, OrderJob, RateGate, WorkerError};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

#[tokio::main]
async fn main() -> Result<(), WorkerError> {
    let queue_name = std::env::var("ORDER_QUEUE").unwrap_or_else(|_| "ecommerce-orders".into());
    let queue = InfraiQueue::from_env(queue_name)?;
    let messages = queue.consume(8, 60).await?;
    let concurrency = Arc::new(Semaphore::new(4));
    let rate = Arc::new(RateGate::per_second(5));
    let mut tasks = JoinSet::new();

    for message in messages {
        let permit = concurrency
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore open");
        let rate = rate.clone();
        let queue = queue.clone();
        tasks.spawn(async move {
            let _permit = permit;
            let job: OrderJob = serde_json::from_value(message.payload)?;
            rate.acquire().await;
            let update = apply_order_job(job);
            println!("{}", serde_json::to_string(&update)?);
            queue.ack(&message.message_id).await?;
            Ok::<_, WorkerError>(())
        });
    }

    while let Some(result) = tasks.join_next().await {
        result??;
    }
    Ok(())
}
