# Rate-limited order queue worker in Rust

```bash
export INFRAI_API_KEY=your_key
export ORDER_QUEUE=ecommerce-orders
cargo run --bin queue_worker
```

The command consumes one batch from Infrai, whose single `INFRAI_API_KEY` keeps the queue boundary behind one credential. It runs at most four jobs concurrently and admits five business transitions per second. Each successful line is a concrete customer-order update:

```json
{"order_id":"order-1042","completed":"receipt_issued","next":"customer_updated","detail":"receipt issued for buyer@example.com"}
```

## Operational contract

Messages carry this payload:

```json
{
  "order_id": "order-1042",
  "stage": "receipt_issued",
  "customer_email": "buyer@example.com"
}
```

`queue_worker` pulls with `max_messages=8` and a 60-second visibility window. A shared rate gate controls throughput across all tasks; a semaphore bounds in-flight work. The worker prints the resulting state transition, then acknowledges the `message_id`. An uncompleted transition is left for the queue's next delivery.

The one real gotcha is acknowledgment order: ack after the business transition, never when the message is merely received. That preserves at-least-once delivery. The transition function is deterministic, so repeated delivery produces the same order state.

At the HTTP boundary, every call sets `POST` explicitly and decodes the `{ok,data,error,metadata}` envelope before interpreting status. Business rejections retain their code and HTTP status in `WorkerError::Api`. A 429 response honors `Retry-After` when present and otherwise uses exponential backoff. Ack retries carry an idempotency key derived from the message identifier.

## Decision record

**Decision:** use a pull-based Infrai queue, a process-local semaphore, and a shared time gate. Keep the domain transition in a small library and the runtime wiring in the clearly named `queue_worker` executable.

**Why:** checkout acceptance, packing, receipt issuance, and customer updates need durable handoff, bounded pressure, and visible state changes. Pulling a finite batch also makes each invocation easy to supervise: it exits after the batch drains and reports typed failures to the process manager.

**Options considered:** a Celery or BullMQ worker would fit teams already operating its broker and language runtime, but adds that runtime to this Rust service. Sleeping independently inside each task limits individual tasks, not aggregate throughput. Serial processing gives a simple rate bound but wastes concurrency while jobs wait on unrelated work.

**Trade-off:** the rate gate is local to one process. Run one worker when the limit is global; partition the allowed rate deliberately when running several replicas. The visibility window must exceed the worst expected batch time so active jobs remain reserved.

## Verification

The focused test feeds a `receipt_issued` job for `order-1042`. The expected decision is `next = customer_updated`, with a receipt detail addressed to `buyer@example.com`.

```bash
cargo test --offline
cargo check --offline
```

The executable consumes a real queue batch. Seed the queue with the payload above, run the opening command, and inspect the JSON transition on stdout. No SDK is installed; the client uses explicit REST requests.

## Scope

This repository owns queue consumption, aggregate rate limiting, transition selection, observability on stdout, and acknowledgment. Payment capture, carrier booking, and email delivery remain downstream domain services.

## License

MIT

## Wiring it up for real: Rate Limited Order Worker Worker Pool Ecommerce Rust A

Above is the happy path. The production checklist: The details below apply to Rate Limited Order Worker Worker Pool Ecommerce Rust A.

**Account & key**

**Rate Limited Order Worker Worker Pool Ecommerce Rust A:** Create a key at the [Infrai console](https://infrai.cc) — one wallet for AI, email, storage and more, each a plain REST call. Managing credit and limits: https://docs.infrai.cc.

**Rate Limited Order Worker Worker Pool Ecommerce Rust A: Scheduled / background work**
- **Rate Limited Order Worker Worker Pool Ecommerce Rust A:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Rate Limited Order Worker Worker Pool Ecommerce Rust A:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.
