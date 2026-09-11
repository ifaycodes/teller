# Teller — Notification Service

A multi-channel notification service built with Node.js. Accepts delivery requests over gRPC and handles routing, retries, and delivery tracking internally.

## How it works

1. A gRPC request arrives with a notification payload and target channels
2. The handler writes atomically to three tables — `notifications`, `notification_attempts`, and `outbox`
3. The outbox relay worker picks up unpublished rows and publishes them to Kafka
4. The delivery worker consumes from Kafka and dispatches through the appropriate provider
5. The circuit breaker tracks provider health in Redis and routes around failing providers
6. The reconciliation worker resolves any attempts stuck mid-flight (e.g. worker crash post-send)

## Stack

- **Transport** — gRPC
- **Queue** — Kafka
- **Database** — PostgreSQL
- **Cache / Circuit Breaker** — Redis
- **Email** — Sendbyte
- **SMS** — Twilio

## Getting started

```bash
# Copy env and fill in values
cp .env.example .env

# Start infrastructure
docker compose up -d

# Run in development
npm run dev
```

## Environment variables

| Variable | Description | Default |
|---|---|---|
| `DATABASE_URL` | PostgreSQL connection string | required |
| `KAFKA_BROKERS` | Comma-separated broker addresses | `localhost:9094` |
| `KAFKA_TOPIC` | Topic name | `notifications` |
| `KAFKA_GROUP_ID` | Consumer group ID | `notification-delivery-group` |
| `REDIS_URL` | Redis connection string | `redis://localhost:6379` |
| `GRPC_PORT` | Port the gRPC server binds to | `50051` |
| `SENDBYTE_API_KEY` | Sendbyte API key | — |
| `TWILIO_ACCOUNT_SID` | Twilio account SID | — |
| `TWILIO_AUTH_TOKEN` | Twilio auth token | — |
| `TWILIO_FROM_NUMBER` | Twilio sender number | — |
| `OUTBOX_POLL_INTERVAL_MS` | How often the relay worker polls | `1000` |
| `OUTBOX_BATCH_SIZE` | Rows per relay poll | `10` |
| `RECONCILIATION_INTERVAL_MS` | How often reconciliation runs | `120000` |
| `CB_FAILURE_THRESHOLD` | Failures before circuit opens | `5` |
| `CB_COOLDOWN_SECONDS` | Seconds before half-open probe | `60` |

## gRPC

Proto file: `src/proto/notifications.proto`

**SendNotification** - Send through GRPC using Postman
```json
{
  "user_id": "uuid",
  "title": "Hello",
  "message": "Your order is confirmed",
  "html_content": "<p>Your order is confirmed</p>",
  "channels": [
    { "channel": "email", "recipient": "user@example.com" },
    { "channel": "sms", "recipient": "+1234567890" }
  ]
}
```

## Project structure

```
src/
├── grpc/           # Server and request handler
├── db/             # Migrations and repositories
├── kafka/          # Producer and consumer
├── workers/        # Outbox relay, delivery, reconciliation
├── providers/      # Sendbyte, Twilio — implement NotificationProvider
├── circuit-breaker/
└── config/
```
