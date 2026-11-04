import dotenv from 'dotenv';
dotenv.config();

function required(key) {
    const value = process.env[key];
    if (!value) throw new Error (`Missing required env var ${key}`);
    return value;
}

function optional(key, fallback) {
    return process.env[key] ?? fallback;
}

export const config = {
    databaseUrl: required('DATABASE_URL'),

    // Kafka envs
    kafka: {
    brokers: optional('KAFKA_BROKERS', 'localhost:9092').split(','),
    clientId: optional('KAFKA_CLIENT_ID', 'notification-service'),
    topic: optional('KAFKA_TOPIC', 'notifications'),
    groupId: optional('KAFKA_GROUP_ID', 'notification-delivery-group'),
  },

    // redis url
    redisUrl: optional('REDIS_URL','redis://localhost:6379'),

    // grpc port
    grpcPort: optional('GRPC_PORT', '50051'),

    // message providers depending on channel
    providers: {
        email: {    
            sendbyte: {
            apiKey: optional('SENDBYTE_API_KEY', ''),
            fromEmail: optional('SENDBYTE_FROM_EMAIL', 'no-reply@example.com'),
            },
        },
        sms: {    
            twilio: {
            accountSid: optional('TWILIO_ACCOUNT_SID', ''),
            authToken: optional('TWILIO_AUTH_TOKEN', ''),
            fromNumber: optional('TWILIO_FROM_NUMBER', ''),
            },
        }
    },

    // redis breaker
    circuitBreaker: {
        failureThreshold: parseInt(optional('CB_FAILURE_THRESHOLD', '5')),
        cooldownSeconds: parseInt(optional('CB_COOLDOWN_SECONDS', '60')),
        probeLockMs: parseInt(optional('CB_PROBE_LOCK_MS', '10000')),
    },

    // redis workers
    workers: {
        outboxPollIntervalMs: parseInt(optional('OUTBOX_POLL_INTERVAL_MS', '1000')),
        outboxBatchSize: parseInt(optional('OUTBOX_BATCH_SIZE', '10')),
        reconciliationIntervalMs: parseInt(optional('RECONCILIATION_INTERVAL_MS', '120000')),
    },
};