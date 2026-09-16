import { test } from 'node:test';
import assert from 'node:assert/strict';
import { pool } from '../src/db/pool.js';
import { producer } from '../src/kafka/producer.js';
import { pollAndRelay } from '../src/workers/outbox-relay.worker.js';

test('pollAndRelay publishes outbox row and updates published_at on success', async () => {
    const publishedMessages = [];
    const queriesExecuted = [];

    // Mock producer.send
    const originalSend = producer.send;
    producer.send = async (args) => {
        publishedMessages.push(args);
        return [{ topicName: args.topic, partition: 0, errorCode: 0 }];
    };

    // Mock Pool Client
    const mockClient = {
        async query(sql, params) {
            queriesExecuted.push({ sql, params });

            if (sql.includes('SELECT id, payload FROM outbox')) {
                return {
                    rows: [{ id: 'outbox_1', payload: { notification_id: 'notif_123' } }]
                };
            }
            if (sql.includes('FOR UPDATE SKIP LOCKED')) {
                return { rows: [{ id: 'outbox_1' }] };
            }
            return { rows: [], rowCount: 1 };
        },
        release() {}
    };

    const originalConnect = pool.connect;
    pool.connect = async () => mockClient;

    try {
        await pollAndRelay();

        // 1. Verify Kafka send was called
        assert.equal(publishedMessages.length, 1);
        assert.equal(publishedMessages[0].messages[0].key, 'notif_123');

        // 2. Verify DB UPDATE query was called
        const updateQuery = queriesExecuted.find(q => q.sql.includes('UPDATE outbox SET published_at'));
        assert.ok(updateQuery, 'Expected UPDATE query to mark row as published');
        assert.equal(updateQuery.params[0], 'outbox_1');
    } finally {
        producer.send = originalSend;
        pool.connect = originalConnect;
    }
});