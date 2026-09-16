// tests/outbox-relayworker-lifecycle.test.js
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { producer } from '../src/kafka/producer.js';
import { pool } from '../src/db/pool.js';
import { startOutboxRelayWorker } from '../src/workers/outbox-relay.worker.js';

test('startOutboxRelayWorker connects producer, polls, and shuts down cleanly on SIGTERM', async () => {
    let connectCalled = false;
    let disconnectCalled = false;
    let pollCount = 0;

    // 1. Stub connect and disconnect on the exported producer instance
    const origConnect = producer.connect;
    const origDisconnect = producer.disconnect;

    producer.connect = async () => { connectCalled = true; };
    producer.disconnect = async () => { disconnectCalled = true; };

    // 2. Mock Pool Client (returns 0 rows so pollAndRelay returns quickly)
    const mockClient = {
        async query(sql) {
            if (sql.includes('SELECT id, payload FROM outbox')) {
                pollCount++;
                return { rows: [] };
            }
            return { rows: [] };
        },
        release() {}
    };

    const origPoolConnect = pool.connect;
    pool.connect = async () => mockClient;

    try {
        // Start worker
        startOutboxRelayWorker();

        // Verify producer.connect() was called by connectProducer()
        assert.equal(connectCalled, true, 'Expected producer.connect to be called on startup');

        // Wait a short duration (e.g., 12000ms) to let timer trigger at least one poll
        await new Promise((r) => setTimeout(r, 12000));
        assert.ok(pollCount > 0, 'Expected at least one poll cycle to run');

        // Trigger SIGTERM signal programmatically to test graceful shutdown
        process.emit('SIGTERM');

        // Wait for graceful shutdown completion
        await new Promise((r) => setTimeout(r, 1000));

        assert.equal(disconnectCalled, true, 'Expected producer.disconnect to be called on SIGTERM');
    } finally {
        // Restore original methods
        producer.connect = origConnect;
        producer.disconnect = origDisconnect;
        pool.connect = origPoolConnect;
    }
});