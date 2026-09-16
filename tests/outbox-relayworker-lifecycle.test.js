// tests/outbox-relayworker-lifecycle.test.js
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { producer } from '../src/kafka/producer.js';
import { pool } from '../src/db/pool.js';
import { startOutboxRelayWorker } from '../src/workers/outbox-relay.worker.js';

test('startOutboxRelayWorker connects producer, polls, and shuts down cleanly on SIGTERM', async (t) => {
    let connectCalled = false;
    let disconnectCalled = false;
    let notifyPollStarted;
    const pollStarted = new Promise((resolve) => { notifyPollStarted = resolve; });

    let finishPoll;
    const pollQueryPending = new Promise((resolve) => { finishPoll = resolve; });

    const origConnect = producer.connect;
    const origDisconnect = producer.disconnect;
    const origPoolConnect = pool.connect;

    t.after(() => {
        producer.connect = origConnect;
        producer.disconnect = origDisconnect;
        pool.connect = origPoolConnect;
    });

    producer.connect = async () => { connectCalled = true; };
    producer.disconnect = async () => { disconnectCalled = true; };

    // 2. Mock Pool Client (returns 0 rows so pollAndRelay returns quickly)
    const mockClient = {
        async query(sql) {
            if (sql.includes('SELECT id, payload FROM outbox')) {
                notifyPollStarted();
                await pollQueryPending; // Wait until we allow it to finish
                return { rows: [] };
            }
            return { rows: [] };
        },
        release() {}
    };

    pool.connect = async () => mockClient;

    // Start worker
    const worker = await startOutboxRelayWorker();

    // Verify producer.connect() was called by connectProducer()
    assert.equal(connectCalled, true, 'Expected producer.connect to be called on startup');

    // Wait a short duration (e.g., 12000ms) to let timer trigger at least one poll
    await pollStarted;
    const shutdownPromise = worker.stop();

    assert.equal(disconnectCalled, false, 'Should not disconnect producer before poll finishes');

    finishPoll(); // Allow the poll to complete
    await shutdownPromise;

    assert.equal(disconnectCalled, true, 'Expected producer.disconnect to be called after poll finishes and SIGTERM is handled');

});