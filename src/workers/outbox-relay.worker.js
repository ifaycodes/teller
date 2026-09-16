import { pool } from "../db/pool.js";
import { connectProducer, publish, disconnectProducer } from "../kafka/producer.js";
import { config } from "../config/index.js";

const BATCH_SIZE = config.workers.outboxBatchSize;
const POLL_INTERVAL = config.workers.outboxPollIntervalMs;

export async function pollAndRelay() {
    let rows;
    const scout = await pool.connect();

    try {
        const result = await scout.query(
            `SELECT id, payload FROM outbox WHERE published_at IS NULL
             ORDER BY created_At ASC
             LIMIT $1`,
        [BATCH_SIZE]);

        rows = result.rows;
    } finally {
        scout.release();
    }

    if (rows.length === 0) return;

    console.log(`[OutboxRelay] Processing batch of ${rows.length} messages`);
    const client = await pool.connect();
    try {
        for (const row of rows) {
            try {
                await client.query('BEGIN');
                const { rows: locked } = await client.query(
                    `SELECT id FROM outbox WHERE id = $1 AND published_at IS NULL
                     FOR UPDATE SKIP LOCKED`, [row.id]
                );
                if (locked.length === 0) { await client.query('ROLLBACK'); continue; }
                await publish(row.payload);
                await client.query(
                    `UPDATE outbox SET published_at = NOW() WHERE id = $1`,
                    [row.id]
                );
                await client.query('COMMIT');
            } catch (err) {
                await client.query('ROLLBACK');
                console.error(`[OutboxRelay] Failed on row ${row.id}: `, err.message);
            }
        }
    } catch (err) {
        await client.query('ROLLBACK');
        console.error(`[OutboxRelay] Failed, rolled back: `, err.message);
    } finally {
        client.release();
    }
}

export async function startOutboxRelayWorker() {
    await connectProducer();
    console.log(`[OutboxRelay] Polling every ${POLL_INTERVAL}ms, batch size: ${BATCH_SIZE}`);

    let isRunning = true;
    let isConnecting = true;
    let timerId = null;
    let activePollPromise = null;

    const onSigterm = async () => {
        console.log(`[OutboxRelay] SIGTERM received, initiating shutdown...`);
        await stopWorker();
    };

    const stopWorker = async () => {
        isRunning = false;
        if (timerId) clearTimeout(timerId);

        process.off('SIGTERM', onSigterm);
        if (isConnecting) {
            try { await connectProducer(); } finally {
                isConnecting = false;
            }
        }
        if (activePollPromise) {
            console.log(`[OutboxRelay] Waiting for active poll to finish...`);
            await activePollPromise;
        }
        await disconnectProducer();
        console.log(`[OutboxRelay] Shut down gracefully`);
    };

    process.on('SIGTERM', onSigterm);

    const scheduleNextPoll = () => {
        if (!isRunning) return;
        timerId = setTimeout(async () => {
            activePollPromise = pollAndRelay().catch((err) => {
                console.error(`[OutboxRelay] Unhandled Error during poll: `, err.message);
            });

            await activePollPromise;
            activePollPromise = null;
            scheduleNextPoll();
        }, POLL_INTERVAL);
    };

    scheduleNextPoll();
    
    return { stop: stopWorker };
}