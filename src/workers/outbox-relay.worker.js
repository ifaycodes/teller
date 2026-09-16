import { pool } from "../db/pool.js";
import { connectProducer, publish, disconnectProducer } from "../kafka/producer.js";
import { config } from "../config/index.js";

const BATCH_SIZE = config.workers.outboxBatchSize;
const POLL_INTERVAL = config.workers.outboxPollIntervalMs;

async function pollAndRelay() {
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

    const interval = setInterval(pollAndRelay, POLL_INTERVAL);

    process.on('SIGTERM', async () => {
        clearInterval(interval);
        await disconnectProducer();
        console.log(`[OutboxRelay] Shut down gracefully`);
    });
}