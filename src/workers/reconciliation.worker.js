import { pool } from '../db/pool.js';
import { getProviderStatus } from '../providers/provider.factory.js';
import { config } from '../config/index.js';

const INTERVAL_MS = config.workers.reconciliationIntervalMs;

async function reconcile() {
    console.log('[Reconciliation] Starting reconciliation cycle');
    const client = await pool.connect();

    try {
        // Find attempts stuck in 'processing' for more than 2 minutes
        const { rows } = await client.query(
            `SELECT * FROM notification_attempts
             WHERE status = 'processing'
             AND updated_at < NOW() - INTERVAL '2 minutes'
             ORDER BY updated_at ASC
             LIMIT 50`
        );

        if (rows.length === 0) {
            console.log('[Reconciliation] Nothing to reconcile');
            return;
        }

        console.log(`[Reconciliation] Found ${rows.length} ambiguous attempts`);

        for (const row of rows) {
            try {
                const status = await getProviderStatus(row.channel, row.provider, row.provider_message_id);

                if (!status) {
                    console.warn(`[Reconciliation] Provider status unavailable | attempt: ${row.id}`);
                    await client.query(
                        `UPDATE notification_attempts SET reconciled_at = NOW() WHERE id = $1`,
                        [row.id]
                    );
                    continue;
                }

                if (status === 'delivered' || status === 'sent') {
                    await client.query(
                        `UPDATE notification_attempts
                         SET status = 'delivered', successful_at = NOW(), reconciled_at = NOW()
                         WHERE id = $1`,
                        [row.id]
                    );
                    console.log(`[Reconciliation] Confirmed delivery | attempt: ${row.id} | provider: ${row.provider}`);

                } else if (status === 'failed' || status === 'undelivered') {
                    await client.query(
                        `UPDATE notification_attempts
                         SET status = $1, failed_at = NOW(), reconciled_at = NOW()
                         WHERE id = $2`,
                        [row.attempts < row.max_attempts ? 'pending' : 'failed', row.id]
                    );
                    console.log(`[Reconciliation] Confirmed failure | attempt: ${row.id} | resetting to ${row.attempts < row.max_attempts ? 'pending' : 'failed'}`);

                } else {
                    // Still in transit - check next cycle
                    await client.query(
                        `UPDATE notification_attempts SET reconciled_at = NOW() WHERE id = $1`,
                        [row.id]
                    );
                    console.log(`[Reconciliation] Still in transit (${status}) | attempt: ${row.id}`);
                }

            } catch (err) {
                console.error(`[Reconciliation] Error reconciling attempt ${row.id}:`, err.message);
            }
        }

    } catch (err) {
        console.error('[Reconciliation] Cycle failed:', err.message);
    } finally {
        client.release();
    }
}

export function startReconciliationWorker() {
    console.log(`[Reconciliation] Starting — interval: ${INTERVAL_MS}ms`);
    const interval = setInterval(reconcile, INTERVAL_MS);

    process.on('SIGTERM', () => {
        clearInterval(interval);
        console.log('[Reconciliation] Shut down');
    });
}