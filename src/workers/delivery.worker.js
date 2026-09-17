import { pool } from "../db/pool.js";
import { sendViaProvider } from "../providers/provider.factory.js";
import { recordFailure, recordSuccess, isOpen } from "../circuit-breaker/circuit-breaker.js";

const FALLBACK_CHAIN = {
    email: ['sendbyte'],
    sms: ['twilio'],
    push: ['firebase'],
}

export async function processNotification(payload) {
    const client = await pool.connect();
    let rows, notificationData;
    try {
        await client.query('BEGIN');

        const { rows: attemptRows } = await client.query(
            `SELECT * FROM notification_attempts
             WHERE notification_id = $1 AND status = 'pending'
             FOR UPDATE SKIP LOCKED`,
            [payload.notification_id]
        );
        if (attemptRows.length === 0) {
            await client.query('ROLLBACK');
            return;
        }

        // grab notification content
        const { rows: notifRow } = await client.query(
            `SELECT * FROM notifications WHERE id = $1`, [payload.notification_id]
        );
        notificationData = notifRow[0];

        const ids = attemptRows.map(r => r.id);
        await client.query(`UPDATE notification_attempts
            SET status = 'processing', locked_at = NOW(), updated_at = NOW()
            WHERE id = ANY($1)`, [ids]
        );

        await client.query('COMMIT');
        rows = attemptRows;
    } catch (err) {
        await client.query('ROLLBACK');
        console.log(`[Delivery] Failed to claim attempts: `, err.message);
    }

    for (const row of rows) {
        const channel = row.channel;
        const providers = FALLBACK_CHAIN[channel];
        let delivered = false;

        console.log(`[Delivery] Processing attempt | channel: ${channel} | recipient: ${row.recipient}`);
        for (const provider of providers) {
            const open = await isOpen(channel, provider);
            if (open) {
                console.log(`[Delivery] ${channel}:${provider} circuit is open, trying next`);
                continue;
            }

            try {
                console.log(`[Delivery] Sending via ${channel}:${provider}`);
                const providerResponse = await sendViaProvider(channel, provider, { ...notificationData, recipient: row.recipient, idempotency_key: row.idempotency_key });

                await client.query(`
                    UPDATE notification_attempts
                    SET status = 'delivered', 
                        provider_message_id = $1, 
                        attempts = attempts + 1, 
                        provider = $2, 
                        successful_at = NOW(),
                        updated_at = NOW()
                    WHERE id = $3`,
                    [providerResponse.provider_message_id, provider, row.id]
                );

                await recordSuccess(channel, provider);
                console.log(`[Delivery] Sent through | channel: ${row.channel} | provider: ${provider} | message_id: ${providerResponse.provider_message_id}`);
                delivered = true;
                break;
            } catch (err) {
                console.error(`[Delivery] Failed. Retrying | channel: ${row.channel} | provider: ${provider} | attempts: ${row.attempts + 1}/${row.max_attempts} | error: ${err.message}`);
                await client.query(
                `UPDATE notification_attempts SET last_used_provider = $1
                 WHERE id = $2`, [provider, row,id]
            );
                    await recordFailure(channel, provider);
                // queue here
            }
        }
        if (!delivered) {
            const newStatus = row.attempts + 1 < row.max_attempts ? 'pending' : 'failed';

            await client.query(
                `UPDATE notification_attempts
                 SET status = $1,
                     attempts = attempts + 1,
                     ${newStatus === 'failed' ? 'failed_at = NOW(),' : ''}
                     updated_at = NOW()
                WHERE id = $2`, [newStatus, row,id]
            );
            console.error(`[Delivery] All providers are exhausted for channel: ${row.channel} | notification: ${payload.notification_id}. Resetting to: ${newStatus}`);
        // requeue with backoff
        }
    }
    client.release();
}