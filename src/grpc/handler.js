import { pool } from '../db/pool.js';
import {createNotificationRepository} from '../db/notification.repository.js';
import {createNotifAttemptsRepository} from '../db/attempt.repository.js';
import {createOutboxRepository} from '../db/outbox.repository.js';

export async function handleSendNotification(input, dbClient = null) {
    /*
    input shape: 
    {
        user_id: String;
        title: String;
        message: String;
        html_content: String;
        channels: [{channel: 'email' | 'sms' | 'push', recipient: string}]
    }
    */

    const client = dbClient || (await pool.connect());
    const shouldRelease = !dbClient;

    try {
        await client.query('BEGIN');

        const notifRepo = createNotificationRepository(client);
        const attemptsRepo = createNotifAttemptsRepository(client);
        const outboxRepo = createOutboxRepository(client);

        const notification = await notifRepo.insertNotification(input);
        if (!notification) {
            await client.query('ROLLBACK');
            console.log('[Handler] Duplicate notification, skipping');
            return { success: true, notification_id: null};
        }

        for (const channelItem of input.channels){
            await attemptsRepo.insertAttemptPerChannel({
                notification_id: notification.id, 
                channel: channelItem.channel,
                provider: null,
                last_used_provider: null,
                recipient: channelItem.recipient
            });
        }

        await outboxRepo.insertOutboxEntry({notification_id:notification.id,
            payload: {
                notification_id: notification.id,
                user_id: input.user_id,
                channels: input.channels
            }
        });

        await client.query("COMMIT");
        console.log(`[Handler] Notification created: ${notification.id}`);
        return { success: true, notification_id: notification.id };
    } catch (err) {
        await client.query('ROLLBACK');
        console.error('[Handler] Transaction failed, rolled back: ', err.message);
        return { success: false, error: err.message };
    } finally {
        if (shouldRelease && client.release){
            client.release();
        }
    }
}