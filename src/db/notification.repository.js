import crypto from 'node:crypto';
export function createNotificationRepository(db) {
    return {
        async insertNotification ({ user_id, title, message, html_content, channels = []}) {
            const channelStr = channels.map(c => c.channel).join(',');
            const hash = crypto.createHash('sha256')
                .update(`${user_id}:${channelStr}:${message}`)
                .digest('hex');

            const result = await db.query(
                `INSERT INTO notifications
                    (user_id, title, message, html_content, hash)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (hash) DO NOTHING
                    RETURNING *`,
                    [user_id, title, message, html_content, hash]
            );
            return result.rows[0] ?? null;
        },

        async updateNotifications ({status, notification_id}) {
            const result = await db.query(
                `UPDATE notifications SET status = $1, updated_at = NOW()
                WHERE id = $2`,
                [status, notification_id]
            );
            if (result.rowCount === 0) {
                console.log("No rows was found with ID: " + notification_id);
            } else {
                console.log("Updated status of row. ID: " + notification_id);
            }
        },

        async getById(id) {
            const result = await db.query('SELECT * FROM notifications WHERE id = $1', [id]);
            return result.rows[0] ?? null;
        }
    }
}