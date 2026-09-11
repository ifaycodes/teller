export function createNotifAttemptsRepository(db) {
    return {
        async insertAttemptPerChannel ({notification_id, channel, provider, last_used_provider, recipient}) {
            const result = await db.query(
                `INSERT INTO notification_attempts (notification_id, channel, provider, last_used_provider, recipient)
                VALUES ($1, $2, $3, $4, $5)`,
                [notification_id, channel, provider, last_used_provider, recipient]
            );
            return console.log("New attempt made for new notification. ID: " + result.id + " through channel: " + channel);
        },

    }
}