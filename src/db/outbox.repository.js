export function createOutboxRepository(db) {
    return {
        async insertOutboxEntry ({notification_id, payload}) {
            const result = await db.query(
                `INSERT INTO outbox (notification_id, payload)
                VALUES ($1, $2)`,
                [notification_id, payload]
            );
            return console.log("New attempt recorded. ID: " + result.id + " for notification: " + notification_id);
        },
    }
}