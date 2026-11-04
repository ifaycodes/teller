export async function up(db) {
    await db.query('BEGIN');

    try {    
        await db.query(`
            CREATE TABLE IF NOT EXISTS notifications (
                id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id         UUID NOT NULL,
                title           VARCHAR(100) NOT NULL,
                message         VARCHAR(255) NOT NULL,
                html_content    TEXT NOT NULL,
                hash            VARCHAR(64) NOT NULL,
                status          VARCHAR(50) NOT NULL DEFAULT 'pending',
                created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

                CONSTRAINT uq_notification_hash UNIQUE (hash)
            );

            CREATE INDEX IF NOT EXISTS idx_notifications_user_id ON notifications(user_id);
            CREATE INDEX IF NOT EXISTS idx_notifications_status  ON notifications(status);
        `);

        await db.query(`
            CREATE TABLE IF NOT EXISTS notification_attempts (
                id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                notification_id     UUID NOT NULL REFERENCES notifications(id),
                channel             VARCHAR(50) NOT NULL,
                provider            VARCHAR(50),
                last_used_provider  VARCHAR(50),
                recipient           VARCHAR(255) NOT NULL,
                idempotency_key     UUID NOT NULL DEFAULT gen_random_uuid(),
                provider_message_id VARCHAR(255),
                status              VARCHAR(50) NOT NULL DEFAULT 'pending',
                attempts            INT NOT NULL DEFAULT 0,
                max_attempts        INT NOT NULL DEFAULT 3,
                locked_at           TIMESTAMPTZ,
                failed_at           TIMESTAMPTZ,
                successful_at       TIMESTAMPTZ,
                reconciled_at       TIMESTAMPTZ,
                created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_attempts_notification_id ON notification_attempts(notification_id);
            CREATE INDEX IF NOT EXISTS idx_attempts_status          ON notification_attempts(status);
            CREATE INDEX IF NOT EXISTS idx_attempts_channel         ON notification_attempts(channel);
        `);

        await db.query(`
            CREATE TABLE IF NOT EXISTS outbox (
                id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                notification_id UUID NOT NULL REFERENCES notifications(id),
                payload         JSONB NOT NULL,
                published_at    TIMESTAMPTZ,         
                created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_outbox_published_at ON outbox(published_at) WHERE published_at IS NULL;
        `);

        await db.query('COMMIT');
    } catch (error) {
        await db.query('ROLLBACK');
        throw error;
    }
}
// hash: is a SHA-256 of (user_id + channel + core payload) in order to avoid duplicate entry creation.

export async function down(db) {
    await db.query('BEGIN');
    try {
        // Drop in reverse to avoid foreign key dependency crash
        await db.query('DROP TABLE IF EXISTS outbox;');
        await db.query('DROP TABLE IF EXISTS notification_attempts;');
        await db.query('DROP TABLE IF EXISTS notifications;');
        await db.query('COMMIT');
    } catch (error) {
        await db.query('ROLLBACK');
        throw error;
    }
}
