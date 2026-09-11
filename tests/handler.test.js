// test/handler.test.js
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { handleSendNotification } from '../src/grpc/handler.js';

test('handleSendNotification successfully creates a notification with mock DB', async () => {
    // 1. Define mock input payload
    const mockInput = {
        user_id: 'usr_999',
        title: 'Welcome Notification',
        message: 'Hello, your account has been created!',
        html_content: '<h1>Welcome!</h1>',
        channels: [
            { channel: 'email', recipient: 'user@example.com' },
            { channel: 'sms', recipient: '+1234567890' }
        ]
    };

    // 2. Create a mock DB client matching pg client behavior
    const mockDbClient = {
        queriesExecuted: [],
        async query(sql, params) {
            this.queriesExecuted.push({ sql, params });
            
            // Mock INSERT INTO notifications RETURNING *
            if (sql.includes('INSERT INTO notifications')) {
                return {
                    rows: [{
                        id: 'notif_uuid_123',
                        user_id: params[0],
                        title: params[1],
                        message: params[2],
                        html_content: params[3],
                        hash: params[4]
                    }]
                };
            }
            // Mock INSERT INTO notification_attempts
            if (sql.includes('INSERT INTO notification_attempts')) {
                return { oid: 1 };
            }
            // Mock INSERT INTO outbox
            if (sql.includes('INSERT INTO outbox')) {
                return { oid: 2 };
            }
            return { rows: [], rowCount: 1 };
        },
        release() {}
    };

    // 3. Execute handler with mock client
    const result = await handleSendNotification(mockInput, mockDbClient);

    // 4. Assertions
    assert.deepEqual(result, {
        success: true,
        notification_id: 'notif_uuid_123'
    });

    // Check that BEGIN and COMMIT transactions were called
    const statements = mockDbClient.queriesExecuted.map(q => q.sql);
    assert.ok(statements.includes('BEGIN'));
    assert.ok(statements.includes('COMMIT'));
});