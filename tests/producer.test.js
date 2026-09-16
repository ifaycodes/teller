import { test } from 'node:test';
import assert from 'node:assert/strict';
import { publish, producer } from '../src/kafka/producer.js';

test('publish sends formatted message to Kafka with correct partition key and acks', async () => {
    let sendArgs = null;

    // Mock producer.send
    const originalSend = producer.send;
    producer.send = async (args) => {
        sendArgs = args;
        return [{ topicName: args.topic, partition: 0, errorCode: 0 }];
    };

    const mockMessage = {
        notification_id: 'notif_123',
        user_id: 'user_456',
        channels: [{ channel: 'email', recipient: 'test@example.com' }]
    };

    await publish(mockMessage);

    // Verify args passed to Kafka
    assert.equal(sendArgs.acks, -1);
    assert.equal(sendArgs.messages.length, 1);
    assert.equal(sendArgs.messages[0].key, 'notif_123');
    assert.equal(sendArgs.messages[0].value, JSON.stringify(mockMessage));

    // Restore mock
    producer.send = originalSend;
});