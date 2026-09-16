import { Kafka } from "kafkajs";
import { config } from "../config/index.js";

export const kafka = new Kafka({
    clientId: config.kafka.clientId,
    brokers: config.kafka.brokers
});

const producer = kafka.producer({
    allowAutoTopicCreation: true,
    idempotent: true,
    maxInFlightRequests: 1
});
let connected = false;

export async function connectProducer() {
    if (!connected) {
        await producer.connect();
        connected = true;
        console.log('[Producer] Connected to Kafka');
    }
}

export async function publish(message) {
    await producer.send({
        topic: config.kafka.topic,
        acks: -1,
        messages: [
            {
                key: message.notification_id,
                value: JSON.stringify(message),
            }
        ]
    });
}

export async function disconnectProducer() {
    if (connected){
        await producer.disconnect();
        connected = false;
    }
}