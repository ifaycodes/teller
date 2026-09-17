import { kafka } from "./producer.js";
import { config } from "../config/index.js";
import { processNotification } from "../workers/delivery.worker.js";

export async function initConsumer() {
    const consumer = kafka.consumer({groupId: config.kafka.groupId});
    const topic = config.kafka.topic;

    await consumer.connect();
    console.log('[Consumer] Consumer coneected successfully');
    
    console.log('[Consumer] Subscribing to Topic..');
    await consumer.subscribe({topic, fromBeginning: false})
    
    await consumer.run({
        eachMessage: async({topic, partition, message}) => {
            const payload = JSON.parse(message.value.toString());
            console.log(`[Consumer] Partition ${partition} received notification: ${payload.notification_id}`);
            
            // delivery worker takes over
            await processNotification(payload);
            console.log(`[Consumer] Notification sent for processing: ${payload.notification_id}`);
        }
    });
    return consumer;
}
