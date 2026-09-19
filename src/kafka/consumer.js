import { kafka } from "./producer.js";
import { config } from "../config/index.js";
import { processNotification } from "../workers/delivery.worker.js";
import { areAllProvidersOpen } from "../circuit-breaker/circuit-breaker.js";

export async function initConsumer() {
    const consumer = kafka.consumer({groupId: config.kafka.groupId});
    const topic = config.kafka.topic;

    await consumer.connect();
    console.log('[Consumer] Consumer coneected successfully');
    
    console.log('[Consumer] Subscribing to Topic..');
    await consumer.subscribe({topic, fromBeginning: false})
    
    await consumer.run({
        eachBatch: async({ batch, resolveOffset, heartbeat, isRunning, isPaused}) => {
            console.log(`[Consumer] Received batch of ${batch.messages.length} messages from partition ${batch.partition}`);

            const emailDown = await areAllProvidersOpen('email', ['sendbyte']);
            const smsDown = await areAllProvidersOpen('sms', ['twilio']);

            if (emailDown && smsDown && !isPaused()) {
                console.warn(`[Consumer] All notification providers are OPEN. Pausing consumer for ${config.circuitBreaker.cooldownSeconds}s...`);
                consumer.pause([{ topic }]);

                setTimeout(() => {
                    console.log(`[Consumer] Circuit cooldown elapsed. Resuming consumer...`);
                    consumer.resume([{ topic }]);
                }, config.circuitBreaker.cooldownSeconds * 1000);

                return;
            }

            for (const message of batch.messages) {
                if (!isRunning() || isPaused()) break;

                if (!message.value) {
                    console.warn(`[Consumer] Received empty message from partition ${batch.partition}`);
                    resolveOffset(message.offset);
                    await heartbeat();
                    continue;
                }
                let payload;
                try {
                    payload = JSON.parse(message.value.toString());
                } catch (parseErr) {
                    console.error(`[Consumer] Failed to parse malformed message from partition ${batch.partition}: `, parseErr.message);
                    resolveOffset(message.offset);
                    await heartbeat();
                    continue;
                }

                try {
                    console.log(`[Consumer] Partition ${batch.partition} received notification: ${payload.notification_id}`);
                    await processNotification(payload);
                    console.log(`[Consumer] Notification sent for processing: ${payload.notification_id}`); 
                } catch (processErr) {
                    console.error(`[Consumer] Failed to process notification ${payload.notification_id} from partition ${batch.partition}: `, processErr.message);
                }

                resolveOffset(message.offset);
                await heartbeat();
            }
        }
    });
    return consumer;
}
