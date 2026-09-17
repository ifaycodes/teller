import { sendbyteProvider } from "./email/sendbyte.provider.js";
import { twilioProvider } from "./sms/twilio.provider.js";

const registry = new Map();

function register(provider) {
    const key = `${provider.channel}:${provider.slug}`;
    registry.set(key, provider);
    console.log(`[Factory] Registered provider: ${key}`);
}

register(sendbyteProvider);
register(twilioProvider);

export async function sendViaProvider(channel, slug, payload) {
    const key = `${channel}:${slug}`;
    const provider = registry.get(key);
    if (!provider) throw new Error(`[Factory] No provider registered for: ${key}`);
    return await provider.send(payload);
}

export async function getProviderStatus(channel, slug, messageId) {
    const key = `${channel}:${slug}`;
    const provider = registry.get(key);
    if (!provider) return 'unknown';
    return await provider.getStatus(messageId);
}