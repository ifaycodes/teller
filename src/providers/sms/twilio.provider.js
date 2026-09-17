import twilio from "twilio";
import { config } from "../../config/index.js";
import { NotificationProvider } from "../provider.interface.js";

const { Twilio } = twilio;
class TwilioProvider extends NotificationProvider {
    get channel() { return 'sms'; }
    get slug() { return 'twilio'; }

    constructor() {
        super();
        this.client = new Twilio (
            config.providers.sms.twilio.accountSid,
            config.providers.sms.twilio.authToken
        );
        this.fromNumber = config.providers.sms.twilio.fromNumber;
    }


    async send(payload) {
        console.log(`[Twilio] Sending SMS to: ${payload.recipient}`);
        const message = await this.client.messages.create({
            body: payload.message,
            from: this.fromNumber,
            to: payload.recipient,
        });
        console.log(`[Twilio] SMS Queued with ID: ${message.sid} | status: ${message.status}`);
        return { success: true, provider_message_id: message.sid };
    }
    
    async getStatus(sid) {
        const message = await this.client.messages(sid).fetch();
        const map = {
            delivered: 'delivered',
            sent: 'delivered',
            failed: 'failed',
            undelivered: 'failed',
            queued: 'pending',
            sending: 'pending',
        };
        return map[message.status] ?? 'unknown';
    }
}

export const twilioProvider = new TwilioProvider();