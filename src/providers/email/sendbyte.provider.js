import { SendByte, SendByteError } from "@sendbyte/node";
import { config } from "../../config/index.js";
import { NotificationProvider, redactRecipient } from "../provider.interface.js";

class SendbyteProvider extends NotificationProvider {
    get channel() { return 'email'; }
    get slug() { return 'sendbyte'; }

    constructor() {
        super();
        this.client = new SendByte(config.providers.email.sendbyte.apiKey);
        this.fromEmail = config.providers.email.sendbyte.fromEmail;
    }

    async send(payload) {
        console.log(`[Sendbyte] Sending email to: ${redactRecipient(payload.recipient)}`);
        try {
            const message = await this.client.emails.send({
                from: this.fromEmail,
                to: payload.recipient,
                subject: payload.title,
                html: payload.html_content,
                text: payload.message,
                idempotencyKey: payload.idempotency_key,
            });
            console.log('[Sendbyte] Email queued with ID:', message.id);
            return { success: true, provider_message_id: message.id };
        } catch (err) {
            if (err instanceof SendByteError) {
                console.error(`[Sendbyte] API error: `, err.code, err.message, err.docsUrl);
            } else {
                console.error(`[Sendbyte] Unexpected error: `, err.message);
            }
            throw err;
        }
    }

    async getStatus(id) {
        const message = await this.client.emails.get(id);
        return message.status;
    }
}

export const sendbyteProvider = new SendbyteProvider();