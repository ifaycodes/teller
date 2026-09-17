export class NotificationProvider {
    // Each provider declares what channel and slug it handles
    get channel() { throw new Error(`${this.constructor.name} must implement channel`); }
    get slug()    { throw new Error(`${this.constructor.name} must implement slug`); }

    /**
     * Send a notification through this provider
     * @param {{ recipient: string, title?: string, message: string, html_content?: string, idempotency_key: string }} payload
     * @returns {Promise<{ success: boolean, provider_message_id?: string, error?: string }>}
     */
    async send(payload) {
        throw new Error(`${this.constructor.name} must implement send()`);
    }

    /**
     * Check delivery status of a previously sent message. Used by reconciliation worker
     * @param {string} messageId
     * @returns {Promise<'delivered' | 'failed' | 'pending' | 'unknown'>}
     */
    async getStatus(messageId) {
        throw new Error(`${this.constructor.name} must implement getStatus()`);
    }
}