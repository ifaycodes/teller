import grpc from '@grpc/grpc-js';
import { loadSync } from '@grpc/proto-loader';
import { fileURLToPath } from 'node:url';

import { handleSendNotification } from './handler.js';
import path from 'node:path';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PROTO_PATH = path.join(__dirname, '..', 'proto', 'notifications.proto');

const packageDefinition = loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
});

const notificationProto = grpc.loadPackageDefinition(packageDefinition).Notifications;

const serviceImplementation = {
    sendNotification: async (call, callback) => {
        const {user_id, title, message, html_content, channels = []} = call.request;
        console.log(`[Unary] Sending notification to user ${user_id}: "${message}"`);

        const input = { user_id, title, message, html_content, channels};
        const result = await handleSendNotification(input);
        if (result.success) {
            // callback(error, response) — null means no error
            callback(null, {
                success: true,
                responseMessage: `Notification created: ${result.notification_id}`
            });
        } else {
            // Pass an error back to the gRPC caller
            callback({
                code: grpc.status.INTERNAL,
                message: result.error
            });
        }
    }
}

export function startGrpcServer(port) {
    const server = new grpc.Server();
    server.addService(notificationProto.NotificationService.service, serviceImplementation);

    server.bindAsync(`0.0.0.0:${port}`, grpc.ServerCredentials.createInsecure(), (err, boundPort) => {
        if (err) {
            console.error(err);
            return;
        }
        console.log(`gRPC Notification Server running on port ${ boundPort }`);
    });
}
