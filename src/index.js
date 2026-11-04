import 'dotenv/config'
import { pool } from "./db/pool.js";
import { up } from "./db/migrations.js";
import { startOutboxRelayWorker } from "./workers/outbox-relay.worker.js";
import { startReconciliationWorker } from './workers/reconciliation.worker.js';
import { startGrpcServer } from "./grpc/server.js";
import { initConsumer } from './kafka/consumer.js';
import { startCircuit } from './circuit-breaker/circuit-breaker.js';

const PORT = process.env.GRPC_PORT || 50051;

async function bootstrap() {
    console.log(`Starting Notification service...`);
    
    await up(pool);
    console.log('Migration Complete...');

    await startCircuit();

    // Start GRPC server
    startGrpcServer(PORT);

    // start workers
    await startOutboxRelayWorker();
    await initConsumer();
    startReconciliationWorker();
    
    console.log('Workers running');
}

bootstrap()
    .then(() => console.log('Notification Service is ready'))
    .catch((err) => {
        console.error('Fatal start up error: ', err);
        process.exit(1);
    });