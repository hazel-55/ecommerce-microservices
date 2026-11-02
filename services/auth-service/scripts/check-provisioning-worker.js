import assert from "node:assert/strict";
import { DomainError } from "../src/domain/errors.js";
import {
    createProvisioningWorker,
} from "../src/infrastructure/workers/customer-provisioning.worker.js";
import {
    customerClient,
} from "../src/infrastructure/grpc/customer.client.js";
import { pool } from "../src/infrastructure/database/pool.js";

let worker;
let releaseSecond;

try {
    let reads = 0;
    const calls = [];
    const warnings = [];
    const errors = [];

    let notifySecond;

    const secondEntered = new Promise((resolve) => {
        notifySecond = resolve;
    });

    const secondGate = new Promise((resolve) => {
        releaseSecond = resolve;
    });

    worker = createProvisioningWorker({
        repository: {
            async findPending() {
                reads += 1;
                return [{ uid: 1 }, { uid: 2 }, { uid: 3 }];
            },
        },

        application: {
            async provisionCustomer(uid) {
                calls.push(uid);

                if (uid === 1) {
                    throw new DomainError(
                        "SERVICE_UNAVAILABLE",
                        "Simulated failure"
                    );
                }

                if (uid === 2) {
                    notifySecond();
                    await secondGate;
                }

                return { uid, ready: true };
            },
        },

        logger: {
            warn(data) {
                warnings.push(data);
            },
            error(data) {
                errors.push(data);
            },
        },
    });

    worker.start();
    worker.start();

    await secondEntered;

    assert.equal(reads, 1);
    assert.deepEqual(calls, [1, 2]);
    assert.equal(warnings.length, 1);
    assert.equal(warnings[0].uid, 1);

    let stopped = false;

    const stopping = worker.stop().then(() => {
        stopped = true;
    });

    await Promise.resolve();
    assert.equal(stopped, false);

    releaseSecond();
    await stopping;

    assert.equal(stopped, true);
    assert.deepEqual(calls, [1, 2]);
    assert.equal(errors.length, 0);

    console.log("Provisioning worker lifecycle: OK");
    console.log("Job failure does not block the next job: OK");
    console.log("Duplicate start does not duplicate the cycle: OK");
    console.log("Graceful stop waits for in-flight processing: OK");
} catch (error) {
    console.error("Worker check failed:", error.code ?? error.message);
    process.exitCode = 1;
} finally {
    releaseSecond?.();
    await worker?.stop();
    customerClient.close();
    await pool.end();
}