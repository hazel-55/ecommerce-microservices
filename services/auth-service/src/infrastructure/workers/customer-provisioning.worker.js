import pino from "pino";
import {
    provisioningRepository,
} from "../repositories/provisioning.repository.js";
import {
    provisioningApplication,
} from "../../application/provisioning.application.js";

export function createProvisioningWorker({
    repository = provisioningRepository,
    application = provisioningApplication,
    logger = pino(),
    intervalMs = 2000,
} = {}) {
    let started = false;
    let stopping = false;
    let timer;
    let inFlight = Promise.resolve();

    async function runCycle() {
        const tasks = await repository.findPending(10);

        for (const task of tasks) {
            if (stopping) break;

            try {
                await application.provisionCustomer(task.uid);
            } catch (error) {
                logger.warn(
                    { uid: task.uid, code: error?.code ?? "UNKNOWN" },
                    "Customer provisioning failed"
                );
            }
        }
    }

    function scheduleCycle() {
        if (stopping) return;

        inFlight = runCycle()
            .catch((error) => {
                logger.error(
                    { code: error?.code ?? "UNKNOWN" },
                    "Unable to read provisioning tasks"
                );
            })
            .finally(() => {
                if (!stopping) {
                    timer = setTimeout(scheduleCycle, intervalMs);
                }
            });
    }

    function start() {
        if (started) return;

        started = true;
        stopping = false;
        scheduleCycle();
    }

    async function stop() {
        stopping = true;
        clearTimeout(timer);

        await inFlight;
        started = false;
    }

    return { start, stop };
}