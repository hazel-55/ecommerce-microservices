import pino from "pino";
import { DomainError } from "../domain/errors.js";

export function createOrderCreationLoop({
    worker,
    intervalMs = 1000,
    logger = pino(),
} = {}) {
    if (!worker || typeof worker.runOnce !== "function") {
        throw new TypeError(
            "Creation worker must provide runOnce",
        );
    }

    if (
        !Number.isInteger(intervalMs) ||
        intervalMs < 1 ||
        intervalMs > 60_000
    ) {
        throw new DomainError(
            "INVALID_ARGUMENT",
            "Worker interval must be between 1 and 60000 milliseconds",
        );
    }

    let activeRun = null;
    let stopRequested = false;
    let wake = null;

    function waitForNextBatch() {
        return new Promise((resolve) => {
            const timer = setTimeout(finish, intervalMs);

            function finish() {
                clearTimeout(timer);

                if (wake === finish) {
                    wake = null;
                }

                resolve();
            }

            wake = finish;

            if (stopRequested) {
                finish();
            }
        });
    }

    async function runLoop() {
        try {
            while (!stopRequested) {
                try {
                    await worker.runOnce();
                } catch (error) {
                    const code =
                        error instanceof DomainError &&
                            error.code === "SERVICE_UNAVAILABLE"
                            ? "SERVICE_UNAVAILABLE"
                            : "INTERNAL";

                    logger.error(
                        { code },
                        "Order creation worker batch failed",
                    );
                }

                if (!stopRequested) {
                    await waitForNextBatch();
                }
            }
        } finally {
            activeRun = null;
        }
    }

    return {
        start() {
            if (activeRun !== null) {
                return false;
            }

            stopRequested = false;
            activeRun = runLoop();

            return true;
        },

        async stop() {
            const currentRun = activeRun;

            if (currentRun === null) {
                return;
            }

            stopRequested = true;

            // Kết thúc thời gian nghỉ nếu đang chờ lượt sau.
            if (wake) {
                wake();
            }

            // Chờ batch đang chạy hoàn tất.
            await currentRun;
        },

        get isRunning() {
            return activeRun !== null;
        },
    };
}