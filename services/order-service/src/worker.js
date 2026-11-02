import pino from "pino";
import {
    creationWorkerConfig,
} from "./infrastructure/config/env.js";
import {
    pool,
} from "./infrastructure/database/pool.js";
import {
    customerClient,
} from "./infrastructure/grpc/customer.client.js";
import {
    productClient,
} from "./infrastructure/grpc/product.client.js";
import {
    createOrderCreationWorker,
} from "./application/order-creation.worker.js";
import {
    createOrderCreationLoop,
} from "./application/order-creation.loop.js";

const logger = pino();

const worker = createOrderCreationWorker({
    batchSize: creationWorkerConfig.batchSize,
    logger,
});

const loop = createOrderCreationLoop({
    worker,
    intervalMs: creationWorkerConfig.intervalMs,
    logger,
});

// Thời gian chờ dừng tăng theo kích thước batch.
const shutdownTimeoutMs =
    creationWorkerConfig.batchSize * 15_000 + 10_000;

let stopping = false;
let shutdownPromise;

async function performShutdown(signal) {
    logger.info(
        { signal },
        "Order creation worker stopping",
    );

    const timer = setTimeout(() => {
        logger.error(
            "Order creation worker shutdown timed out",
        );

        process.exit(1);
    }, shutdownTimeoutMs);

    timer.unref();

    try {
        await loop.stop();

        // Chỉ đóng tài nguyên sau khi batch đã kết thúc.
        const results = await Promise.allSettled([
            Promise.resolve().then(() => customerClient.close()),
            Promise.resolve().then(() => productClient.close()),
            Promise.resolve().then(() => pool.end()),
        ]);

        const failure = results.find(
            (result) => result.status === "rejected",
        );

        if (failure) {
            throw failure.reason;
        }

        logger.info(
            "Order creation worker stopped",
        );
    } catch (error) {
        process.exitCode = 1;

        logger.error(
            { code: error?.code ?? "INTERNAL" },
            "Order creation worker shutdown failed",
        );
    } finally {
        clearTimeout(timer);
    }
}

function shutdown(signal) {
    if (shutdownPromise) {
        return shutdownPromise;
    }

    stopping = true;
    shutdownPromise = performShutdown(signal);

    return shutdownPromise;
}

process.once("SIGINT", () => {
    void shutdown("SIGINT");
});

process.once("SIGTERM", () => {
    void shutdown("SIGTERM");
});

async function main() {
    // Kiểm tra kết nối và các cột task trước khi chạy.
    await pool.query(
        `
        SELECT
            operation_id,
            uid,
            actor_role,
            items,
            score,
            status,
            failure_code,
            attempts,
            last_error_code,
            next_attempt_at
        FROM order_creation_tasks
        LIMIT 0
        `,
    );

    if (stopping) {
        return;
    }

    loop.start();

    logger.info(
        {
            batchSize: creationWorkerConfig.batchSize,
            intervalMs: creationWorkerConfig.intervalMs,
        },
        "Order creation worker started",
    );
}

main().catch(async (error) => {
    process.exitCode = 1;

    logger.error(
        { code: error?.code ?? "INTERNAL" },
        "Order creation worker startup failed",
    );

    await shutdown("STARTUP_FAILURE");
});