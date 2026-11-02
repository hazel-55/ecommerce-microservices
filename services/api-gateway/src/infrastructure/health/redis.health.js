export function createRedisHealthCheck({
    redis,
    timeoutMs = 1000,
} = {}) {
    if (
        !redis ||
        typeof redis.withCommandOptions !== "function"
    ) {
        throw new TypeError(
            "Redis health client is required",
        );
    }

    if (
        !Number.isInteger(timeoutMs) ||
        timeoutMs < 1 ||
        timeoutMs > 5000
    ) {
        throw new TypeError(
            "Health timeout must be between 1 and 5000 milliseconds",
        );
    }

    let activeProbe = null;

    return async function checkRedis() {
        if (!redis.isReady) {
            return false;
        }

        if (activeProbe !== null) {
            return activeProbe.result;
        }

        const controller = new AbortController();

        let resolveResult;
        let finished = false;
        let timer;

        const result = new Promise((resolve) => {
            resolveResult = resolve;
        });

        const probe = { result };

        activeProbe = probe;

        function finish(healthy) {
            if (finished) {
                return;
            }

            finished = true;

            clearTimeout(timer);
            resolveResult(healthy);
        }

        timer = setTimeout(() => {
            finish(false);
            controller.abort();
        }, timeoutMs);

        const pending = Promise.resolve().then(() =>
            redis.withCommandOptions({
                abortSignal: controller.signal,
                timeout: timeoutMs,
            }).ping(),
        );

        function releaseProbe() {
            controller.abort();

            if (activeProbe === probe) {
                activeProbe = null;
            }
        }

        void pending.then(
            (reply) => {
                finish(
                    reply === "PONG" &&
                    redis.isReady === true,
                );

                releaseProbe();
            },
            () => {
                finish(false);
                releaseProbe();
            },
        );

        return result;
    };
}