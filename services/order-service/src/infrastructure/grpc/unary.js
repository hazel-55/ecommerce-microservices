import grpc from "@grpc/grpc-js";

function rpcError(code, details) {
    return Object.assign(new Error(details), {
        code,
        details,
    });
}

export async function invokeUnary({
    client,
    method,
    request,
    metadata,
    timeoutMs = 10_000,
    deadline = null,
    signal,
} = {}) {
    if (
        !client ||
        typeof method !== "string" ||
        typeof client[method] !== "function" ||
        !(metadata instanceof grpc.Metadata)
    ) {
        throw new TypeError("Invalid unary RPC arguments");
    }

    if (
        !Number.isInteger(timeoutMs) ||
        timeoutMs < 1 ||
        timeoutMs > 60_000 ||
        (
            deadline !== null &&
            (
                !(deadline instanceof Date) ||
                !Number.isFinite(deadline.getTime())
            )
        ) ||
        (
            signal !== undefined &&
            !(signal instanceof AbortSignal)
        )
    ) {
        throw new TypeError("Invalid unary RPC options");
    }

    if (signal?.aborted) {
        throw rpcError(
            grpc.status.CANCELLED,
            "RPC cancelled before start",
        );
    }

    const startedAt = Date.now();

    const effectiveDeadline = Math.min(
        startedAt + timeoutMs,
        deadline === null
            ? Infinity
            : deadline.getTime(),
    );

    if (effectiveDeadline <= startedAt) {
        throw rpcError(
            grpc.status.DEADLINE_EXCEEDED,
            "RPC deadline exceeded before start",
        );
    }

    return new Promise((resolve, reject) => {
        let call;
        let settled = false;
        let cancelRequested = false;

        function finish(error, response) {
            if (settled) {
                return;
            }

            settled = true;

            signal?.removeEventListener(
                "abort",
                onAbort,
            );

            if (error) {
                reject(error);
                return;
            }

            resolve(response);
        }

        function onAbort() {
            cancelRequested = true;

            if (call && !settled) {
                // Chờ callback của SDK xác nhận RPC đã kết thúc.
                call.cancel();
            }
        }

        signal?.addEventListener(
            "abort",
            onAbort,
            { once: true },
        );

        if (signal?.aborted) {
            finish(rpcError(
                grpc.status.CANCELLED,
                "RPC cancelled before start",
            ));

            return;
        }

        try {
            call = client[method](
                request,
                metadata,
                {
                    deadline: new Date(effectiveDeadline),
                },
                finish,
            );

            // Bảo vệ trường hợp signal bị hủy khi đang tạo call.
            if (cancelRequested && !settled) {
                call.cancel();
            }
        } catch (error) {
            finish(error);
        }
    });
}