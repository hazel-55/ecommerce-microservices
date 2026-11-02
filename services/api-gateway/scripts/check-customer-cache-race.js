import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { redisClient } from "../src/infrastructure/redis.js";
import { customerClient } from "../src/infrastructure/grpc/customer.client.js";
import {
    customerCacheKeys,
    getCachedCustomer,
    invalidateCustomerCache,
} from "../src/infrastructure/cache/customer.cache.js";

// Key riêng cho bài kiểm tra, không dùng key của hồ sơ thật.
// UID này chỉ truyền vào helper cache với client đã giả lập.
const testUid = `test-race-${randomUUID()}`;
const keys = customerCacheKeys(testUid);
const actor = { uid: 13, role: "CUSTOMER" };

const oldCustomer = {
    uid: 13,
    fullname: "Before update",
    mid: 1,
    mname: "Test membership",
    score: 0,
};

const newCustomer = {
    ...oldCustomer,
    fullname: "After update",
};

const originalGetCustomer = customerClient.getCustomer;

let signalStarted;
const started = new Promise((resolve) => {
    signalStarted = resolve;
});

let releaseOldRead;
const oldReadResult = new Promise((resolve) => {
    releaseOldRead = resolve;
});

let pendingRead;
let rpcCalls = 0;

try {
    await redisClient.connect();

    customerClient.getCustomer = async (uid, caller) => {
        assert.equal(uid, testUid);
        assert.deepEqual(caller, actor);
        rpcCalls++;

        if (rpcCalls === 1) {
            signalStarted();
            return oldReadResult;
        }

        return newCustomer;
    };

    // GET đã đọc phiên bản cache và đang chờ Customer trả dữ liệu cũ.
    pendingRead = getCachedCustomer(testUid, actor);

    await Promise.race([
        started,
        pendingRead.then(() => {
            throw new Error("Expected the first GET to remain pending");
        }),
        new Promise((_, reject) => {
            const timer = setTimeout(
                () => reject(new Error("Timed out waiting for simulated RPC")),
                5000,
            );
            timer.unref();
        }),
    ]);

    console.log("Old GET paused before returning customer data: OK");

    // Giả lập thời điểm cập nhật đã thành công và Gateway xóa cache.
    await invalidateCustomerCache(testUid);

    assert.notEqual(await redisClient.get(keys.version), null);
    assert.equal(await redisClient.get(keys.data), null);

    // GET cũ tiếp tục sau khi cache đã đổi phiên bản.
    releaseOldRead(oldCustomer);

    const oldRead = await pendingRead;
    assert.equal(oldRead.cacheStatus, "MISS");
    assert.deepEqual(oldRead.customer, oldCustomer);

    // GET đang chạy có thể trả snapshot cũ,
    // nhưng không được ghi snapshot đó trở lại cache.
    assert.equal(await redisClient.get(keys.data), null);

    console.log("Old GET cannot repopulate invalidated cache: OK");

    const freshRead = await getCachedCustomer(testUid, actor);

    assert.equal(freshRead.cacheStatus, "MISS");
    assert.deepEqual(freshRead.customer, newCustomer);
    assert.deepEqual(
        JSON.parse(await redisClient.get(keys.data)),
        newCustomer,
    );

    console.log("Next GET caches the updated customer: OK");

    const cachedRead = await getCachedCustomer(testUid, actor);

    assert.equal(cachedRead.cacheStatus, "HIT");
    assert.deepEqual(cachedRead.customer, newCustomer);
    assert.equal(rpcCalls, 2);

    console.log("Following GET uses updated cache without another RPC: OK");
} catch (error) {
    console.error("Customer cache race check failed:", error.message);
    process.exitCode = 1;
} finally {
    // Giải phóng lời gọi giả lập nếu bài kiểm tra dừng giữa chừng.
    releaseOldRead(oldCustomer);

    try {
        if (pendingRead) {
            await pendingRead.catch(() => { });
        }

        if (redisClient.isOpen) {
            await redisClient.del([keys.data, keys.version]);
            console.log("Isolated test cache keys removed: OK");
        }
    } catch (error) {
        console.error("Test cache cleanup failed:", error.message);
        process.exitCode = 1;
    } finally {
        customerClient.getCustomer = originalGetCustomer;
        customerClient.close();

        if (redisClient.isOpen) {
            await redisClient.close();
        }
    }
}