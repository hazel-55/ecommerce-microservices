import { randomUUID } from "node:crypto";
import { redisClient } from "../redis.js";
import { customerClient } from "../grpc/customer.client.js";

export const CUSTOMER_CACHE_TTL_SECONDS = 60;

export function customerCacheKeys(uid) {
    return {
        data: `gateway:customer:{${uid}}:data`,
        version: `gateway:customer:{${uid}}:version`,
    };
}

const READ_CACHE = `
  return {
    redis.call("GET", KEYS[1]) or "0",
    redis.call("GET", KEYS[2]) or ""
  }
`;

const STORE_CACHE = `
  local current = redis.call("GET", KEYS[1]) or "0"

  if current ~= ARGV[1] then
    return 0
  end

  redis.call("SET", KEYS[2], ARGV[2], "EX", ARGV[3])
  return 1
`;

const INVALIDATE_CACHE = `
  redis.call("SET", KEYS[1], ARGV[1])
  redis.call("DEL", KEYS[2])
  return 1
`;

export async function getCachedCustomer(uid, actor) {
    const keys = customerCacheKeys(uid);

    const [version, cached] = await redisClient.eval(READ_CACHE, {
        keys: [keys.version, keys.data],
        arguments: [],
    });

    if (cached !== "") {
        return {
            customer: JSON.parse(cached),
            cacheStatus: "HIT",
        };
    }

    const customer = await customerClient.getCustomer(uid, actor);

    await redisClient.eval(STORE_CACHE, {
        keys: [keys.version, keys.data],
        arguments: [
            version,
            JSON.stringify(customer),
            String(CUSTOMER_CACHE_TTL_SECONDS),
        ],
    });

    return {
        customer,
        cacheStatus: "MISS",
    };
}

export async function invalidateCustomerCache(uid) {
    const keys = customerCacheKeys(uid);

    await redisClient.eval(INVALIDATE_CACHE, {
        keys: [keys.version, keys.data],
        arguments: [randomUUID()],
    });
}