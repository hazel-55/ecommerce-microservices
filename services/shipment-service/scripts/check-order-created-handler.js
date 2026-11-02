import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { DomainError } from "../src/domain/errors.js";
import {
    createOrderCreatedHandler,
} from "../src/infrastructure/kafka/order-created.handler.js";
import {
    orderCreatedTopic,
} from "../src/infrastructure/kafka/topics.js";

const event = {
    eventId: randomUUID(),
    eventType: "OrderCreated",
    version: 1,
    oid: 1001,
    uid: 13,
    discount: "2.55",
    items: [
        { pid: 1, qty: 2, unit_price: "25.50" },
    ],
};

const applicationResult = {
    created: true,
    shipment: {
        shipid: 501,
        oid: event.oid,
        uid: event.uid,
        sourceEventId: event.eventId,
        status: "PENDING",
    },
};

const calls = [];
const warnings = [];
const errors = [];

let operation = async () => structuredClone(applicationResult);

const application = {
    async handleOrderCreated(input) {
        calls.push(structuredClone(input));
        return await operation(input);
    },
};

const logger = {
    warn(fields, message) {
        warnings.push({ fields, message });
    },

    error(fields, message) {
        errors.push({ fields, message });
    },
};

const handler = createOrderCreatedHandler({
    application,
    logger,
});

function record(payload = event, key = String(payload?.oid)) {
    return {
        topic: orderCreatedTopic,
        partition: 0,
        message: {
            offset: "7",
            key: Buffer.from(key),
            value: Buffer.from(JSON.stringify(payload)),
        },
    };
}

async function expectDomain(operation, code) {
    await assert.rejects(operation, (error) => {
        assert.ok(error instanceof DomainError);
        assert.equal(error.code, code);
        return true;
    });
}

async function main() {
    const extended = {
        ...structuredClone(event),
        shipid: 999,
        status: "DELIVERED",
    };

    assert.deepEqual(await handler(record(extended)), {
        outcome: "CREATED",
        eventId: event.eventId,
        shipid: 501,
    });

    assert.deepEqual(calls[0], event);

    console.log(
        "Valid Kafka record delegates canonical OrderCreated: OK"
    );

    const malformedJson = record();
    malformedJson.message.value = Buffer.from("{broken");

    const tombstone = record();
    tombstone.message.value = null;

    const missingKey = record();
    missingKey.message.key = null;

    const invalidUtf8 = record();
    invalidUtf8.message.value = Buffer.from([0xff]);

    const missingOid = structuredClone(event);
    delete missingOid.oid;

    const badRecords = [
        malformedJson,
        tombstone,
        missingKey,
        invalidUtf8,
        record(missingOid),
        record({ ...event, version: 2 }),
        record(event, "1002"),
        record(event, "01001"),
        record(null),
    ];

    const callsBeforeInvalid = calls.length;

    for (const invalid of badRecords) {
        assert.deepEqual(await handler(invalid), {
            outcome: "REJECTED",
            code: "INVALID_ARGUMENT",
        });
    }

    assert.equal(calls.length, callsBeforeInvalid);
    assert.equal(warnings.length, badRecords.length);

    console.log(
        "Malformed events rejected before application access: OK"
    );

    const persistence = Promise.withResolvers();

    operation = () => persistence.promise;

    let settled = false;
    const pending = handler(record());

    void pending.then(
        () => { settled = true; },
        () => { settled = true; }
    );

    try {
        await Promise.resolve();
        assert.equal(settled, false);
    } finally {
        persistence.resolve(
            structuredClone(applicationResult)
        );
    }

    assert.equal((await pending).outcome, "CREATED");

    console.log("Handler waits for Shipment persistence: OK");

    const duplicate = {
        created: false,
        shipment: {
            ...applicationResult.shipment,
            status: "SHIPPING",
        },
    };

    operation = async () => duplicate;

    assert.deepEqual(await handler(record()), {
        outcome: "DUPLICATE",
        eventId: event.eventId,
        shipid: 501,
    });

    assert.equal(duplicate.shipment.status, "SHIPPING");

    console.log(
        "Duplicate event keeps existing Shipment identity: OK"
    );

    const failures = [
        new Error("Private database failure details"),
        new DomainError(
            "CONFLICT",
            "Shipment ID limit reached"
        ),
        new DomainError(
            "SERVICE_UNAVAILABLE",
            "Storage unavailable"
        ),
    ];

    const errorStart = errors.length;

    for (const failure of failures) {
        operation = async () => {
            throw failure;
        };

        await assert.rejects(
            () => handler(record()),
            (error) => error === failure
        );
    }

    assert.deepEqual(
        errors.slice(errorStart).map(
            (entry) => entry.fields.code
        ),
        ["INTERNAL", "CONFLICT", "SERVICE_UNAVAILABLE"]
    );

    assert.equal(
        JSON.stringify(errors).includes(
            "Private database failure details"
        ),
        false
    );

    console.log(
        "Persistence failures propagate for consumer retry: OK"
    );

    const callsBeforeRouting = calls.length;

    const wrongTopic = record();
    wrongTopic.topic = "unexpected-topic";

    await expectDomain(
        () => handler(wrongTopic),
        "INTERNAL"
    );

    const badMetadata = record();
    badMetadata.partition = -1;

    await expectDomain(
        () => handler(badMetadata),
        "INTERNAL"
    );

    assert.equal(calls.length, callsBeforeRouting);

    operation = async () => ({
        created: true,
        shipment: {
            ...applicationResult.shipment,
            uid: 14,
        },
    });

    await expectDomain(
        () => handler(record()),
        "INTERNAL"
    );

    operation = async () => undefined;

    await expectDomain(
        () => handler(record()),
        "INTERNAL"
    );

    assert.throws(
        () => createOrderCreatedHandler({
            application: {},
            logger,
        }),
        TypeError
    );

    for (const entry of [...warnings, ...errors]) {
        assert.equal("payload" in entry.fields, false);
        assert.equal("value" in entry.fields, false);
        assert.equal("key" in entry.fields, false);
    }

    console.log(
        "Unexpected routing and application results rejected: OK"
    );
}

try {
    await main();
} catch (error) {
    console.error(
        "OrderCreated handler check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
}