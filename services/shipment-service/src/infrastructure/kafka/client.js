import kafkaJs from "kafkajs";
import { kafkaConfig } from "../config/env.js";

const { Kafka, logLevel } = kafkaJs;

export function createKafkaClient({
    clientId = kafkaConfig.clientId,
    brokers = kafkaConfig.brokers,
} = {}) {
    return new Kafka({
        clientId,
        brokers: [...brokers],
        connectionTimeout: 5000,
        requestTimeout: 10000,
        retry: {
            retries: 2,
        },
        logLevel: logLevel.ERROR,
    });
}