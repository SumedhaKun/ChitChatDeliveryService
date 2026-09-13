import { KafkaJS } from "@confluentinc/kafka-javascript";

import type { MessageCreatedEvent } from "./events.js";
import { parseMessageCreatedEvent } from "./events.js";

const { Kafka } = KafkaJS;

export const MESSAGE_CREATED_TOPIC = "messageCreated";
export const DEFAULT_KAFKA_CLIENT_ID = "chitchat-delivery-service";
export const DEFAULT_KAFKA_GROUP_ID = "chitchat-delivery-service";

export type KafkaSettings =
  | {
      brokers: string[];
      clientId: string;
      groupId: string;
      ssl: false;
    }
  | {
      brokers: string[];
      clientId: string;
      groupId: string;
      ssl: true;
      sasl: {
        mechanism: "plain";
        username: string;
        password: string;
      };
    };

export function readKafkaSettings(
  env: NodeJS.ProcessEnv = process.env,
): KafkaSettings {
  const brokers = (env.KAFKA_BROKERS ?? "")
    .split(",")
    .map((broker) => broker.trim())
    .filter(Boolean);
  if (brokers.length === 0) throw new Error("KAFKA_BROKERS is required");

  const base = {
    brokers,
    clientId: env.KAFKA_CLIENT_ID || DEFAULT_KAFKA_CLIENT_ID,
    groupId: env.KAFKA_GROUP_ID || DEFAULT_KAFKA_GROUP_ID,
  };
  if (env.KAFKA_SSL !== "true") return { ...base, ssl: false };
  if (!env.KAFKA_API_KEY || !env.KAFKA_API_SECRET) {
    throw new Error(
      "KAFKA_API_KEY and KAFKA_API_SECRET are required when KAFKA_SSL=true",
    );
  }
  return {
    ...base,
    ssl: true,
    sasl: {
      mechanism: "plain",
      username: env.KAFKA_API_KEY,
      password: env.KAFKA_API_SECRET,
    },
  };
}

export interface ConsumerClient {
  connect(): Promise<void>;
  subscribe(options: { topic: string }): Promise<void>;
  run(options: {
    eachMessage(payload: { message: { value: Buffer | null } }): Promise<void>;
  }): Promise<void>;
  disconnect(): Promise<void>;
}

export interface MessageConsumer {
  start(): Promise<void>;
  disconnect(): Promise<void>;
}

export interface ConsumerLogger {
  info(event: Record<string, unknown>): void;
  error(event: Record<string, unknown>): void;
}

export function createConfluentConsumer(
  settings: KafkaSettings,
): ConsumerClient {
  const kafka = new Kafka({
    kafkaJS: settings.ssl
      ? {
          clientId: settings.clientId,
          brokers: settings.brokers,
          ssl: true,
          sasl: settings.sasl,
        }
      : {
          clientId: settings.clientId,
          brokers: settings.brokers,
        },
  });
  return kafka.consumer({
    kafkaJS: {
      groupId: settings.groupId,
      fromBeginning: false,
    },
  });
}

export function createMessageConsumer(
  consumer: ConsumerClient,
  handle: (event: MessageCreatedEvent) => Promise<void>,
  logger: ConsumerLogger = console,
): MessageConsumer {
  return {
    async start() {
      await consumer.connect();
      logger.info({
        event: "kafka_consumer_connected",
        topic: MESSAGE_CREATED_TOPIC,
      });
      await consumer.subscribe({
        topic: MESSAGE_CREATED_TOPIC,
      });
      logger.info({
        event: "kafka_consumer_subscribed",
        topic: MESSAGE_CREATED_TOPIC,
      });
      logger.info({
        event: "kafka_consumer_running",
        topic: MESSAGE_CREATED_TOPIC,
      });
      await consumer.run({
        async eachMessage({ message }) {
          try {
            const event = parseMessageCreatedEvent(message.value);
            logger.info({
              event: "kafka_message_received",
              messageId: event.messageId,
              conversationId: event.conversationId,
              senderId: event.senderId,
            });
            await handle(event);
          } catch (error) {
            logger.error({
              event: "kafka_message_invalid",
              error: error instanceof Error ? error.message : "Unknown error",
            });
          }
        },
      });
    },
    async disconnect() {
      await consumer.disconnect();
    },
  };
}
