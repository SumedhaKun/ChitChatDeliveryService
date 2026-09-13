import { randomUUID } from "node:crypto";

import { describe, expect, it, vi } from "vitest";

import {
  DEFAULT_KAFKA_CLIENT_ID,
  DEFAULT_KAFKA_GROUP_ID,
  MESSAGE_CREATED_TOPIC,
  createMessageConsumer,
  readKafkaSettings,
  type ConsumerClient,
} from "../src/kafka.js";

describe("Kafka settings", () => {
  it("omits SASL for local plaintext brokers", () => {
    expect(
      readKafkaSettings({
        KAFKA_BROKERS: "localhost:9092",
      }),
    ).toEqual({
      brokers: ["localhost:9092"],
      clientId: DEFAULT_KAFKA_CLIENT_ID,
      groupId: DEFAULT_KAFKA_GROUP_ID,
      ssl: false,
    });
  });

  it("requires API credentials when SSL is enabled", () => {
    expect(() =>
      readKafkaSettings({
        KAFKA_BROKERS: "pkc.example.confluent.cloud:9092",
        KAFKA_SSL: "true",
      }),
    ).toThrow(
      "KAFKA_API_KEY and KAFKA_API_SECRET are required when KAFKA_SSL=true",
    );
  });

  it("sets SASL PLAIN for Confluent Cloud", () => {
    expect(
      readKafkaSettings({
        KAFKA_BROKERS: "pkc.example.confluent.cloud:9092",
        KAFKA_SSL: "true",
        KAFKA_API_KEY: "cluster-key",
        KAFKA_API_SECRET: "cluster-secret",
        KAFKA_CLIENT_ID: "delivery",
        KAFKA_GROUP_ID: "delivery-group",
      }),
    ).toEqual({
      brokers: ["pkc.example.confluent.cloud:9092"],
      clientId: "delivery",
      groupId: "delivery-group",
      ssl: true,
      sasl: {
        mechanism: "plain",
        username: "cluster-key",
        password: "cluster-secret",
      },
    });
  });

  it("rejects missing brokers", () => {
    expect(() => readKafkaSettings({})).toThrow("KAFKA_BROKERS is required");
  });
});

describe("createMessageConsumer", () => {
  it("subscribes to messageCreated and handles valid events", async () => {
    const event = {
      messageId: randomUUID(),
      senderId: randomUUID(),
      conversationId: randomUUID(),
      content: "hello",
      createdAt: "2026-09-13T20:00:00.000Z",
    };
    let eachMessage:
      | ((payload: { message: { value: Buffer | null } }) => Promise<void>)
      | undefined;
    const subscribe = vi.fn(() => Promise.resolve());
    const disconnect = vi.fn(() => Promise.resolve());
    const consumer: ConsumerClient = {
      connect: () => Promise.resolve(),
      subscribe,
      run: (options) => {
        eachMessage = (payload) => options.eachMessage(payload);
        return Promise.resolve();
      },
      disconnect,
    };
    const handle = vi.fn(() => Promise.resolve());
    const onInvalid = vi.fn();
    const wrapper = createMessageConsumer(consumer, handle, onInvalid);

    await wrapper.start();
    expect(subscribe).toHaveBeenCalledWith({
      topic: MESSAGE_CREATED_TOPIC,
      fromBeginning: false,
    });
    await eachMessage?.({
      message: { value: Buffer.from(JSON.stringify(event)) },
    });
    await eachMessage?.({ message: { value: null } });
    await wrapper.disconnect();

    expect(handle).toHaveBeenCalledWith(event);
    expect(onInvalid).toHaveBeenCalledOnce();
    expect(disconnect).toHaveBeenCalledOnce();
  });
});
