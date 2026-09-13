import { randomUUID } from "node:crypto";

import { describe, expect, it } from "vitest";

import { parseMessageCreatedEvent } from "../src/events.js";

const valid = {
  messageId: randomUUID(),
  senderId: randomUUID(),
  conversationId: randomUUID(),
  content: "hello",
  createdAt: "2026-09-13T20:00:00.000Z",
};

describe("parseMessageCreatedEvent", () => {
  it("parses a valid camelCase event", () => {
    expect(parseMessageCreatedEvent(JSON.stringify(valid))).toEqual(valid);
    expect(
      parseMessageCreatedEvent(Buffer.from(JSON.stringify(valid))),
    ).toEqual(valid);
  });

  it("rejects null, invalid JSON, and extra fields", () => {
    expect(() => parseMessageCreatedEvent(null)).toThrow(
      "Kafka message value is null",
    );
    expect(() => parseMessageCreatedEvent("{")).toThrow(
      "Kafka message value is not valid JSON",
    );
    expect(() =>
      parseMessageCreatedEvent(JSON.stringify({ ...valid, extra: true })),
    ).toThrow();
    expect(() =>
      parseMessageCreatedEvent(JSON.stringify({ ...valid, messageId: "nope" })),
    ).toThrow();
  });
});
