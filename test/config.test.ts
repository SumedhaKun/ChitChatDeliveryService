import { describe, expect, it } from "vitest";

import { readEnvironment } from "../src/config.js";

const required = {
  DATABASE_URL: "postgres://localhost/chitchat",
  SUPABASE_URL: "https://example.supabase.co",
  SUPABASE_ANON_KEY: "anon",
  KAFKA_BROKERS: "localhost:9092",
};

describe("readEnvironment", () => {
  it("applies delivery defaults", () => {
    expect(readEnvironment(required)).toMatchObject({
      PORT: 8_082,
      KAFKA_CLIENT_ID: "chitchat-delivery-service",
      KAFKA_GROUP_ID: "chitchat-delivery-service",
      HEARTBEAT_INTERVAL_MS: 30_000,
      CONNECTION_STALE_MS: 60_000,
    });
  });

  it("requires Kafka credentials when SSL is enabled", () => {
    expect(() =>
      readEnvironment({
        ...required,
        KAFKA_SSL: "true",
      }),
    ).toThrow(
      "KAFKA_API_KEY and KAFKA_API_SECRET are required when KAFKA_SSL=true",
    );
  });
});
