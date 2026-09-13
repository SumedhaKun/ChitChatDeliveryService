import { randomUUID } from "node:crypto";
import type { AddressInfo } from "node:net";

import { afterEach, describe, expect, it, vi } from "vitest";
import WebSocket, { type RawData } from "ws";

import { DEFAULT_MAX_TRANSPORT_FRAME_BYTES } from "../src/protocol.js";
import {
  createDeliveryService,
  type DeliveryService,
  type Logger,
} from "../src/server.js";

const silentLogger: Logger = {
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
};

const user = { id: randomUUID() };

function rawDataToBuffer(data: RawData): Buffer {
  if (Buffer.isBuffer(data)) return data;
  if (Array.isArray(data)) return Buffer.concat(data);
  return Buffer.from(data);
}

async function openClient(url: string): Promise<WebSocket> {
  const client = new WebSocket(url);
  await new Promise<void>((resolve, reject) => {
    const onError = (error: Error): void => {
      reject(error);
    };
    client.once("error", onError);
    client.once("open", () => {
      client.off("error", onError);
      resolve();
    });
  });
  return client;
}

async function receiveJson(client: WebSocket): Promise<unknown> {
  return await new Promise<unknown>((resolve, reject) => {
    client.once("error", reject);
    client.once("message", (data) => {
      try {
        resolve(JSON.parse(rawDataToBuffer(data).toString("utf8")) as unknown);
      } catch (error) {
        reject(error instanceof Error ? error : new Error("Invalid response"));
      }
    });
  });
}

async function closeClient(client: WebSocket): Promise<void> {
  if (client.readyState === WebSocket.CLOSED) return;
  await new Promise<void>((resolve) => {
    client.once("close", () => resolve());
    if (client.readyState === WebSocket.OPEN) client.close();
  });
}

describe("Delivery WebSocket server", () => {
  let service: DeliveryService | undefined;

  afterEach(async () => {
    if (service !== undefined) {
      await service.close();
      service = undefined;
    }
  });

  async function startService(): Promise<string> {
    service = createDeliveryService({
      port: 0,
      logger: silentLogger,
      authenticate: (token) =>
        Promise.resolve(token === "valid-token" ? user : null),
      heartbeatIntervalMs: 60_000,
      connectionStaleMs: 120_000,
    });
    await service.start();
    const address = service.httpServer.address() as AddressInfo;
    return `http://127.0.0.1:${address.port}`;
  }

  it("serves health checks", async () => {
    const base = await startService();
    const response = await fetch(`${base}/health`);
    await expect(response.json()).resolves.toEqual({ status: "ok" });
  });

  it("authenticates a real WebSocket and registers presence", async () => {
    const base = await startService();
    const client = await openClient(base.replace("http", "ws"));
    client.send(JSON.stringify({ type: "auth", accessToken: "valid-token" }));

    await expect(receiveJson(client)).resolves.toEqual({ type: "auth_ack" });
    expect(service?.presence.isActive(user.id)).toBe(true);
    expect(service?.presence.getConnections(user.id).size).toBe(1);

    await closeClient(client);
    await vi.waitFor(() => {
      expect(service?.presence.isActive(user.id)).toBe(false);
    });
  });

  it("rejects frames before a valid auth", async () => {
    const base = await startService();
    const client = await openClient(base.replace("http", "ws"));
    client.send("{");
    await expect(receiveJson(client)).resolves.toEqual({
      type: "error",
      code: "MALFORMED_JSON",
      message: "Message must be valid JSON",
    });

    client.send(JSON.stringify({ type: "auth", accessToken: "bad-token" }));
    await expect(receiveJson(client)).resolves.toMatchObject({
      type: "error",
      code: "AUTH_FAILED",
    });
    await closeClient(client);
  });

  it("hard-limits frames above the transport maximum", async () => {
    const base = await startService();
    const client = await openClient(base.replace("http", "ws"));
    client.send("a".repeat(DEFAULT_MAX_TRANSPORT_FRAME_BYTES + 1));

    const closeCode = await new Promise<number>((resolve, reject) => {
      client.once("error", reject);
      client.once("close", (code) => resolve(code));
    });
    expect(closeCode).toBe(1009);
  });
});
