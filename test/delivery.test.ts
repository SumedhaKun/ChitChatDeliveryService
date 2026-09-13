import { randomUUID } from "node:crypto";

import { describe, expect, it, vi } from "vitest";

import { TtlDedupe } from "../src/dedupe.js";
import { DeliveryHandler, type PushNotifier } from "../src/delivery.js";
import type { MessageCreatedEvent } from "../src/events.js";
import { Presence } from "../src/presence.js";
import { FakeSocket } from "./fakes.js";

function event(
  overrides: Partial<MessageCreatedEvent> = {},
): MessageCreatedEvent {
  return {
    messageId: randomUUID(),
    senderId: randomUUID(),
    conversationId: randomUUID(),
    content: "hello",
    createdAt: "2026-09-13T20:00:00.000Z",
    ...overrides,
  };
}

describe("DeliveryHandler", () => {
  it("fans out to every open recipient socket and excludes the sender", async () => {
    const senderId = randomUUID();
    const recipientId = randomUUID();
    const created = event({ senderId });
    const presence = new Presence();
    const senderSocket = new FakeSocket();
    const first = new FakeSocket();
    const second = new FakeSocket();
    const notify = vi.fn();
    presence.setConnection(senderId, senderSocket);
    presence.setConnection(recipientId, first);
    presence.setConnection(recipientId, second);

    const handler = new DeliveryHandler(
      {
        getUserIds: () => Promise.resolve([senderId, recipientId]),
      },
      presence,
      { notifyMessageCreated: notify },
      new TtlDedupe(60_000, 100),
    );

    await handler.handle(created);

    const expected = JSON.stringify({
      type: "message_created",
      message: created,
    });
    expect(first.sent).toEqual([expected]);
    expect(second.sent).toEqual([expected]);
    expect(senderSocket.sent).toEqual([]);
    expect(notify).not.toHaveBeenCalled();
  });

  it("does not push the sender and no-ops for offline recipients", async () => {
    const senderId = randomUUID();
    const offlineId = randomUUID();
    const created = event({ senderId });
    const notify: PushNotifier["notifyMessageCreated"] = vi.fn();

    const handler = new DeliveryHandler(
      {
        getUserIds: () => Promise.resolve([senderId, offlineId]),
      },
      new Presence(),
      { notifyMessageCreated: notify },
      new TtlDedupe(60_000, 100),
    );

    await handler.handle(created);

    expect(notify).toHaveBeenCalledTimes(1);
    expect(notify).toHaveBeenCalledWith(offlineId, created);
  });

  it("removes sockets that fail to send and continues", async () => {
    const recipientId = randomUUID();
    const created = event();
    const presence = new Presence();
    const failing = new FakeSocket();
    const healthy = new FakeSocket();
    failing.sendError = new Error("write failed");
    presence.setConnection(recipientId, failing);
    presence.setConnection(recipientId, healthy);

    const handler = new DeliveryHandler(
      { getUserIds: () => Promise.resolve([recipientId]) },
      presence,
      { notifyMessageCreated: vi.fn() },
      new TtlDedupe(60_000, 100),
    );

    await handler.handle(created);

    expect(healthy.sent).toHaveLength(1);
    expect(presence.getConnections(recipientId).has(failing)).toBe(false);
    expect(presence.getConnections(recipientId).has(healthy)).toBe(true);
  });

  it("removes sockets that throw while sending", async () => {
    const recipientId = randomUUID();
    const created = event();
    const presence = new Presence();
    const throwing = new FakeSocket();
    throwing.throwOnSend = true;
    presence.setConnection(recipientId, throwing);

    const handler = new DeliveryHandler(
      { getUserIds: () => Promise.resolve([recipientId]) },
      presence,
      { notifyMessageCreated: vi.fn() },
      new TtlDedupe(60_000, 100),
    );

    await handler.handle(created);

    expect(presence.isActive(recipientId)).toBe(false);
  });

  it("skips already completed message IDs", async () => {
    const recipientId = randomUUID();
    const created = event();
    const getUserIds = vi.fn(() => Promise.resolve([recipientId]));
    const presence = new Presence();
    const socket = new FakeSocket();
    presence.setConnection(recipientId, socket);
    const handler = new DeliveryHandler(
      { getUserIds },
      presence,
      { notifyMessageCreated: vi.fn() },
      new TtlDedupe(60_000, 100),
    );

    await handler.handle(created);
    await handler.handle(created);

    expect(getUserIds).toHaveBeenCalledTimes(1);
    expect(socket.sent).toHaveLength(1);
  });
});
