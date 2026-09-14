import { randomUUID } from "node:crypto";

import { afterEach, describe, expect, it, vi } from "vitest";

import { Presence } from "../src/presence.js";
import { TypingHub } from "../src/typing.js";
import { FakeSocket, fakeMembers } from "./fakes.js";

function parseSent(socket: FakeSocket): unknown[] {
  return socket.sent.map((payload) => JSON.parse(payload) as unknown);
}

describe("TypingHub", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("forwards typing to conversation members and excludes the sender", async () => {
    vi.useFakeTimers();
    const conversationId = randomUUID();
    const senderId = randomUUID();
    const peerId = randomUUID();
    const outsiderId = randomUUID();
    const presence = new Presence();
    const senderSocket = new FakeSocket();
    const peerSocket = new FakeSocket();
    const outsiderSocket = new FakeSocket();
    presence.setConnection(senderId, senderSocket);
    presence.setConnection(peerId, peerSocket);
    presence.setConnection(outsiderId, outsiderSocket);
    const hub = new TypingHub(
      fakeMembers({
        getUserIds: (id) =>
          Promise.resolve(
            id === conversationId ? [senderId, peerId] : [outsiderId],
          ),
      }),
      presence,
      5_000,
    );

    await expect(hub.handle(senderId, conversationId, true)).resolves.toBe(
      "ok",
    );
    expect(parseSent(peerSocket)).toEqual([
      { type: "typing", userId: senderId, conversationId, isTyping: true },
    ]);
    expect(parseSent(senderSocket)).toEqual([]);
    expect(parseSent(outsiderSocket)).toEqual([]);

    peerSocket.sent.length = 0;
    await hub.handle(senderId, conversationId, true);
    expect(parseSent(peerSocket)).toEqual([]);

    hub.stop();
  });

  it("rejects typing in a conversation the sender is not in", async () => {
    const hub = new TypingHub(
      fakeMembers({
        getUserIds: () => Promise.resolve([randomUUID()]),
      }),
      new Presence(),
    );

    await expect(hub.handle(randomUUID(), randomUUID(), true)).resolves.toBe(
      "forbidden",
    );
    hub.stop();
  });

  it("clears typing immediately on false and after the timeout", async () => {
    vi.useFakeTimers();
    const conversationId = randomUUID();
    const senderId = randomUUID();
    const peerId = randomUUID();
    const presence = new Presence();
    const peerSocket = new FakeSocket();
    presence.setConnection(peerId, peerSocket);
    const hub = new TypingHub(
      fakeMembers({
        getUserIds: () => Promise.resolve([senderId, peerId]),
      }),
      presence,
      5_000,
    );

    await hub.handle(senderId, conversationId, true);
    peerSocket.sent.length = 0;
    await hub.handle(senderId, conversationId, false);
    expect(parseSent(peerSocket)).toEqual([
      { type: "typing", userId: senderId, conversationId, isTyping: false },
    ]);

    peerSocket.sent.length = 0;
    await hub.handle(senderId, conversationId, false);
    expect(parseSent(peerSocket)).toEqual([]);

    await hub.handle(senderId, conversationId, true);
    peerSocket.sent.length = 0;
    await vi.advanceTimersByTimeAsync(5_000);
    expect(parseSent(peerSocket)).toEqual([
      { type: "typing", userId: senderId, conversationId, isTyping: false },
    ]);
    hub.stop();
  });
});
