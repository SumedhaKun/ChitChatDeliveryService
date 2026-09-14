import { randomUUID } from "node:crypto";

import { describe, expect, it } from "vitest";
import { WebSocket } from "ws";

import { ActivityHub } from "../src/activity.js";
import { Presence } from "../src/presence.js";
import { FakeSocket, fakeMembers } from "./fakes.js";

function parseSent(socket: FakeSocket): unknown[] {
  return socket.sent.map((payload) => JSON.parse(payload) as unknown);
}

describe("ActivityHub", () => {
  it("broadcasts set on first connect and sends a snapshot of online contacts", async () => {
    const userId = randomUUID();
    const contactId = randomUUID();
    const strangerId = randomUUID();
    const presence = new Presence();
    const userSocket = new FakeSocket();
    const contactSocket = new FakeSocket();
    const strangerSocket = new FakeSocket();
    presence.setConnection(contactId, contactSocket);
    presence.setConnection(strangerId, strangerSocket);
    presence.setConnection(userId, userSocket);
    const hub = new ActivityHub(
      fakeMembers({
        getContactIds: (id) =>
          Promise.resolve(id === userId ? [contactId] : [userId]),
      }),
      presence,
    );
    await hub.afterAuthenticated(contactId, contactSocket, true);
    contactSocket.sent.length = 0;
    strangerSocket.sent.length = 0;
    userSocket.sent.length = 0;

    await hub.afterAuthenticated(userId, userSocket, true);

    expect(parseSent(contactSocket)).toEqual([
      { type: "activity_changed", userId, action: "set" },
    ]);
    expect(parseSent(strangerSocket)).toEqual([]);
    expect(parseSent(userSocket)).toEqual([
      {
        type: "activity_snapshot",
        users: [{ userId: contactId, action: "set" }],
      },
    ]);
  });

  it("does not rebroadcast set for a second device", async () => {
    const userId = randomUUID();
    const contactId = randomUUID();
    const presence = new Presence();
    const first = new FakeSocket();
    const second = new FakeSocket();
    const contactSocket = new FakeSocket();
    presence.setConnection(contactId, contactSocket);
    presence.setConnection(userId, first);
    const hub = new ActivityHub(
      fakeMembers({
        getContactIds: (id) =>
          Promise.resolve(id === userId ? [contactId] : [userId]),
      }),
      presence,
    );
    await hub.afterAuthenticated(contactId, contactSocket, true);
    contactSocket.sent.length = 0;

    await hub.afterAuthenticated(userId, first, true);
    expect(parseSent(contactSocket)).toEqual([
      { type: "activity_changed", userId, action: "set" },
    ]);
    contactSocket.sent.length = 0;
    presence.setConnection(userId, second);
    await hub.afterAuthenticated(userId, second, false);

    expect(parseSent(contactSocket)).toEqual([]);
    expect(parseSent(second)).toEqual([
      {
        type: "activity_snapshot",
        users: [{ userId: contactId, action: "set" }],
      },
    ]);
  });

  it("fans out delete only when the stored action changes", async () => {
    const userId = randomUUID();
    const contactId = randomUUID();
    const presence = new Presence();
    const userSocket = new FakeSocket();
    const contactSocket = new FakeSocket();
    presence.setConnection(userId, userSocket);
    presence.setConnection(contactId, contactSocket);
    const hub = new ActivityHub(
      fakeMembers({
        getContactIds: () => Promise.resolve([contactId]),
      }),
      presence,
    );
    await hub.afterAuthenticated(userId, userSocket, true);
    contactSocket.sent.length = 0;

    await hub.handleClientAction(userId, "delete");
    await hub.handleClientAction(userId, "delete");

    expect(parseSent(contactSocket)).toEqual([
      { type: "activity_changed", userId, action: "delete" },
    ]);
  });

  it("broadcasts timeout when the last socket goes inactive unless already deleted", async () => {
    const userId = randomUUID();
    const contactId = randomUUID();
    const presence = new Presence();
    const userSocket = new FakeSocket();
    const contactSocket = new FakeSocket();
    presence.setConnection(userId, userSocket);
    presence.setConnection(contactId, contactSocket);
    const hub = new ActivityHub(
      fakeMembers({
        getContactIds: () => Promise.resolve([contactId]),
      }),
      presence,
    );
    await hub.afterAuthenticated(userId, userSocket, true);
    contactSocket.sent.length = 0;

    await hub.onBecameInactive(userId);
    expect(parseSent(contactSocket)).toEqual([
      { type: "activity_changed", userId, action: "timeout" },
    ]);

    contactSocket.sent.length = 0;
    await hub.onBecameInactive(userId);
    expect(parseSent(contactSocket)).toEqual([]);

    presence.setConnection(userId, userSocket);
    await hub.afterAuthenticated(userId, userSocket, true);
    await hub.handleClientAction(userId, "delete");
    contactSocket.sent.length = 0;
    await hub.onBecameInactive(userId);
    expect(parseSent(contactSocket)).toEqual([]);
  });

  it("does not send activity frames to closed sockets", async () => {
    const userId = randomUUID();
    const contactId = randomUUID();
    const presence = new Presence();
    const userSocket = new FakeSocket();
    const closed = new FakeSocket();
    closed.readyState = WebSocket.CLOSED;
    presence.setConnection(userId, userSocket);
    presence.setConnection(contactId, closed);
    const hub = new ActivityHub(
      fakeMembers({
        getContactIds: () => Promise.resolve([contactId]),
      }),
      presence,
    );

    await hub.afterAuthenticated(userId, userSocket, true);

    expect(closed.sent).toEqual([]);
  });
});
