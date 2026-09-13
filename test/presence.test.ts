import { describe, expect, it, vi } from "vitest";
import { WebSocket } from "ws";

import { Presence } from "../src/presence.js";
import { FakeSocket } from "./fakes.js";

describe("Presence", () => {
  it("tracks multiple sockets for one user", () => {
    const presence = new Presence();
    const first = new FakeSocket();
    const second = new FakeSocket();

    presence.setConnection("user-1", first, 10);
    presence.setConnection("user-1", second, 20);

    expect(presence.isActive("user-1")).toBe(true);
    expect(presence.getConnections("user-1").size).toBe(2);
    expect(presence.getConnections("user-1").get(first)?.lastSeen).toBe(10);
    expect(presence.getConnections("user-1").get(second)?.lastSeen).toBe(20);
  });

  it("touches, deletes, and clears inactive users", () => {
    const presence = new Presence();
    const socket = new FakeSocket();
    presence.setConnection("user-1", socket, 10);

    expect(presence.touchConnection("user-1", socket, 30)).toBe(true);
    expect(presence.getConnections("user-1").get(socket)?.lastSeen).toBe(30);
    expect(presence.touchConnection("missing", socket, 40)).toBe(false);
    expect(presence.deleteConnection("user-1", socket)).toBe(true);
    expect(presence.isActive("user-1")).toBe(false);
    expect(presence.deleteConnection("user-1", socket)).toBe(false);
  });

  it("pings open sockets and terminates stale ones", () => {
    const presence = new Presence();
    const fresh = new FakeSocket();
    const stale = new FakeSocket();
    const closed = new FakeSocket();
    closed.readyState = WebSocket.CLOSED;
    const onStale = vi.fn();

    presence.setConnection("user-1", fresh, 80);
    presence.setConnection("user-1", stale, 10);
    presence.setConnection("user-2", closed, 90);
    presence.pingAll();
    const removed = presence.removeStale(50, onStale);

    expect(fresh.pingCount).toBe(1);
    expect(stale.pingCount).toBe(1);
    expect(closed.pingCount).toBe(0);
    expect(removed).toBe(1);
    expect(onStale).toHaveBeenCalledWith(stale, "user-1");
    expect(presence.getConnections("user-1").has(fresh)).toBe(true);
    expect(presence.getConnections("user-1").has(stale)).toBe(false);
  });
});
