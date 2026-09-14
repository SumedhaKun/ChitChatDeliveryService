import { afterEach, describe, expect, it, vi } from "vitest";

import { startPresenceHeartbeat } from "../src/heartbeat.js";
import { Presence } from "../src/presence.js";
import { FakeSocket } from "./fakes.js";

describe("startPresenceHeartbeat", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("pings sockets and removes stale connections", () => {
    vi.useFakeTimers();
    const presence = new Presence();
    const fresh = new FakeSocket();
    const stale = new FakeSocket();
    presence.setConnection("user-1", fresh, 80);
    presence.setConnection("user-1", stale, 10);

    const heartbeat = startPresenceHeartbeat(presence, {
      intervalMs: 1_000,
      staleMs: 30,
      now: () => 90,
    });
    vi.advanceTimersByTime(1_000);

    expect(fresh.pingCount).toBe(1);
    expect(stale.terminated).toBe(true);
    expect(presence.getConnections("user-1").has(fresh)).toBe(true);
    heartbeat.stop();
  });

  it("notifies when a user's last socket becomes stale", () => {
    vi.useFakeTimers();
    const presence = new Presence();
    const stale = new FakeSocket();
    const onBecameInactive = vi.fn();
    presence.setConnection("user-1", stale, 10);

    const heartbeat = startPresenceHeartbeat(presence, {
      intervalMs: 1_000,
      staleMs: 30,
      now: () => 90,
      onBecameInactive,
    });
    vi.advanceTimersByTime(1_000);

    expect(onBecameInactive).toHaveBeenCalledWith("user-1");
    expect(presence.isActive("user-1")).toBe(false);
    heartbeat.stop();
  });
});
