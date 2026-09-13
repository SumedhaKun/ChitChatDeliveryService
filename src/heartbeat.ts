import type { Presence } from "./presence.js";

export interface HeartbeatOptions {
  intervalMs: number;
  staleMs: number;
  now?: () => number;
}

export interface Heartbeat {
  stop(): void;
}

export function startPresenceHeartbeat(
  presence: Presence,
  options: HeartbeatOptions,
): Heartbeat {
  const now = options.now ?? Date.now;
  const timer = setInterval(() => {
    const timestamp = now();
    presence.pingAll();
    presence.removeStale(timestamp - options.staleMs);
  }, options.intervalMs);
  timer.unref();

  return {
    stop() {
      clearInterval(timer);
    },
  };
}
