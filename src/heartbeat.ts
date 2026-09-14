import type { Presence, SocketLike } from "./presence.js";

export interface HeartbeatOptions {
  intervalMs: number;
  staleMs: number;
  now?: () => number;
  onBecameInactive?: (userId: string) => void;
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
    const idleUsers = new Set<string>();
    presence.removeStale(
      timestamp - options.staleMs,
      (socket: SocketLike, userId: string) => {
        socket.terminate?.();
        idleUsers.add(userId);
      },
    );
    for (const userId of idleUsers) {
      if (!presence.isActive(userId)) {
        options.onBecameInactive?.(userId);
      }
    }
  }, options.intervalMs);
  timer.unref();

  return {
    stop() {
      clearInterval(timer);
    },
  };
}
