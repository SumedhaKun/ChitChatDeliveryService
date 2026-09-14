import WebSocket from "ws";

import type { Presence, SocketLike } from "./presence.js";

export interface FanoutLogger {
  error(message: string, details?: unknown): void;
}

export function sendToOpenSockets(
  presence: Presence,
  userId: string,
  payload: string,
  logger?: FanoutLogger,
): number {
  const openSockets = [...presence.getConnections(userId).keys()].filter(
    (socket) => socket.readyState === WebSocket.OPEN,
  );
  for (const socket of openSockets) {
    sendBestEffort(presence, userId, socket, payload, logger);
  }
  return openSockets.length;
}

export function sendBestEffort(
  presence: Presence,
  userId: string,
  socket: SocketLike,
  payload: string,
  logger?: FanoutLogger,
): void {
  try {
    socket.send(payload, (error) => {
      if (!error) return;
      presence.deleteConnection(userId, socket);
      logger?.error("WebSocket delivery failed", {
        userId,
        error: error.message,
      });
    });
  } catch (error) {
    presence.deleteConnection(userId, socket);
    logger?.error("WebSocket delivery failed", { userId, error });
  }
}
